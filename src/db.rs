use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::{self, File};
use tokio::fs::{OpenOptions, ReadDir};
use tokio::io::{BufReader, BufWriter};
use tokio::sync::RwLock;

use crate::entry::Entry;
use crate::key_dir::{KeyData, KeyDir};

const MAX_FILE_SIZE: u64 = 64;
pub const DB_PATH: &str = "db/";

pub async fn open_db_dir(db_path: &str) -> io::Result<ReadDir> {
    // Try to open db/, create it if it doesn't exist
    let dir = match fs::read_dir(db_path).await {
        Ok(d) => d,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            fs::create_dir(db_path).await?;
            fs::read_dir(db_path).await?
        }
        Err(e) => panic!("{}", e),
    };

    Ok(dir)
}

pub async fn get_active_file(db_path: &str) -> io::Result<Option<PathBuf>> {
    // Get the latest created dir in db/
    let mut dir = fs::read_dir(db_path).await.expect("Couldn't access db");

    // Get the latest file:
    let mut latest_time = 0;
    let mut latest_file = None;
    while let Some(d) = dir.next_entry().await? {
        let file_name = d.file_name().into_string().expect("Invalid file name");

        let parts: Vec<&str> = file_name.split('_').collect();
        if parts.len() == 0 {
            continue;
        }

        let timestamp: u64 = parts[0].parse().expect("Invalid file name");
        if timestamp > latest_time {
            latest_time = timestamp;
            latest_file = Some(d.path());
        }
    }

    match latest_file {
        Some(path) => Ok(Some(path)),
        None => Ok(None),
    }
}

/// Returns latest file, file size and path
/// Will create a new file if latest file is greater than MAX_FILE_SIZE
pub async fn open_latest(key_dir: &Arc<RwLock<KeyDir>>) -> io::Result<(File, u64, PathBuf)> {
    // Return the file if its less than MAX_FILE_SIZE
    if let Some(path) = key_dir.read().await.latest() {
        let file_path = PathBuf::from(path);
        let file = OpenOptions::new().append(true).open(&file_path).await?;
        let position = file.metadata().await?.len();

        if position < MAX_FILE_SIZE {
            return Ok((file, position, file_path));
        }
    }

    // At this point, either there are no files in db/ or the latest
    // file exceeds MAX_FILE_SIZE

    // File name is time in ms
    let file_name = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time before UNIX Epoch")
        .as_millis()
        .to_string();

    let mut file_path = PathBuf::new();
    file_path.push(DB_PATH);
    file_path.push(file_name);

    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&file_path)
        .await?;

    key_dir.write().await.set_latest(file_path.clone());

    Ok((file, 0, file_path))
}

async fn compaction_loop(key_dir: Arc<RwLock<KeyDir>>, interval: Duration) -> io::Result<()> {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = compaction(key_dir.clone()).await {
            dbg!(e);
        }
    }
}

async fn compaction(key_dir: Arc<RwLock<KeyDir>>) -> io::Result<()> {
    let mut dir = open_db_dir(DB_PATH).await?;

    while let Some(file) = dir.next_entry().await? {
        let mut path = file.path();
        // Don't clean up hint files, delete any later if needed
        if path.ends_with("_hint") {
            continue;
        }

        // Skip if latest file
        match key_dir.read().await.latest() {
            Some(latest_path) if *latest_path == path => continue,
            _ => {}
        }

        // Iterate over each entry, see what isn't in the keydir
        // Create a new file without those entries, update the keydir
        // Remove old file

        let mut keep = Vec::new();
        let mut deleted = 0;

        let file = OpenOptions::new().read(true).open(file.path()).await?;
        let mut reader = BufReader::new(file);

        // Read each entry in the file
        while let Some(entry) = Entry::read(&mut reader).await {
            let key = &entry.key;

            // Keep if path of entry matches path of current file
            match key_dir.read().await.get(key) {
                Some(kd) => {
                    if kd.path == path {
                        keep.push(entry);
                    } else {
                        deleted += 1;
                    }
                }
                None => deleted += 1,
            }
        }

        // Nothing to keep, remove file
        if keep.len() == 0 {
            // Delete file
            fs::remove_file(path).await?;
            continue;
        }

        // Rewrite file with entries to keep
        if deleted > 0 {
            let file_name = match path.file_name() {
                Some(name) => name.to_owned().into_string().expect("Invalid file name"),
                None => {
                    eprintln!("ERROR: Path has no file name");
                    continue;
                }
            };

            let parts: Vec<&str> = file_name.split('_').collect();

            // Get the version of the file
            let version;
            if parts.len() == 1 {
                version = 1
            } else if parts.len() == 2 {
                let current_version: u64 = match parts[1].parse() {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "ERROR: file version could not be parsed, setting version to 1: {e}"
                        );
                        0
                    }
                };

                version = current_version + 1;
            } else {
                eprintln!("ERROR: parts len neither 1 nor 2, setting version to 1");
                version = 1
            };

            // Update path with new file name
            let new_file_name = format!("{}_{}", parts[0], version);
            path.pop();
            path.push(new_file_name);

            let mut writer = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await?;

            for entry in keep {
                let position = writer.metadata().await?.len();
                entry.write(&mut writer).await?;

                key_dir.write().await.insert(
                    entry.key,
                    KeyData {
                        path: path.to_owned(),
                        value_s: entry.value_s,
                        pos: position,
                        time: entry.time,
                    },
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::{io, path::PathBuf};

    use tokio::fs::OpenOptions;

    use crate::{command::Command, db, key_dir};

    struct CleanUp(&'static str);
    impl Drop for CleanUp {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(self.0) {
                eprintln!("ERROR: could not remove {} - {}", self.0, e);
            }
        }
    }

    #[tokio::test]
    async fn test_get_active_file() -> io::Result<()> {
        const DB_PATH: &str = "test_db_active_file/";

        let _ = db::open_db_dir(DB_PATH).await?;

        let files = ["100_1", "200", "300_2", "400"];

        for file in files {
            let mut path = PathBuf::new();
            path.push(DB_PATH);
            path.push(file);

            dbg!(&path);

            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;
        }

        let _c = CleanUp(DB_PATH);

        let got = db::get_active_file(DB_PATH).await?;

        let mut expected = PathBuf::new();
        expected.push(DB_PATH);
        expected.push("400");

        assert!(got == Some(expected));

        Ok(())
    }

    // Cases:
    // 1. File full of entries that have either been deleted, or updated
    // and are active in newer files
    // 2. File contains some entries that have either been deleted, or updated
    // and are active in newer files
    // 3. File untouched
    #[tokio::test]
    async fn test_compaction() -> io::Result<()> {
        const DB_PATH: &str = "test_db_compaction/";

        // Setup
        let entries = [
            ("key", "value"),
            ("key", "value"),
            ("key", "value"),
            ("key", "value"),
            ("key", "value"),
        ];

        let mut stdout = io::stdout().lock();

        let key_dir = key_dir::bootstrap(DB_PATH)
            .await
            .expect("key dir bootstrap failed");

        for entry in entries {
            Command::insert(&key_dir, &mut stdout, entry.0, entry.1).await?;
        }

        db::compaction(key_dir).await.expect("compaction failed");

        Ok(())
    }
}
