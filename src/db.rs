use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::{self, File};
use tokio::fs::{OpenOptions, ReadDir};
use tokio::io::BufReader;
use tokio::sync::RwLock;

use crate::entry::Entry;
use crate::key_dir::{KeyData, KeyDir};

/// Attemps to open db_path. Will create if not found.
pub async fn open_db_dir<T>(db_path: T) -> io::Result<ReadDir>
where
    T: AsRef<Path> + Copy,
{
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

/// Returns latest file, position and path
/// Will create a new file if latest file is greater than MAX_FILE_SIZE
pub async fn open_latest(key_dir: &Arc<RwLock<KeyDir>>) -> io::Result<(File, u64, PathBuf)> {
    // Return the file if its less than MAX_FILE_SIZE
    if let Some(path) = key_dir.read().await.latest() {
        let file = OpenOptions::new().append(true).open(&path).await?;
        let position = file.metadata().await?.len();

        if position < key_dir.read().await.max_file_size() {
            return Ok((file, position, path.clone()));
        }
    }

    // At this point, either there are no files in db path or the latest
    // file exceeds MAX_FILE_SIZE

    // File name is time is millisecond timestamp
    let file_name = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time before UNIX Epoch")
        .as_millis()
        .to_string();

    let mut file_path = key_dir.read().await.path().clone();
    file_path.push(file_name);

    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&file_path)
        .await?;

    key_dir.write().await.set_latest(file_path.clone());

    Ok((file, 0, file_path))
}

pub async fn compaction_loop(key_dir: Arc<RwLock<KeyDir>>, interval: Duration) -> io::Result<()> {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = compaction(&key_dir).await {
            eprintln!("ERROR: Running compaction: {e}");
        }
    }
}

async fn compaction(key_dir: &Arc<RwLock<KeyDir>>) -> io::Result<()> {
    let mut dir = open_db_dir(key_dir.read().await.path()).await?;

    while let Some(file) = dir.next_entry().await? {
        let mut path = file.path();
        eprintln!("Compacting {:?}", &path);

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

        eprintln!("Reading {:?}", file.path());
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

        eprintln!("Keeping: {}", keep.len());
        if keep.len() == 0 {
            fs::remove_file(path).await?;
            continue;
        }

        eprintln!("Deleting: {}", deleted);
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

    use tokio::fs::{self, OpenOptions};

    use crate::{
        command::Command,
        db,
        key_dir::{self, KeyData},
    };

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
        let _c = CleanUp(DB_PATH);

        let files = ["100_1", "200", "300_2", "400"];

        for file in files {
            let mut path = PathBuf::new();
            path.push(DB_PATH);
            path.push(file);

            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;
        }

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
    async fn test_compaction_delete_entire_file() -> io::Result<()> {
        const DB_PATH: &str = "test_compaction_delete_entire_file/";
        const MAX_FILE_SIZE: u64 = 256;

        // Set a dummy file to be the latest
        let _ = db::open_db_dir(DB_PATH).await?;
        let dummy_file = PathBuf::from(format!("{DB_PATH}0"));
        fs::File::create(&dummy_file).await?;
        let _c = CleanUp(DB_PATH);

        let key_dir = key_dir::bootstrap(DB_PATH, MAX_FILE_SIZE)
            .await
            .expect("key dir bootstrap failed");

        let entries = [
            ("key", "value"),
            ("key", "value"),
            ("key", "value"),
            ("key", "value"),
            ("key", "value"),
        ];

        let mut null = Vec::new();
        for entry in entries {
            Command::insert(&key_dir, &mut null, entry.0, entry.1).await?;
        }

        key_dir.write().await.set_latest(dummy_file.clone());
        key_dir.write().await.insert(
            "key".into(),
            KeyData {
                path: dummy_file,
                value_s: 5,
                pos: 0,
                time: 0,
            },
        );
        db::compaction(&key_dir).await.expect("compaction failed");

        // There should only be the one dummy file in DB_PATH
        let mut db = db::open_db_dir(DB_PATH).await?;
        let mut count = 0;
        while let Some(_) = db.next_entry().await? {
            count += 1
        }

        assert!(count == 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_delete_some() -> io::Result<()> {
        const DB_PATH: &str = "test_compaction_delete_some/";
        const MAX_FILE_SIZE: u64 = 512;

        // Set a dummy file to be the latest
        let _ = db::open_db_dir(DB_PATH).await?;
        let dummy_file = PathBuf::from(format!("{DB_PATH}0"));
        fs::File::create(&dummy_file).await?;
        let _c = CleanUp(DB_PATH);

        let key_dir = key_dir::bootstrap(DB_PATH, MAX_FILE_SIZE)
            .await
            .expect("key dir bootstrap failed");

        let entries = [
            ("key", "value"),
            ("other_key", "other_value"),
            ("key", "value"),
            ("other_key", "other_value"),
            ("key", "value"),
            ("other_key", "other_value"),
            ("key", "value"),
            ("other_key", "other_value"),
            ("key", "value"),
        ];

        let mut null = Vec::new();
        for entry in entries {
            Command::insert(&key_dir, &mut null, entry.0, entry.1).await?;
        }

        key_dir.write().await.set_latest(dummy_file.clone());
        key_dir.write().await.insert(
            "key".into(),
            KeyData {
                path: dummy_file,
                value_s: 5,
                pos: 0,
                time: 0,
            },
        );
        db::compaction(&key_dir).await.expect("compaction failed");

        // Running compaction once the original file remains and a new file with a _1 suffix is
        // created. The new file will only contain "other_key".
        // The second time compaction is run, the original file is deleted because there is nothing
        // to keep - "key" lives in the dummy file and "other_key" lives in the new file.

        let got = key_dir
            .read()
            .await
            .get("other_key")
            .unwrap()
            .path
            .clone()
            .into_os_string()
            .into_string()
            .unwrap();

        assert!(got.ends_with("_1"));

        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_delete_none() -> io::Result<()> {
        const DB_PATH: &str = "test_compaction_delete_none/";
        const MAX_FILE_SIZE: u64 = 256;

        // Set a dummy file to be the latest
        let _ = db::open_db_dir(DB_PATH).await?;
        let dummy_file = PathBuf::from(format!("{DB_PATH}0"));
        fs::File::create(&dummy_file).await?;
        let _c = CleanUp(DB_PATH);

        let key_dir = key_dir::bootstrap(DB_PATH, MAX_FILE_SIZE)
            .await
            .expect("key dir bootstrap failed");

        let entries = [
            ("key", "value"),
            ("other_key", "other_value"),
            ("other_key2", "other_value"),
            ("other_key3", "other_value"),
            ("other_key4", "other_value"),
            ("other_key5", "other_value"),
            ("other_key6", "other_value"),
        ];

        let mut null = Vec::new();
        for entry in entries {
            Command::insert(&key_dir, &mut null, entry.0, entry.1).await?;
        }

        key_dir.write().await.set_latest(dummy_file.clone());
        db::compaction(&key_dir).await.expect("compaction failed");

        let mut db = db::open_db_dir(DB_PATH).await?;
        let mut size = 0;
        while let Some(dir) = db.next_entry().await? {
            size += dir.metadata().await.expect("couldn't get metadata").len()
        }

        assert!(size == 308);

        Ok(())
    }
}
