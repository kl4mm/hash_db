use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::{self, File};
use tokio::fs::{OpenOptions, ReadDir};
use tokio::io::{BufReader, BufWriter};
use tokio::sync::RwLock;

use crate::entry::Entry;
use crate::key_dir::KeyDirMap;

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

pub async fn get_latest_file(db_path: &str) -> io::Result<Option<PathBuf>> {
    // Get the latest created dir in db/
    let mut dir = fs::read_dir(db_path).await.expect("Couldn't access db");

    // Get the latest file:
    let mut latest_time = UNIX_EPOCH;
    let mut latest_file = None;
    while let Some(d) = dir.next_entry().await? {
        let created_at = d.metadata().await?.created()?;

        if created_at > latest_time {
            latest_time = created_at;
            latest_file = Some(d);
        }
    }

    match latest_file {
        Some(entry) => Ok(Some(entry.path())),
        None => Ok(None),
    }
}

/// Returns latest file, file size and path
/// Will create a new file if latest file is greater than MAX_FILE_SIZE
pub async fn open_latest() -> io::Result<(File, u64, PathBuf)> {
    // Return the file if its less than MAX_FILE_SIZE
    if let Some(path) = get_latest_file(DB_PATH).await? {
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

    Ok((file, 0, file_path))
}

async fn clean_up(key_dir: Arc<RwLock<KeyDirMap>>) -> io::Result<()> {
    loop {
        // Run clean up every 5 minutes
        tokio::time::sleep(Duration::from_secs(5 * 60)).await;

        let mut dir = open_db_dir(DB_PATH).await?;

        while let Some(file) = dir.next_entry().await? {
            let path = file.path();
            // Don't clean up hint files, delete any later if needed
            if path.ends_with("_hint") {
                continue;
            }

            // Skip if latest file
            // TODO: reading dir each iteration
            match get_latest_file(DB_PATH).await? {
                Some(latest_path) if latest_path == path => continue,
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
                let key = match std::str::from_utf8(&entry.key) {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("ERROR: key is not UTF-8 - {e}");
                        continue;
                    }
                };

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

            if keep.len() == 0 {
                // Delete file
                fs::remove_file(path).await?;
            }
            if deleted > 0 {
                // Rewrite file with keep
                // TODO: creating a new file would affect the result of get_latest_file,
                // there needs to be a different way to get the latest file
                let new_file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("TODO")
                    .await?;
                let mut writer = BufWriter::new(new_file);

                for entry in keep {
                    entry.write(&mut writer).await?;
                }
            }
        }
    }
}
