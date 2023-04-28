use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::{self, File};
use tokio::fs::{OpenOptions, ReadDir};
use tokio::io::BufReader;

use crate::entry::{Entry, KeyData};

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

pub async fn bootstrap() -> io::Result<HashMap<String, KeyData>> {
    let mut ret = HashMap::new();

    let mut dir = open_db_dir(DB_PATH).await?;

    // Parse each file inside db/
    while let Some(file) = dir.next_entry().await? {
        eprintln!("Parsing: {:?}", file.path());
        if file.path().ends_with("_hint") {
            // TODO: parse hint files
            eprintln!("Unimplemented: parse hint files");
            continue;
        }

        let open = OpenOptions::new().read(true).open(file.path()).await?;
        let mut reader = BufReader::new(open);

        // Add to index only if the entry hasn't been deleted
        while let Some(entry) = Entry::read(&mut reader).await {
            if entry.delete {
                ret.remove(std::str::from_utf8(&entry.key).unwrap());
                continue;
            }

            entry.add_to_index(&mut ret, file.path());
        }
    }

    Ok(ret)
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

async fn clean_up() -> io::Result<()> {
    loop {
        // Run clean up every 5 minutes
        tokio::time::sleep(Duration::from_secs(60 * 5)).await;

        let mut dir = open_db_dir(DB_PATH).await?;

        while let Some(file) = dir.next_entry().await? {
            // Don't clean up hint files, delete any later if needed
            if file.path().ends_with("_hint") {
                continue;
            }

            // Skip if latest file
            // TODO: reading dir each iteration
            match get_latest_file(DB_PATH).await? {
                Some(path) if file.path() == path => continue,
                _ => {}
            }

            // Iterate over each entry, see what isn't in the keydir
            // Create a new file without those entries, update the keydir
            // Remove old file

            let file = OpenOptions::new().read(true).open(file.path()).await?;
            let mut reader = BufReader::new(file);
            while let Some(entry) = Entry::read(&mut reader).await {
                // TODO
            }
        }
    }
}
