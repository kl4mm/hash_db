use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs::OpenOptions;
use tokio::fs::{self, File};
use tokio::io::BufReader;

use crate::entry::{Entry, KeyData};

const MAX_FILE_SIZE: u64 = 64;
const DB_PATH: &str = "db/";

pub async fn bootstrap() -> io::Result<HashMap<String, KeyData>> {
    let mut ret = HashMap::new();

    // Try to open db/, create it if it doesn't exist
    let mut dir = match fs::read_dir(DB_PATH).await {
        Ok(d) => d,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            fs::create_dir(DB_PATH).await?;
            fs::read_dir(DB_PATH).await?
        }
        Err(e) => panic!("{}", e),
    };

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

        while let Some(entry) = Entry::read(&mut reader).await {
            entry.add_to_index(file.path(), &mut ret);
        }
    }

    Ok(ret)
}

/// Returns latest file, file size and path
/// Will create a new file if latest file is greater than MAX_FILE_SIZE
pub async fn open_latest() -> io::Result<(File, u64, PathBuf)> {
    // Get the latest created dir in db/
    let mut dir = fs::read_dir(DB_PATH).await.expect("Couldn't access db");

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

    // Return the file if its less than MAX_FILE_SIZE
    if let Some(file) = latest_file {
        let file_path = PathBuf::from(file.path());
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
