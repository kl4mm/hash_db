use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::fs::OpenOptions;
use tokio::io::{self, BufReader};
use tokio::sync::RwLock;

use crate::db;
use crate::entry::Entry;

#[derive(Clone)]
pub struct KeyData {
    pub path: PathBuf,
    pub value_s: u64,
    pub pos: u64,
    pub time: u64,
}

pub type KeyDirMap = HashMap<String, KeyData>;

pub async fn bootstrap() -> io::Result<Arc<RwLock<KeyDirMap>>> {
    let mut ret = HashMap::new();

    let mut dir = db::open_db_dir(db::DB_PATH).await?;

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

            entry.add_to_key_dir(&mut ret, file.path());
        }
    }

    Ok(Arc::new(RwLock::new(ret)))
}
