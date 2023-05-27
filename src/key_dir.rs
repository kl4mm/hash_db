use std::collections::hash_map::{HashMap, Iter};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::fs::OpenOptions;
use tokio::io::{self, BufReader};
use tokio::sync::RwLock;

use crate::db;
use crate::entry::Entry;

#[derive(Debug, Clone, PartialEq)]
pub struct KeyData {
    pub path: PathBuf,
    pub value_s: u64,
    pub pos: u64,
    pub time: u64,
}

pub type KeyDirMap = HashMap<String, KeyData>;

pub struct KeyDir {
    inner: KeyDirMap,
    latest: Option<PathBuf>,
    path: PathBuf,
    max_file_size: u64,
}

impl KeyDir {
    pub fn get(&self, k: &str) -> Option<&KeyData> {
        self.inner.get(k)
    }

    pub fn insert(&mut self, k: String, v: KeyData) -> Option<KeyData> {
        self.inner.insert(k, v)
    }

    pub fn remove(&mut self, k: &str) -> Option<KeyData> {
        self.inner.remove(k)
    }

    pub fn latest(&self) -> &Option<PathBuf> {
        &self.latest
    }

    pub fn set_latest(&mut self, path: PathBuf) {
        self.latest = Some(path);
    }

    pub fn iter(&self) -> Iter<String, KeyData> {
        self.inner.iter()
    }

    pub fn insert_entry(&mut self, file: PathBuf, entry: &Entry) {
        let key_data = KeyData {
            path: file,
            value_s: entry.value_s,
            pos: entry.pos,
            time: entry.time,
        };

        self.inner.insert(entry.key.to_owned(), key_data);
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }
}

pub async fn bootstrap(db_path: &str, max_file_size: u64) -> io::Result<Arc<RwLock<KeyDir>>> {
    let mut ret = KeyDir {
        inner: HashMap::new(),
        latest: db::get_active_file(db_path).await?,
        path: PathBuf::from(db_path),
        max_file_size,
    };

    let mut dir = db::open_db_dir(db_path).await?;

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
                ret.remove(&entry.key);
                continue;
            }

            ret.insert_entry(file.path(), &entry);
        }
    }

    Ok(Arc::new(RwLock::new(ret)))
}

#[cfg(test)]
mod test {
    use std::{io, path::PathBuf};

    use tokio::fs::OpenOptions;
    use tokio::io::BufWriter;

    use crate::key_dir::KeyData;
    use crate::{db, entry::Entry, key_dir};

    const TEST_KEY_DIR_PATH: &str = "test_key_dir_db/";
    const MAX_FILE_SIZE: u64 = 64;

    fn expected_test_db_path() -> PathBuf {
        let mut path = PathBuf::new();
        path.push(TEST_KEY_DIR_PATH);
        path.push("1000000");

        path
    }

    async fn setup() -> io::Result<()> {
        let entries = [("hello", "world"), ("test", "key"), ("apple", "fruit")];

        // Will create if path doesn't exist
        let _ = db::open_db_dir(TEST_KEY_DIR_PATH).await?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(expected_test_db_path())
            .await?;
        let mut writer = BufWriter::new(file);

        let mut ts = 0;
        for entry in entries {
            Entry::write_new(&mut writer, entry.0, entry.1, ts, false).await?;
            ts += 1;
        }

        Entry::write_new(&mut writer, "deleted", "key", ts, true).await?;

        Ok(())
    }

    struct CleanUp(&'static str);
    impl Drop for CleanUp {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(self.0) {
                eprintln!("ERROR: could not remove {} - {}", self.0, e);
            }
        }
    }

    #[tokio::test]
    async fn test_bootstrap() -> io::Result<()> {
        setup().await?;
        let _cu = CleanUp(TEST_KEY_DIR_PATH);

        let res = key_dir::bootstrap(TEST_KEY_DIR_PATH, MAX_FILE_SIZE).await?;

        let res = res.read().await;
        for (k, v) in res.iter() {
            match k.as_str() {
                "hello" => {
                    assert!(
                        v == &KeyData {
                            path: expected_test_db_path(),
                            value_s: 5,
                            pos: 0,
                            time: 0,
                        }
                    );
                }
                "test" => {
                    assert!(
                        v == &KeyData {
                            path: expected_test_db_path(),
                            value_s: 3,
                            pos: 35,
                            time: 1,
                        }
                    );
                }
                "apple" => {
                    assert!(
                        v == &KeyData {
                            path: expected_test_db_path(),
                            value_s: 5,
                            pos: 67,
                            time: 2,
                        }
                    );
                }
                _ => panic!("Key should not exist"),
            }
        }

        Ok(())
    }
}
