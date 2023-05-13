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
}

pub async fn bootstrap(db_path: &str) -> io::Result<Arc<RwLock<KeyDir>>> {
    let mut ret = KeyDir {
        inner: HashMap::new(),
        latest: db::get_latest_file(db_path).await?,
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
                ret.remove(std::str::from_utf8(&entry.key).unwrap());
                continue;
            }

            entry.add_to_key_dir(&mut ret.inner, file.path());
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

    const TEST_DB_PATH: &str = "test_db/";

    fn expected_test_db_path() -> PathBuf {
        let mut path = PathBuf::new();
        path.push(TEST_DB_PATH);
        path.push("1000000");

        path
    }

    async fn setup() -> io::Result<()> {
        let entries: Vec<Entry> = vec![
            Entry::new(false, 10, 5, 5, b"hello".to_vec(), b"world".to_vec(), 0),
            Entry::new(false, 20, 4, 3, b"test".to_vec(), b"key".to_vec(), 35),
            Entry::new(false, 30, 5, 5, b"apple".to_vec(), b"fruit".to_vec(), 67),
            Entry::new(true, 30, 7, 3, b"deleted".to_vec(), b"key".to_vec(), 101),
        ];

        // Will xreate if path doesn't exist
        let _ = db::open_db_dir(TEST_DB_PATH).await?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(expected_test_db_path())
            .await?;
        let mut writer = BufWriter::new(file);

        for entry in entries {
            entry.write(&mut writer).await?;
        }

        Ok(())
    }

    struct CleanUp;
    impl Drop for CleanUp {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(TEST_DB_PATH) {
                eprintln!("ERROR: could not remove test db dir- {e}")
            }
        }
    }

    #[tokio::test]
    async fn test_bootstrap() -> io::Result<()> {
        setup().await?;
        let _cu = CleanUp;

        let res = key_dir::bootstrap(TEST_DB_PATH).await?;

        let res = res.read().await;
        for (k, v) in res.iter() {
            match k.as_str() {
                "hello" => {
                    assert!(
                        v == &KeyData {
                            path: expected_test_db_path(),
                            value_s: 5,
                            pos: 0,
                            time: 10
                        }
                    );
                }
                "test" => {
                    assert!(
                        v == &KeyData {
                            path: expected_test_db_path(),
                            value_s: 3,
                            pos: 35,
                            time: 20
                        }
                    );
                }
                "apple" => {
                    assert!(
                        v == &KeyData {
                            path: expected_test_db_path(),
                            value_s: 5,
                            pos: 67,
                            time: 30
                        }
                    );
                }
                _ => panic!("Key should not exist"),
            }
        }

        Ok(())
    }
}
