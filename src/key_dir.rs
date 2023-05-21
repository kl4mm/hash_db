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

    pub fn insert_entry(&mut self, file: PathBuf, entry: &Entry) {
        let key_data = KeyData {
            path: file,
            value_s: entry.value_s,
            pos: entry.pos,
            time: entry.time,
        };

        self.inner.insert(entry.key.to_owned(), key_data);
    }
}

pub async fn bootstrap(db_path: &str) -> io::Result<Arc<RwLock<KeyDir>>> {
    let mut ret = KeyDir {
        inner: HashMap::new(),
        latest: db::get_active_file(db_path).await?,
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

    fn expected_test_db_path() -> PathBuf {
        let mut path = PathBuf::new();
        path.push(TEST_KEY_DIR_PATH);
        path.push("1000000");

        path
    }

    async fn setup() -> io::Result<()> {
        let entries: Vec<Entry> = vec![
            Entry::new(false, 10, 5, 5, "hello".into(), "world".into(), 0),
            Entry::new(false, 20, 4, 3, "test".into(), "key".into(), 35),
            Entry::new(false, 30, 5, 5, "apple".into(), "fruit".into(), 67),
            Entry::new(true, 30, 7, 3, "deleted".into(), "key".into(), 101),
        ];

        // Will create if path doesn't exist
        let _ = db::open_db_dir(TEST_KEY_DIR_PATH).await?;

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

        let res = key_dir::bootstrap(TEST_KEY_DIR_PATH).await?;

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
