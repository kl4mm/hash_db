use std::collections::HashMap;

use bytes::BytesMut;

use crate::storagev2::{disk::Disk, log::EntryType, page::PageID};

#[derive(Debug, PartialEq)]
pub struct KeyData {
    page_id: PageID,
    offset: u64,
}

impl KeyData {
    pub fn new(page_id: PageID, offset: u64) -> Self {
        Self { page_id, offset }
    }
}

type KeyDirMap = HashMap<BytesMut, KeyData>;

#[derive(Debug, PartialEq)]
pub struct KeyDir {
    inner: KeyDirMap,
}

impl KeyDir {
    pub fn get(&self, k: &[u8]) -> Option<&KeyData> {
        self.inner.get(k)
    }

    pub fn insert(&mut self, k: &[u8], v: KeyData) -> Option<KeyData> {
        let k = BytesMut::from(k);

        self.inner.insert(k, v)
    }

    pub fn remove(&mut self, k: &[u8]) -> Option<KeyData> {
        self.inner.remove(k)
    }
}

pub async fn bootstrap<const PAGE_SIZE: usize>(disk: &Disk) -> KeyDir {
    let len = disk.len().await;
    let pages = len / PAGE_SIZE;

    let mut inner = HashMap::new();
    for page_id in 0..pages as u32 {
        let page = disk
            .read_page::<PAGE_SIZE>(page_id)
            .expect("should read page");

        let mut offset = 0;
        while let Some(entry) = page.read_entry(offset) {
            match entry.t {
                EntryType::Put => {
                    inner.insert(
                        entry.key.clone(),
                        KeyData {
                            page_id,
                            offset: offset as u64,
                        },
                    );
                }
                EntryType::Delete => {
                    inner.remove(&entry.key);
                }
            };

            offset = offset + entry.len();
        }
    }

    KeyDir { inner }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, io};

    use crate::storagev2::{
        disk::Disk,
        key_dir::{bootstrap, KeyData, KeyDir},
        log::{Entry, EntryType},
        page::Page,
        test::CleanUp,
    };

    #[tokio::test]
    async fn test_bootstrap() -> io::Result<()> {
        const DB_FILE: &str = "./test_bootstrap.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let entries = [
            Entry::new(b"key1", b"value1", EntryType::Put),
            Entry::new(b"key2", b"value2", EntryType::Put),
            Entry::new(b"key3", b"value3", EntryType::Put),
            Entry::new(b"key4", b"value4", EntryType::Put),
            Entry::new(b"key1", b"value1", EntryType::Delete),
            Entry::new(b"key5", b"value5", EntryType::Put),
            Entry::new(b"key5", b"value5", EntryType::Delete),
            Entry::new(b"key4", b"latest", EntryType::Put),
            Entry::new(b"key5", b"latest", EntryType::Put),
        ];

        const PAGE_SIZE: usize = 256;
        let mut current_id = 0;
        let mut current = Page::<PAGE_SIZE>::new(current_id);
        for e in entries {
            if let Err(_) = current.write_entry(&e) {
                disk.write_page(&current).expect("failed to write page");
                current_id += 1;
                current = Page::<PAGE_SIZE>::new(current_id);
                current
                    .write_entry(&e)
                    .expect("new current should have space");
            }
        }
        disk.write_page(&current).expect("failed to write page");

        let key_dir = bootstrap::<PAGE_SIZE>(&disk).await;

        let expected = KeyDir {
            inner: HashMap::from([
                (
                    "key2".into(),
                    KeyData {
                        page_id: 0,
                        offset: 35,
                    },
                ),
                (
                    "key3".into(),
                    KeyData {
                        page_id: 0,
                        offset: 70,
                    },
                ),
                (
                    "key4".into(),
                    KeyData {
                        page_id: 1,
                        offset: 0,
                    },
                ),
                (
                    "key5".into(),
                    KeyData {
                        page_id: 1,
                        offset: 35,
                    },
                ),
            ]),
        };

        assert!(
            key_dir == expected,
            "\nExpected: {:?}\nGot: {:?}\n",
            expected,
            key_dir,
        );

        Ok(())
    }
}
