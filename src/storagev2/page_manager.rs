use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicU32, Ordering::*},
        Arc,
    },
};

use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::storagev2::{
    disk::Disk,
    key_dir::KeyData,
    log::Entry,
    page::{Page, PageID, PageInner},
    replacer::LRUKHandle,
};

#[derive(Debug)]
pub enum PageIndex {
    Write,
    Read(usize),
}

pub const DEFAULT_READ_SIZE: usize = 8;

pub struct Pin<'a> {
    pub page: &'a Page,
    i: usize,
    replacer: LRUKHandle,
}

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            self.replacer.blocking_unpin(self.i);
        });
    }
}

impl<'a> Pin<'a> {
    pub fn new(page: &'a Page, i: usize, replacer: LRUKHandle) -> Self {
        Self { page, i, replacer }
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, PageInner> {
        self.page.write().await
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, PageInner> {
        self.page.read().await
    }
}

#[derive(Clone)]
pub struct PageCache(Arc<PageCacheInner>);

impl PageCache {
    pub fn new(disk: Disk, lruk: usize, latest: Page, latest_id: PageID) -> Self {
        Self(Arc::new(PageCacheInner::new(disk, lruk, latest, latest_id)))
    }

    pub fn inc_id(&self) -> PageID {
        self.0.inc_id()
    }

    pub async fn replace_current(
        &self,
        current: &mut RwLockWriteGuard<'_, PageInner>,
    ) -> io::Result<()> {
        self.0.replace_current(current).await
    }

    #[cfg(test)]
    pub async fn new_page<'a>(&mut self) -> Option<PageID> {
        self.0.new_page().await
    }

    pub async fn fetch_entry(&self, kd: &KeyData) -> Option<Entry> {
        self.0.fetch_entry(kd).await
    }

    pub async fn get_current(&self) -> RwLockWriteGuard<'_, PageInner> {
        self.0.get_current().await
    }

    pub async fn flush_current(&self) {
        self.0.flush_current().await
    }
}

struct PageCacheInner<const READ_SIZE: usize = DEFAULT_READ_SIZE> {
    disk: Disk,
    page_table: RwLock<HashMap<PageID, PageIndex>>,
    current: Page,
    read: [Page; READ_SIZE],
    free: Mutex<Vec<usize>>,
    next_id: AtomicU32,
    replacer: LRUKHandle,
}

impl<const READ_SIZE: usize> PageCacheInner<READ_SIZE> {
    pub fn new(disk: Disk, lruk: usize, latest: Page, latest_id: PageID) -> Self {
        let next_id = latest_id + 1;
        let page_table = RwLock::new(HashMap::from([(latest_id, PageIndex::Write)]));
        let current = latest;
        let read: [_; READ_SIZE] = std::array::from_fn(|_| Page::default());
        let next_id = AtomicU32::new(next_id);
        let free = Mutex::new((0..READ_SIZE).rev().collect());
        let replacer = LRUKHandle::new(lruk);

        Self {
            disk,
            page_table,
            current,
            read,
            free,
            next_id,
            replacer,
        }
    }

    pub fn inc_id(&self) -> PageID {
        self.next_id.fetch_add(1, SeqCst)
    }

    pub async fn replace_current(
        &self,
        current: &mut RwLockWriteGuard<'_, PageInner>,
    ) -> io::Result<()> {
        self.disk.write_page(current.id, &current.data);

        let mut page_table = self.page_table.write().await;

        let old_id = current.id;
        if let None = page_table.remove(&old_id) {
            eprintln!("No write page while replacing write page");
        }

        let page_id = self.inc_id();
        current.reset();
        current.id = page_id;
        page_table.insert(page_id, PageIndex::Write);

        Ok(())
    }

    #[cfg(test)]
    pub async fn new_page<'a>(&self) -> Option<PageID> {
        let i = match self.free.lock().await.pop() {
            Some(i) => i,
            None => self.replacer.evict().await?,
        };
        self.replacer.remove(i).await;
        self.replacer.record_access(i).await;
        self.replacer.pin(i).await;

        let page_id = self.inc_id();

        let pin = Pin::new(&self.read[i], i, self.replacer.clone());
        let mut page = pin.write().await;
        page.reset();
        page.id = page_id;

        self.disk.write_page(page.id, &page.data);
        self.page_table
            .write()
            .await
            .insert(page_id, PageIndex::Read(i));

        Some(page_id)
    }

    pub async fn fetch_entry(&self, kd: &KeyData) -> Option<Entry> {
        if let Some(i) = self.page_table.read().await.get(&kd.page_id) {
            return match i {
                PageIndex::Write => {
                    let page = self.current.read().await;

                    page.read_entry(kd.offset as usize)
                }
                PageIndex::Read(i) => {
                    assert!(*i < READ_SIZE);
                    self.replacer.record_access(*i).await;
                    self.replacer.pin(*i).await;

                    let pin = Pin::new(&self.read[*i], *i, self.replacer.clone());
                    let page = pin.read().await;

                    page.read_entry(kd.offset as usize)
                }
            };
        };

        let i = match self.free.lock().await.pop() {
            Some(i) => i,
            None => self.replacer.evict().await?,
        };
        self.replacer.remove(i).await;
        self.replacer.record_access(i).await;
        self.replacer.pin(i).await;

        assert!(i < READ_SIZE);

        // Replace page
        let page_data = self.disk.read_page(kd.page_id).expect("Couldn't read page");
        let pin = Pin::new(&self.read[i], i, self.replacer.clone());
        let mut page = pin.write().await;
        page.reset();
        page.id = kd.page_id;
        page.data = page_data;

        let entry = page.read_entry(kd.offset as usize);
        self.page_table
            .write()
            .await
            .insert(page.id, PageIndex::Read(i));

        entry
    }

    pub async fn get_current(&self) -> RwLockWriteGuard<'_, PageInner> {
        self.current.write().await
    }

    pub async fn flush_current(&self) {
        let current = self.current.write().await;
        self.disk.write_page(current.id, &current.data);
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::storagev2::{
        disk::Disk,
        key_dir::KeyData,
        log::{Entry, EntryType},
        page::Page,
        page_manager::{PageCacheInner, DEFAULT_READ_SIZE},
        test::CleanUp,
    };

    #[tokio::test]
    async fn test_page_manager() -> io::Result<()> {
        const DB_FILE: &str = "./test_page_manager.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let m = PageCacheInner::<DEFAULT_READ_SIZE>::new(disk, 2, Page::new(0), 0);

        let mut page_w = m.get_current().await;

        let entry_a = Entry::new(b"test_keya", b"test_valuea", EntryType::Put);
        let entry_b = Entry::new(b"test_keyb", b"test_valueb", EntryType::Put);
        let offset_a = page_w.write_entry(&entry_a).expect("should not be full");
        let offset_b = page_w.write_entry(&entry_b).expect("should not be full");

        assert!(offset_a == 0);
        assert!(offset_b as usize == entry_a.len());
        drop(page_w);

        let kda = KeyData {
            page_id: 0,
            offset: 0,
        };
        let kdb = KeyData {
            page_id: 0,
            offset: entry_a.len() as u64,
        };
        let got_a = m
            .fetch_entry(&kda)
            .await
            .expect("should fetch current page");

        let got_b = m
            .fetch_entry(&kdb)
            .await
            .expect("should fetch current page");

        assert!(
            entry_a == got_a,
            "\nExpected: {:?}\nGot: {:?}\n",
            entry_a,
            got_a
        );
        assert!(
            entry_b == got_b,
            "\nExpected: {:?}\nGot: {:?}\n",
            entry_b,
            got_b
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replacer() -> io::Result<()> {
        const DB_FILE: &str = "./test_replacer.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let m = PageCacheInner::<3>::new(disk, 2, Page::new(0), 0);

        {
            let _ = m.new_page().await.expect("should have space for page 1"); // ts = 0
            let _ = m.new_page().await.expect("should have space for page 2"); // ts = 1
            let _ = m.new_page().await.expect("should have space for page 3"); // ts = 2

            let kd1 = KeyData {
                page_id: 1,
                offset: 0,
            };
            let kd2 = KeyData {
                page_id: 2,
                offset: 0,
            };
            let kd3 = KeyData {
                page_id: 3,
                offset: 0,
            };

            m.fetch_entry(&kd1).await; // ts = 3
            m.fetch_entry(&kd2).await; // ts = 4
            m.fetch_entry(&kd1).await; // ts = 5

            m.fetch_entry(&kd1).await; // ts = 6
            m.fetch_entry(&kd2).await; // ts = 7
            m.fetch_entry(&kd1).await; // ts = 8
            m.fetch_entry(&kd2).await; // ts = 9

            m.fetch_entry(&kd3).await; // ts = 10 - Least accessed, should get evicted
        }

        let new_page_id = m.new_page().await.expect("a page should have been evicted");
        assert!(new_page_id == 4, "Got: {}", new_page_id);

        let pages = &m.read;
        let expected_ids = vec![1, 2, 4];
        let mut actual_ids = Vec::new();
        for page in pages.iter() {
            actual_ids.push(page.read().await.id)
        }

        assert!(
            expected_ids == actual_ids,
            "\nExpected: {:?}\nGot: {:?}\n",
            expected_ids,
            actual_ids
        );

        Ok(())
    }
}
