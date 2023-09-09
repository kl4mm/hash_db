use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use crate::storagev2::{
    disk::Disk,
    key_dir::KeyData,
    log::Entry,
    page::{Page, PageID},
    replacer::LrukReplacer,
};

#[derive(Debug)]
pub enum PageIndex {
    Write,
    Read(usize),
}

pub const DEFAULT_PAGE_SIZE: usize = 4 * 1024;
pub const DEFAULT_READ_SIZE: usize = 8;

#[derive(Clone)]
pub struct PageManager<
    const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE,
    const READ_SIZE: usize = DEFAULT_READ_SIZE,
> {
    disk: Arc<RwLock<Disk>>,
    page_table: Arc<RwLock<HashMap<PageID, PageIndex>>>, // Map page ids to index
    current: Arc<RwLock<Page<PAGE_SIZE>>>,
    read: Arc<RwLock<[Option<Page<PAGE_SIZE>>]>>,
    free: Arc<Mutex<Vec<usize>>>,
    next_id: Arc<AtomicU32>,
    replacer: Arc<Mutex<LrukReplacer>>,
}

impl<const PAGE_SIZE: usize, const READ_SIZE: usize> PageManager<PAGE_SIZE, READ_SIZE> {
    pub fn new(disk: Disk, lruk: usize, latest: Page<PAGE_SIZE>) -> Self {
        let next_id = latest.id + 1;
        let disk = Arc::new(RwLock::new(disk));
        let page_table = Arc::new(RwLock::new(HashMap::from([(latest.id, PageIndex::Write)])));
        let current = Arc::new(RwLock::new(latest));
        let read: Arc<RwLock<[_; READ_SIZE]>> =
            Arc::new(RwLock::new(std::array::from_fn(|_| None)));
        let next_id = Arc::new(AtomicU32::new(next_id));
        let free = Arc::new(Mutex::new((0..READ_SIZE).rev().collect()));
        let replacer = Arc::new(Mutex::new(LrukReplacer::new(lruk)));

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
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn replace_page(
        &self,
        current: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>,
    ) -> io::Result<()> {
        self.disk.write().await.write_page(&current)?;

        let mut page_table = self.page_table.write().await;

        let old_id = current.id;
        if let None = page_table.remove(&old_id) {
            eprintln!("No write page while replacing write page");
        }

        let id = self.inc_id();
        **current = Page::new(id);
        page_table.insert(id, PageIndex::Write);

        Ok(())
    }

    pub async fn new_page<'a>(&mut self) -> Option<PageID> {
        let i = if let Some(i) = self.free.lock().await.pop() {
            i
        } else {
            let Some(i) = self.replacer.lock().await.evict() else { return None };

            i
        };
        self.replacer.lock().await.record_access(i);

        let page_id = self.inc_id();
        let page = Page::<PAGE_SIZE>::new(page_id);
        page.pin();
        self.disk
            .write()
            .await
            .write_page(&page)
            .expect("Couldn't write page");
        self.page_table
            .write()
            .await
            .insert(page_id, PageIndex::Read(i));

        let id = page.id;
        self.read.write().await[i].replace(page);

        Some(id)
    }

    pub async fn fetch_entry(&self, kd: &KeyData) -> Option<Entry> {
        if let Some(i) = self.page_table.read().await.get(&kd.page_id) {
            return match i {
                PageIndex::Write => {
                    let page = self.current.as_ref().read().await;

                    page.read_entry(kd.offset as usize)
                }
                PageIndex::Read(i) => {
                    assert!(*i < READ_SIZE);
                    self.replacer.lock().await.record_access(*i);

                    let pages = self.read.read().await;
                    let page = pages[*i].as_ref().expect("Invalid page index in table");

                    page.read_entry(kd.offset as usize)
                }
            };
        };

        let i = if let Some(i) = self.free.lock().await.pop() {
            i
        } else {
            // TODO: Should return an error here
            let Some(i) = self.replacer.lock().await.evict() else { return None };
            self.replacer.lock().await.record_access(i);

            i
        };

        assert!(i < READ_SIZE);
        if let Some(_) = &self.read.read().await[i] {
            self.page_table.write().await.remove(&kd.page_id);
        }

        let page = self
            .disk
            .read()
            .await
            .read_page::<PAGE_SIZE>(kd.page_id)
            .expect("Couldn't read page");

        let entry = page.read_entry(kd.offset as usize);
        self.page_table
            .write()
            .await
            .insert(page.id, PageIndex::Read(i));
        self.read.write().await[i].replace(page);

        entry
    }

    pub async fn unpin_page(&mut self, page_id: PageID) {
        let page_table = self.page_table.read().await;
        let Some(i) = page_table.get(&page_id) else { return };

        let i = match i {
            PageIndex::Read(i) => *i,
            _ => unreachable!("Can't unpin write page"),
        };
        drop(page_table);

        let pages = self.read.read().await;
        let page = pages[i].as_ref().expect("Invalid page index in table");

        if page.pins.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.replacer.lock().await.set_evictable(i, true);
        }
    }

    pub async fn get_current(&self) -> RwLockWriteGuard<Page<PAGE_SIZE>> {
        self.current.write().await
    }

    pub async fn flush_current(&mut self) {
        let current = self.current.write().await;
        self.disk
            .write()
            .await
            .write_page(&current)
            .expect("Couldn't write page");
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
        page_manager::{PageManager, DEFAULT_PAGE_SIZE, DEFAULT_READ_SIZE},
        test::CleanUp,
    };

    #[tokio::test]
    async fn test_page_manager() -> io::Result<()> {
        const DB_FILE: &str = "./test_page_manager.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let m = PageManager::<DEFAULT_PAGE_SIZE, DEFAULT_READ_SIZE>::new(disk, 2, Page::new(0));

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

    #[tokio::test]
    async fn test_replacer() -> io::Result<()> {
        const DB_FILE: &str = "./test_replacer.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let mut m = PageManager::<DEFAULT_PAGE_SIZE, 3>::new(disk, 2, Page::new(0));

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

        m.unpin_page(1).await;
        m.unpin_page(1).await;
        m.unpin_page(1).await;
        m.unpin_page(1).await;
        m.unpin_page(1).await;

        m.unpin_page(2).await;
        m.unpin_page(2).await;
        m.unpin_page(2).await;
        m.unpin_page(2).await;

        m.unpin_page(3).await;
        m.unpin_page(3).await;

        let new_page_id = m.new_page().await.expect("a page should have been evicted");
        assert!(new_page_id == 4, "Got: {}", new_page_id);

        let pages = m.read.read().await;
        let expected_ids = vec![1, 2, 4];
        let mut actual_ids = Vec::new();
        for page in pages.iter() {
            if let Some(page) = page {
                actual_ids.push(page.id)
            }
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
