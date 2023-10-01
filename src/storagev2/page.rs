use bytes::Buf;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::storagev2::log::Entry;

#[cfg(not(test))]
pub const PAGE_SIZE: usize = 4 * 1024;

#[cfg(test)]
pub const PAGE_SIZE: usize = 256;

pub type PageID = u32;

#[macro_export]
macro_rules! put_bytes {
    ($dst:expr, $src:expr, $o:expr, $l:expr) => {
        $dst[$o as usize..$o as usize + $l as usize].copy_from_slice(&$src);
    };
}

macro_rules! get_bytes {
    ($src:expr, $o:expr, $l:expr) => {
        &$src[$o as usize..$o as usize + $l as usize]
    };
}

#[derive(Debug, PartialEq)]
pub enum PageError {
    NotEnoughSpace,
}

pub struct Page(RwLock<PageInner>);

impl Page {
    pub fn new(id: PageID) -> Self {
        Self(RwLock::new(PageInner::new(id)))
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, PageInner> {
        self.0.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, PageInner> {
        self.0.write().await
    }
}

impl Default for Page {
    fn default() -> Self {
        Self(RwLock::new(PageInner::default()))
    }
}

#[derive(Debug)]
pub struct PageInner {
    pub id: PageID,
    pub data: [u8; PAGE_SIZE],
    len: usize,
}

impl Default for PageInner {
    fn default() -> Self {
        Self {
            id: 0,
            data: [0; PAGE_SIZE],
            len: 0,
        }
    }
}

impl PageInner {
    pub fn new(id: PageID) -> Self {
        let data = [0; PAGE_SIZE];
        let len = 0;

        Self { id, data, len }
    }

    pub fn from_bytes(id: PageID, data: [u8; PAGE_SIZE]) -> Self {
        let mut empty = 0;

        const WINDOW: usize = 8;
        for w in data.windows(WINDOW).rev() {
            if u64::from_be_bytes(w.try_into().unwrap()) == 0 {
                empty += WINDOW;
            }
        }

        let len = PAGE_SIZE - empty;
        Self { id, data, len }
    }

    pub fn write_entry(&mut self, entry: &Entry) -> Result<u64, PageError> {
        let len = entry.len();

        let offset = self.len;
        if offset + len > PAGE_SIZE {
            return Err(PageError::NotEnoughSpace);
        }
        self.len += len;

        put_bytes!(self.data, entry.as_bytes(), offset, len);

        Ok(offset as u64)
    }

    // TODO: handle invalid bounds
    pub fn read_entry(&self, offset: usize) -> Option<Entry> {
        let mut src = &self.data[offset..];

        let rm = offset + Entry::METADATA_LEN;
        if rm >= PAGE_SIZE {
            return None;
        }

        let t = src.get_u8();
        let time = src.get_u64();
        let key_len = src.get_u64();
        let value_len = src.get_u64();

        // if rm + (key_len + value_len) as usize > PAGE_SIZE {
        //     eprintln!("ERROR: log entry was written that exceeded page size");
        //     return None;
        // }

        if time == 0 && key_len == 0 && value_len == 0 {
            return None;
        }

        // let rest = &src[0..];
        let key = get_bytes!(&src[0..], 0, key_len);
        let value = get_bytes!(&src[0..], key_len as usize, value_len);

        Some(Entry {
            t: t.into(),
            time,
            key: key.into(),
            value: value.into(),
        })
    }

    pub fn reset(&mut self) {
        self.data = [0; PAGE_SIZE];
        self.len = 0;
    }
}
