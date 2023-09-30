use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering::*};

use bytes::Buf;

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

#[derive(Debug)]
pub struct Page {
    pub id: PageID,
    pub data: [u8; PAGE_SIZE],
    pub pins: AtomicI32,
    len: AtomicUsize,
}

impl Page {
    pub fn new(id: PageID) -> Self {
        let data = [0; PAGE_SIZE];
        let pins = AtomicI32::new(0);
        let len = AtomicUsize::new(0);

        Self {
            id,
            data,
            pins,
            len,
        }
    }

    pub fn from_bytes(id: PageID, data: [u8; PAGE_SIZE]) -> Self {
        let mut empty = 0;
        for i in (0..data.len()).rev() {
            if data[i] != b'\0' {
                break;
            }

            empty += 1;
        }

        let pins = AtomicI32::new(0);
        let len = AtomicUsize::new(PAGE_SIZE - empty);
        Self {
            id,
            data,
            pins,
            len,
        }
    }

    pub fn write_entry(&mut self, entry: &Entry) -> Result<u64, PageError> {
        let len = entry.len();
        let offset = self.len.fetch_add(len, SeqCst);
        if offset + len > PAGE_SIZE {
            return Err(PageError::NotEnoughSpace);
        }

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

    pub fn pin(&self) {
        self.pins.fetch_add(1, SeqCst);
    }
}
