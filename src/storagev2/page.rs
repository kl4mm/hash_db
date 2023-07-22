use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use bytes::{Buf, BytesMut};

use crate::storagev2::log::Entry;

pub type PageID = u32;

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

#[derive(Debug)]
pub enum PageError {
    NotEnoughSpace,
}

#[derive(Debug)]
pub struct Page<const SIZE: usize> {
    pub id: PageID,
    pub data: BytesMut,
    pub pins: AtomicU32,
    len: AtomicUsize,
}

impl<const SIZE: usize> Page<SIZE> {
    pub fn new(id: PageID) -> Self {
        let data = BytesMut::zeroed(SIZE);
        let pins = AtomicU32::new(0);
        let len = AtomicUsize::new(0);

        Self {
            id,
            data,
            pins,
            len,
        }
    }

    pub fn from_bytes(id: PageID, data: BytesMut, len: usize) -> Self {
        let pins = AtomicU32::new(0);
        let len = AtomicUsize::new(len);

        Self {
            id,
            data,
            pins,
            len,
        }
    }

    pub fn write_entry(&mut self, entry: &Entry) -> Result<usize, PageError> {
        let len = entry.len();
        let offset = self.len.fetch_add(len, Ordering::Relaxed);
        if offset + len > SIZE {
            return Err(PageError::NotEnoughSpace);
        }

        put_bytes!(self.data, entry.as_bytes(), offset, len);

        Ok(offset)
    }

    // TODO: handle invalid bounds
    pub fn read_entry(&self, offset: usize) -> Option<Entry> {
        let mut src = BytesMut::from(&self.data[offset..]);

        let rm = offset + Entry::METADATA_LEN;
        if rm >= SIZE {
            return None;
        }

        let t = src.get_u8();
        let time = src.get_u64();
        let key_len = src.get_u64();
        let value_len = src.get_u64();

        if rm + (key_len + value_len) as usize > SIZE {
            eprintln!("log entry was written that exceeded page size");
            return None;
        }

        if time == 0 && key_len == 0 && value_len == 0 {
            return None;
        }

        let rest = &src[0..];
        let key = get_bytes!(rest, 0, key_len);
        let value = get_bytes!(rest, key_len as usize, value_len);

        Some(Entry {
            t: t.into(),
            time,
            key: key.into(),
            value: value.into(),
        })
    }

    pub fn pin(&self) {
        self.pins.fetch_add(1, Ordering::Relaxed);
    }
}
