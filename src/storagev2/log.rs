use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryType {
    Put,    // 0
    Delete, // 1
}

impl From<u8> for EntryType {
    fn from(value: u8) -> Self {
        match value {
            0 => EntryType::Put,
            1 => EntryType::Delete,
            _ => unreachable!(),
        }
    }
}

impl Into<u8> for EntryType {
    fn into(self) -> u8 {
        match self {
            EntryType::Put => 0,
            EntryType::Delete => 1,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Entry {
    pub t: EntryType,
    pub time: u64,
    pub key: BytesMut,
    pub value: BytesMut,
}

impl Entry {
    // t + time + key_s + value_s
    pub const METADATA_LEN: usize = 1 + 8 + 8 + 8;
    pub fn len(&self) -> usize {
        Self::METADATA_LEN + self.key.len() + self.value.len()
    }

    pub fn new(key: &[u8], value: &[u8], t: EntryType) -> Entry {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time before UNIX epoch")
            .as_secs();

        Entry {
            t,
            time,
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn as_bytes(&self) -> BytesMut {
        let mut ret = BytesMut::with_capacity(self.len());
        ret.put_u8(self.t.into());
        ret.put_u64(self.time);
        ret.put_u64(self.key.len() as u64);
        ret.put_u64(self.value.len() as u64);
        ret.put(self.key.clone());
        ret.put(self.value.clone());

        ret
    }
}
