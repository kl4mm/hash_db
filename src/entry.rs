use std::{collections::HashMap, path::PathBuf};

use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt};

pub struct Entry {
    pub delete: bool,
    time: u64,
    key_s: u64,
    value_s: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,

    pos: u64,
}

pub struct KeyData {
    pub file: PathBuf,
    pub value_s: u64,
    pub pos: u64,
    pub time: u64,
}

impl Entry {
    pub async fn read<T>(reader: &mut T) -> Option<Entry>
    where
        T: AsyncBufRead + AsyncSeekExt + Unpin,
    {
        let pos = reader.stream_position().await.unwrap();

        // First byte indicates if entry was deleted
        let delete = match reader.read_u8().await {
            Ok(d) if d == 0 => false,
            Ok(d) if d == 1 => true,
            Ok(_) => panic!("Delete is neither 0 nor 1"),
            Err(_) => return None,
        };

        // Read the timestamp, key len and value len
        let time = match reader.read_u64().await {
            Ok(t) => t,
            Err(_) => return None,
        };

        let key_s = match reader.read_u64().await {
            Ok(t) => t,
            Err(_) => return None,
        };
        let value_s = match reader.read_u64().await {
            Ok(t) => t,
            Err(_) => return None,
        };

        // Read the key and value:
        let mut key = vec![0; key_s as usize];
        match reader.read_exact(&mut key).await {
            Ok(t) => t,
            Err(_) => return None,
        };

        let mut value = vec![0; value_s as usize];
        match reader.read_exact(&mut value).await {
            Ok(t) => t,
            Err(_) => return None,
        };

        Some(Self {
            delete,
            time,
            key_s,
            value_s,
            key,
            value,

            pos,
        })
    }

    pub fn new_bytes(k: &str, v: &str, time: u64) -> Vec<u8> {
        let mut entry: Vec<u8> = Vec::new();
        // Delete
        entry.extend_from_slice(&[0]);

        // Timestamp, key len and value len occupy 8 bytes each
        entry.extend_from_slice(&time.to_be_bytes());
        entry.extend_from_slice(&(k.len()).to_be_bytes());
        entry.extend_from_slice(&(v.len()).to_be_bytes());

        // Key and Value:
        entry.extend_from_slice(k.as_bytes());
        entry.extend_from_slice(v.as_bytes());

        entry
    }

    pub fn add_to_index(&self, index: &mut HashMap<String, KeyData>, file: PathBuf) {
        let key = std::str::from_utf8(&self.key).unwrap().to_string();
        let key_data = KeyData {
            file,
            value_s: self.value_s,
            pos: self.pos,
            time: self.time,
        };

        index.insert(key, key_data);
    }
}
