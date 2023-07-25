use std::{io::Cursor, str, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::storagev2::{
    key_dir::{KeyData, KeyDir},
    log::{Entry, EntryType},
    page::PageError,
    page_manager::PageManager,
};

#[derive(PartialEq)]
pub enum Message {
    Insert(Bytes, Bytes),
    Delete(Bytes),
    Get(Bytes),

    Result(Bytes, Bytes),

    Success,
    None,
}

impl Message {
    pub async fn exec(&self, m: &PageManager, kd: &Arc<RwLock<KeyDir>>) -> Message {
        match self {
            Message::Insert(k, v) => {
                let mut current = m.get_current().await;

                let entry = Entry::new(k, v, EntryType::Put);
                let offset = match current.write_entry(&entry) {
                    Ok(o) => o,
                    Err(e) if e == PageError::NotEnoughSpace => {
                        if let Err(e) = m.replace_page(&mut current).await {
                            todo!()
                        }

                        current.write_entry(&entry).unwrap()
                    }
                    Err(e) => {
                        todo!()
                    }
                };

                let data = KeyData::new(current.id, offset);
                kd.write().await.insert(k, data);

                Message::Success
            }
            Message::Delete(_) => todo!(),
            Message::Get(_) => todo!(),

            Message::Result(_, _) | Message::Success | Message::None => unreachable!(),
        }
    }

    pub fn parse(buf: &[u8]) -> Option<Self> {
        let mut buf = Cursor::new(buf);
        if buf.remaining() < 3 {
            return None;
        }

        let maybe_get = str::from_utf8(&buf.get_ref()[0..3]).unwrap();
        if maybe_get == "get" {
            let Some(key) = read_until(&buf, b'\n') else { return None };

            return Some(Message::Get(key.into()));
        }

        if buf.remaining() < 6 {
            return None;
        }
        let maybe_insert_or_delete = str::from_utf8(&buf.get_ref()[0..6]).unwrap();
        match maybe_insert_or_delete {
            "insert" => {
                let Some(key) = read_until(&buf, b' ') else { return None };
                buf.advance(key.len() + 1);
                let Some(value) = read_until(&buf, b'\n') else { return None };

                Some(Message::Insert(key, value))
            }
            "delete" => {
                let Some(key) = read_until(&buf, b'\n') else { return None };

                return Some(Message::Delete(key.into()));
            }
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Message::Insert(k, v) => 8 + k.len() + v.len(),
            Message::Delete(k) => 7 + k.len(),
            Message::Get(k) => 5 + k.len(),

            Message::Result(k, v) => k.len() + v.len() + 1,
            Message::Success => 8,
            Message::None => 0,
        }
    }
}

fn read_until(cursor: &Cursor<&[u8]>, c: u8) -> Option<Bytes> {
    let start = cursor.position() as usize;
    let end = cursor.get_ref().len() - 1;

    for i in start..end {
        if cursor.get_ref()[i] == c {
            let ret = BytesMut::from(&cursor.get_ref()[start..i]);
            let ret = Bytes::from(ret);
            return Some(ret);
        }
    }

    None
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        match self {
            Message::Insert(_, _) | Message::Delete(_) | Message::Get(_) | Message::None => {
                unreachable!()
            }

            Message::Result(mut k, mut v) => {
                // Might need to advance dst?
                let mut dst = BytesMut::with_capacity(k.len() + v.len() + 2);
                k.copy_to_slice(&mut dst);
                dst.put_u8(b'\0');
                v.copy_to_slice(&mut dst);
                dst.put_u8(b'\n');

                dst.into()
            }
            Message::Success => Bytes::from("Success\n"),
        }
    }
}
