use std::{io::Cursor, sync::Arc};

use bytes::{Buf, Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::storagev2::{
    key_dir::{KeyData, KeyDir},
    log::{Entry, EntryType},
    page::PageError,
    page_manager::PageCache,
};

#[derive(Debug, PartialEq)]
pub enum Message {
    Insert(Bytes, Bytes),
    Delete(Bytes),
    Get(Bytes),

    Result(Bytes, Bytes),

    Success,
    Ignore(usize),
    None,
}

impl Message {
    pub async fn exec(&self, m: &PageCache, kd: &Arc<RwLock<KeyDir>>) -> Message {
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
            Message::Delete(k) => {
                let mut current = m.get_current().await;

                let entry = Entry::new(k, &[], EntryType::Delete);
                if let Err(e) = current.write_entry(&entry) {
                    if e == PageError::NotEnoughSpace {
                        if let Err(e) = m.replace_page(&mut current).await {
                            todo!()
                        }
                        current.write_entry(&entry).unwrap();
                    } else {
                        todo!()
                    }
                };

                kd.write().await.remove(k);

                Message::Success
            }
            Message::Get(k) => {
                let kd = kd.read().await;
                let Some(data) = kd.get(k) else { return Message::None };

                // TODO: return error if replacer couldn't replace
                let Some(entry) = m.fetch_entry(data).await else { return Message::None };

                Message::Result(entry.key.into(), entry.value.into())
            }

            Message::Result(_, _) | Message::Success | Message::Ignore(_) | Message::None => {
                Message::None
            }
        }
    }

    pub fn parse(buf: &[u8]) -> Option<Self> {
        let mut buf = Cursor::new(buf);

        if buf.get_ref()[..].starts_with(b"\n") {
            return Some(Message::Ignore(1));
        }

        // check for "get " first
        if buf.remaining() <= 4 {
            return None;
        }

        let maybe_get = &buf.get_ref()[0..3];
        if maybe_get == b"get" {
            buf.advance(4);
            let Some(key) = read_until(&buf, b'\n') else { return None };

            return Some(Message::Get(key.into()));
        }

        // check for "insert " or "delete "
        if buf.remaining() < 7 {
            return None;
        }
        let maybe_insert_or_delete = &buf.get_ref()[0..6];
        match maybe_insert_or_delete {
            b"insert" => {
                buf.advance(7);
                let Some(key) = read_until(&buf, b' ') else { return None };
                buf.advance(key.len() + 1);
                let Some(value) = read_until(&buf, b'\n') else { return None };

                Some(Message::Insert(key, value))
            }
            b"delete" => {
                buf.advance(7);
                let Some(key) = read_until(&buf, b'\n') else { return None };

                return Some(Message::Delete(key.into()));
            }
            _ => {
                return Some(Message::Ignore(buf.get_ref().len()));
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Message::Insert(k, v) => 9 + k.len() + v.len(),
            Message::Delete(k) => 7 + k.len(),
            Message::Get(k) => 5 + k.len(),

            Message::Result(k, v) => k.len() + v.len() + 1,
            Message::Success => 8,
            Message::Ignore(l) => *l,
            Message::None => 0,
        }
    }
}

fn read_until(cursor: &Cursor<&[u8]>, c: u8) -> Option<Bytes> {
    let start = cursor.position() as usize;
    let end = cursor.get_ref().len();

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
            Message::Insert(_, _)
            | Message::Delete(_)
            | Message::Get(_)
            | Message::Ignore(_)
            | Message::None => Bytes::new(),

            Message::Result(k, v) => {
                let len = k.len() + v.len() + 2;
                let mut dst = BytesMut::zeroed(len);

                crate::put_bytes!(dst, k, 0, k.len());
                dst[k.len()] = b' ';
                crate::put_bytes!(dst, v, k.len() + 1, v.len());
                dst[len - 1] = b'\n';

                dst.into()
            }
            Message::Success => Bytes::from("Success\n"),
        }
    }
}
