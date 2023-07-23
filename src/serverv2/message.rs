use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::storagev2::{key_dir::KeyDir, page_manager::PageManager};

pub enum Message {
    Insert(Bytes, Bytes),
    Delete(Bytes),
    Get(Bytes),

    Result(Bytes, Bytes),

    None,
}

impl Message {
    pub async fn exec(&self, m: &PageManager, kd: &Arc<RwLock<KeyDir>>) -> Message {
        match self {
            Message::Insert(_, _) => todo!(),
            Message::Delete(_) => todo!(),
            Message::Get(_) => todo!(),

            Message::Result(_, _) => Message::None,
            Message::None => Message::None,
        }
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        match self {
            Message::Insert(_, _) => todo!(),
            Message::Delete(_) => todo!(),
            Message::Get(_) => todo!(),
            Message::Result(mut k, mut v) => {
                // Might need to advance dst?
                let mut dst = BytesMut::with_capacity(k.len() + v.len() + 2);
                k.copy_to_slice(&mut dst);
                dst.put_u8(b'\0');
                v.copy_to_slice(&mut dst);
                dst.put_u8(b'\n');

                dst.into()
            }
            Message::None => todo!(),
        }
    }
}
