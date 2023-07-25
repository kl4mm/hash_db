use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};

use crate::serverv2::message::Message;

pub struct Connection<R, W> {
    r: R,
    w: W,
    buf: bytes::BytesMut,
}

impl<R, W> Connection<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(r: R, w: W) -> Self {
        let buf = BytesMut::with_capacity(4 * 1024);

        Self { r, w, buf }
    }

    pub async fn read(&mut self) -> io::Result<Option<Message>> {
        loop {
            if let Some(message) = Message::parse(&self.buf) {
                self.buf.advance(message.len());

                return Ok(Some(message));
            }

            if 0 == self.r.read_buf(&mut self.buf).await? {
                return Err(io::Error::from(io::ErrorKind::ConnectionReset));
            }
        }
    }

    pub async fn write(&mut self, m: Message) -> io::Result<()> {
        let b: Bytes = m.into();
        self.w.write_all(&b).await?;
        self.w.flush().await?;

        Ok(())
    }
}
