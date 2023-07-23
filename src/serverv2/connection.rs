use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

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
        // loop {
        //     if let Some(message) = Message::parse(&self.buf) {
        //         self.buf.advance(message.len());

        //         return Ok(Some(message));
        //     }

        //     if 0 == self.reader.read_buf(&mut self.buf).await? {
        //         // if self.buf.is_empty() {
        //         //     return Ok(None);
        //         // } else {
        //         return Err(io::Error::from(io::ErrorKind::ConnectionReset));
        //         // }
        //     }
        // }
        todo!()
    }

    pub async fn write(&mut self, b: &[u8]) -> io::Result<()> {
        // match message {
        //     Message::Query { raw } => {
        //         log::info!("Executing: {}", raw);
        //     }
        //     Message::Error { message } => {
        //         self.writer.write_u16(message::ERROR).await?;
        //         self.writer.write_u16(message.len() as u16).await?;
        //         self.writer.write(message.as_bytes()).await?;
        //         self.writer.flush().await?;
        //     }
        // }
        // Ok(())

        todo!()
    }
}
