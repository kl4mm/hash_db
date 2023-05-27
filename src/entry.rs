use std::io;

use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub struct Entry {
    pub delete: bool,
    pub time: u64,
    pub key_s: u64,
    pub value_s: u64,
    pub key: String,
    pub value: String,

    pub pos: u64,
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
        if let Err(_) = reader.read_exact(&mut key).await {
            return None;
        }
        let key = match std::str::from_utf8(&key) {
            Ok(k) => k.to_owned(),
            Err(_) => return None,
        };

        let mut value = vec![0; value_s as usize];
        if let Err(_) = reader.read_exact(&mut value).await {
            return None;
        }
        let value = match std::str::from_utf8(&value) {
            Ok(v) => v.to_owned(),
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

    pub async fn write<T>(&self, writer: &mut T) -> io::Result<()>
    where
        T: AsyncWriteExt + Unpin,
    {
        writer.write_u8(self.delete as u8).await?;
        writer.write_u64(self.time).await?;
        writer.write_u64(self.key_s).await?;
        writer.write_u64(self.value_s).await?;
        writer.write(self.key.as_bytes()).await?;
        writer.write(self.value.as_bytes()).await?;
        writer.flush().await?;

        Ok(())
    }

    pub async fn write_new<T>(
        writer: &mut T,
        k: &str,
        v: &str,
        time: u64,
        delete: bool,
    ) -> io::Result<()>
    where
        T: AsyncWriteExt + Unpin,
    {
        writer.write_u8(delete as u8).await?;
        writer.write_u64(time).await?;
        writer.write_u64(k.len() as u64).await?;
        writer.write_u64(v.len() as u64).await?;
        writer.write(k.as_bytes()).await?;
        writer.write(v.as_bytes()).await?;
        writer.flush().await?;

        Ok(())
    }
}
