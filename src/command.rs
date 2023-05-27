use std::{
    io::{self, SeekFrom},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    sync::RwLock,
};

use crate::{
    db,
    entry::Entry,
    key_dir::{KeyData, KeyDir},
};

pub enum Command<'a> {
    Insert(&'a str, &'a str),
    Delete(&'a str),
    Get(&'a str),
    None,
}

impl<'a> Into<Command<'a>> for &'a str {
    fn into(self) -> Command<'a> {
        let split: Vec<&str> = self.split_whitespace().collect();

        if split.len() < 2 || split.len() > 3 {
            return Command::None;
        }

        match split[0].to_lowercase().as_str() {
            "insert" => {
                if split.len() != 3 {
                    return Command::None;
                }

                Command::Insert(split[1], split[2])
            }
            "delete" => {
                if split.len() != 2 {
                    return Command::None;
                }

                Command::Delete(split[1])
            }
            "get" => {
                if split.len() != 2 {
                    return Command::None;
                }

                Command::Get(split[1])
            }
            _ => Command::None,
        }
    }
}

impl<'a> Command<'a> {
    pub async fn handle<T>(
        command: Command<'a>,
        key_dir: &Arc<RwLock<KeyDir>>,
        write_to: &mut T,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        match command {
            Command::Insert(k, v) => Self::insert(key_dir, write_to, k, v).await?,
            Command::Delete(k) => Self::delete(key_dir, write_to, k).await?,
            Command::Get(k) => Self::get(key_dir, write_to, k).await?,
            Command::None => {
                write_to
                    .write_all(b"Invalid Command. Usage:\nINSERT key value\nGET key\n")
                    .await?;
                write_to.flush().await?;
            }
        }

        Ok(())
    }

    #[inline]
    pub async fn insert<T>(
        key_dir: &Arc<RwLock<KeyDir>>,
        write_to: &mut T,
        k: &str,
        v: &str,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        let (file, position, file_path) = db::open_latest(key_dir).await?;

        // Get current time
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time before UNIX Epoch")
            .as_secs();

        // Get entry bytes
        let entry = Entry::new_bytes(k, v, time);

        // Write the entry
        let mut writer = BufWriter::new(file);
        writer.write_all(&entry).await?;
        writer.flush().await?;

        // Insert the key and offset to index
        key_dir.write().await.insert(
            k.into(),
            KeyData {
                path: file_path,
                value_s: v.len() as u64,
                pos: position,
                time,
            },
        );

        write_to.write_all(b"OK\n").await?;
        write_to.flush().await?;

        Ok(())
    }

    #[inline]
    pub async fn get<T>(key_dir: &Arc<RwLock<KeyDir>>, write_to: &mut T, k: &str) -> io::Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        if let Some(key_data) = key_dir.read().await.get(k) {
            let file = OpenOptions::new().read(true).open(&key_data.path).await?;

            // Find start of entry
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(key_data.pos)).await?;

            let entry = match Entry::read(&mut reader).await {
                Some(e) => e,
                None => {
                    write_to
                        .write_all(b"There was a problem reading the entry\n")
                        .await?;
                    write_to.flush().await?;
                    return Ok(());
                }
            };

            write_to.write(&entry.key.as_bytes()).await?;
            write_to.write(b" ").await?;
            write_to.write(&entry.value.as_bytes()).await?;
            write_to.write(b"\n").await?;
            write_to.flush().await?;
        }

        Ok(())
    }

    #[inline]
    pub async fn delete<T>(
        key_dir: &Arc<RwLock<KeyDir>>,
        write_to: &mut T,
        k: &str,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        if let Some(key_data) = key_dir.write().await.remove(k.into()) {
            let mut file = OpenOptions::new().write(true).open(&key_data.path).await?;

            // Find start of entry
            file.seek(SeekFrom::Start(key_data.pos)).await?;

            // Write 1 in delete position of entry:
            let mut writer = BufWriter::new(file);
            writer.write_u8(1).await?;
            writer.flush().await?;

            // Write to stdout
            write_to.write(b"OK\n").await?;
            write_to.flush().await?;
        }

        Ok(())
    }
}
