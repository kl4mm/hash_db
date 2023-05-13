use std::io::{
    self, BufRead, BufReader as SyncBufReader, BufWriter as SyncBufWriter, SeekFrom, Write,
};
use std::time::{SystemTime, UNIX_EPOCH};

use hash_db::command::Command;
use hash_db::db;
use hash_db::entry::Entry;
use hash_db::key_dir::{self, KeyData};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

#[tokio::main]
async fn main() -> io::Result<()> {
    let key_dir = key_dir::bootstrap(db::DB_PATH).await?;

    let mut stdin = SyncBufReader::new(io::stdin());
    let mut stdout = SyncBufWriter::new(io::stdout());
    let mut stderr = SyncBufWriter::new(io::stderr());

    let mut buf = String::new();
    loop {
        stdin.read_line(&mut buf)?;

        match Command::from_str(&buf) {
            Command::Insert(k, v) => {
                let (file, position, file_path) = db::open_latest(&key_dir).await?;

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

                stdout.write_all(b"OK\n")?;
                stdout.flush()?;
            }
            Command::Delete(k) => {
                if let Some(key_data) = key_dir.write().await.remove(k.into()) {
                    let mut file = OpenOptions::new().write(true).open(&key_data.path).await?;

                    // Find start of entry
                    file.seek(SeekFrom::Start(key_data.pos)).await?;

                    // Write 1 in delete position of entry:
                    let mut writer = BufWriter::new(file);
                    writer.write_u8(1).await?;
                    writer.flush().await?;

                    // Write to stdout
                    stdout.write(b"OK\n")?;
                    stdout.flush()?;
                }
            }
            Command::Get(k) => {
                if let Some(key_data) = key_dir.read().await.get(k) {
                    let file = OpenOptions::new().read(true).open(&key_data.path).await?;

                    // Find start of entry
                    let mut reader = BufReader::new(file);
                    reader.seek(SeekFrom::Start(key_data.pos)).await?;

                    let entry = match Entry::read(&mut reader).await {
                        Some(e) => e,
                        None => {
                            stderr.write_all(b"There was a problem reading the entry\n")?;
                            stderr.flush()?;
                            continue;
                        }
                    };

                    // Write to stdout
                    stdout.write(&entry.key)?;
                    stdout.write(b" ")?;
                    stdout.write(&entry.value)?;
                    stdout.write(b"\n")?;
                    stdout.flush()?;
                }
            }
            Command::None => {
                stderr.write_all(b"Invalid Command. Usage:\nINSERT key value\nGET key\n")?;
                stderr.flush()?;
            }
        };

        buf.clear();
    }
}
