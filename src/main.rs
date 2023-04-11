use std::collections::HashMap;
use std::io::Cursor;
use std::io::{
    self, BufRead, BufReader as SyncBufReader, BufWriter as SyncBufWriter, SeekFrom, Write,
};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

struct KeyData {
    file: String,
    value_s: u64,
    value_p: u64,
    time: u64,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut index: HashMap<String, KeyData> = HashMap::new();

    let mut stdin = SyncBufReader::new(io::stdin());
    let mut stdout = SyncBufWriter::new(io::stdout());
    let mut stderr = SyncBufWriter::new(io::stderr());

    let mut buf = String::new();
    loop {
        stdin.read_line(&mut buf)?;

        match Command::from_str(&buf) {
            Command::Insert(k, v) => {
                let log = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open("log") // TODO: get latest log file
                    .await?;

                let time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time before UNIX Epoch")
                    .as_secs();

                let mut entry: Vec<u8> = Vec::new();
                // timestamp, key len and value len occupy 8 bytes each
                entry.extend_from_slice(&time.to_be_bytes());
                entry.extend_from_slice(&(k.len()).to_be_bytes());
                entry.extend_from_slice(&(v.len()).to_be_bytes());
                // Key and Value:
                entry.extend_from_slice(k.as_bytes());
                entry.extend_from_slice(v.as_bytes());

                // The position of the entry will be the len of the file
                let position = log.metadata().await?.len();

                // Write the entry
                let mut writer = BufWriter::new(log);
                writer.write_all(&entry).await?;
                writer.flush().await?;

                // Insert the key and offset to index
                index.insert(
                    k.into(),
                    KeyData {
                        file: "log".into(), // TODO: get latest log file,
                        value_s: v.len() as u64,
                        value_p: position,
                        time,
                    },
                );

                stdout.write_all(b"OK\n")?;
                stdout.flush()?;
            }
            Command::Get(k) => {
                if let Some(offset) = index.get(k) {
                    let log = OpenOptions::new()
                        .read(true)
                        .open("log") // TODO: get latest log file
                        .await?;

                    // Find start of entry
                    let mut reader = BufReader::new(log);
                    reader.seek(SeekFrom::Start(offset.value_p)).await?;

                    // Read the timestamp, key len and value len
                    let _timestamp = reader.read_u64().await?;
                    let key_s = reader.read_u64().await?;
                    let value_s = reader.read_u64().await?;

                    // Read the key and value:
                    let mut key = vec![0; key_s as usize];
                    reader.read_exact(&mut key).await?;

                    let mut value = vec![0; value_s as usize];
                    reader.read_exact(&mut value).await?;

                    // Write to stdout
                    stdout.write(&key)?;
                    stdout.write(b" ")?;
                    stdout.write(&value)?;
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

enum Command<'a> {
    Insert(&'a str, &'a str),
    Get(&'a str),
    None,
}

impl<'a> Command<'a> {
    pub fn from_str(s: &'a str) -> Self {
        let split: Vec<&str> = s.split_whitespace().collect();

        if split.len() < 2 {
            return Command::None;
        }

        match split[0].to_lowercase().as_str() {
            "insert" => {
                if split.len() != 3 {
                    return Command::None;
                }

                Command::Insert(split[1], split[2])
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
