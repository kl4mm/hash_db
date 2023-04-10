use std::collections::HashMap;
use std::io::{
    self, BufRead, BufReader as SyncBufReader, BufWriter as SyncBufWriter, SeekFrom, Write,
};

use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut index: HashMap<String, u64> = HashMap::new();

    let mut stdin = SyncBufReader::new(io::stdin());
    let mut stdout = SyncBufWriter::new(io::stdout());
    let mut stderr = SyncBufWriter::new(io::stderr());
    let mut log = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open("log")
        .await?;

    let mut buf = String::new();
    loop {
        stdin.read_line(&mut buf)?;

        match Command::from_str(&buf) {
            Command::Insert(k, v) => {
                let entry = format!("{k}:{v}\n");

                // The offset of the entry will be the len of the file
                let offset = log.metadata().await?.len();

                // Write the entry
                let mut writer = BufWriter::new(log);
                writer.write_all(entry.as_bytes()).await?;
                writer.flush().await?;

                // Insert the key and offset to index
                index.insert(k.into(), offset);

                stdout.write_all(b"OK\n")?;

                log = writer.into_inner();
            }
            Command::Get(k) => {
                if let Some(offset) = index.get(k) {
                    let mut entry = String::new();

                    // Read from the offset until \n
                    let mut reader = BufReader::new(log);
                    reader.seek(SeekFrom::Start(*offset)).await?;
                    reader.read_line(&mut entry).await?;

                    // Split with : to get the value
                    let value = match entry.split_once(':') {
                        Some((_, v)) => v,
                        None => {
                            stderr.write(b"There was an error writing this entry\n")?;
                            stderr.flush()?;

                            log = reader.into_inner();
                            continue;
                        }
                    };

                    stdout.write(value.as_bytes())?;
                    stdout.flush()?;

                    log = reader.into_inner();
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
