use std::collections::HashMap;
use std::io::{
    self, BufRead, BufReader as SyncBufReader, BufWriter as SyncBufWriter, SeekFrom, Write,
};
use std::time::{SystemTime, UNIX_EPOCH};

use hash_db::command::Command;
use hash_db::entry::{Entry, KeyData};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut index = bootstrap().await?;

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

                // Get current time
                let time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time before UNIX Epoch")
                    .as_secs();

                // Get entry bytes
                let entry = Entry::new_bytes(k, v, time);

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
                        pos: position,
                        time,
                    },
                );

                stdout.write_all(b"OK\n")?;
                stdout.flush()?;
            }
            Command::Get(k) => {
                if let Some(key_data) = index.get(k) {
                    let log = OpenOptions::new()
                        .read(true)
                        .open(&key_data.file) // TODO: get latest log file
                        .await?;

                    // Find start of entry
                    let mut reader = BufReader::new(log);
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

pub async fn bootstrap() -> io::Result<HashMap<String, KeyData>> {
    let mut ret = HashMap::new();

    // TODO: loop over directory, check for hint files, then read log files
    let log = OpenOptions::new().read(true).open("log").await?;
    let mut reader = BufReader::new(log);

    while let Some(entry) = Entry::read(&mut reader).await {
        entry.add_to_index("log".into(), &mut index);
    }

    Ok(ret)
}
