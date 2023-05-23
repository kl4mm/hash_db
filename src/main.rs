use std::io::{self, BufRead, BufReader as SyncBufReader, BufWriter as SyncBufWriter};

use hash_db::command::Command;
use hash_db::db;
use hash_db::key_dir;

const MAX_FILE_SIZE: u64 = 64;
const DB_PATH: &str = "db/";

#[tokio::main]
async fn main() -> io::Result<()> {
    let key_dir = key_dir::bootstrap(DB_PATH, MAX_FILE_SIZE).await?;

    let mut stdin = SyncBufReader::new(io::stdin());
    let mut stdout = SyncBufWriter::new(io::stdout());

    let mut buf = String::new();
    loop {
        stdin.read_line(&mut buf)?;

        if let Err(e) = Command::handle(buf.as_str().into(), &key_dir, &mut stdout).await {
            eprintln!("ERROR: Command handle failed: {e}")
        }

        buf.clear();
    }
}
