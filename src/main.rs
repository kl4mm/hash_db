use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use hash_db::command::Command;
use hash_db::db;
use hash_db::key_dir;

const MAX_FILE_SIZE: u64 = 64;
const DB_PATH: &str = "db/";
const COMPACTION_INTERVAL: u64 = 5 * 60;

use crate::key_dir::KeyDir;

#[tokio::main]
async fn main() -> io::Result<()> {
    let key_dir = key_dir::bootstrap(DB_PATH, MAX_FILE_SIZE).await?;
    tokio::spawn(db::compaction_loop(
        key_dir.clone(),
        Duration::from_secs(COMPACTION_INTERVAL),
    ));

    let listener = TcpListener::bind("0.0.0.0:4444")
        .await
        .expect("could not bind address");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tokio::spawn(accept_loop(stream, addr, key_dir.clone()));
            }
            Err(e) => eprintln!("ERROR: {e}"),
        }
    }
}

async fn accept_loop(
    mut stream: TcpStream,
    addr: SocketAddr,
    key_dir: Arc<RwLock<KeyDir>>,
) -> io::Result<()> {
    eprintln!("Client connected: {}", addr);

    let (reader, writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    let mut buf = String::new();
    loop {
        reader.read_line(&mut buf).await?;

        if let Err(e) = Command::handle(buf.as_str().into(), &key_dir, &mut writer).await {
            eprintln!("Client disconnected: {}", addr);

            break Ok(());
        }

        buf.clear();
    }
}
