use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use hash_db::{
    serverv2::{connection::Connection, message::Message},
    storagev2::{
        disk::Disk,
        key_dir::{self, KeyDir},
        page_manager::{PageManager, DEFAULT_PAGE_SIZE, DEFAULT_READ_SIZE},
    },
};
use tokio::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

const DB_FILE: &str = "main.db";

#[tokio::main]
async fn main() {
    let disk = Disk::new(DB_FILE).await.expect("Failed to open db file");
    let kd = key_dir::bootstrap::<DEFAULT_PAGE_SIZE>(&disk).await;
    let kd = Arc::new(RwLock::new(kd));

    // TODO: latest page params?
    let m = PageManager::<DEFAULT_PAGE_SIZE, DEFAULT_READ_SIZE>::new(disk);

    let listener = TcpListener::bind("0.0.0.0:4444")
        .await
        .expect("Could not bind");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tokio::spawn(accept(stream, addr, m.clone(), kd.clone()));
            }
            Err(e) => eprintln!("ERROR: {}", e),
        }
    }
}

pub async fn accept(stream: TcpStream, addr: SocketAddr, m: PageManager, kd: Arc<RwLock<KeyDir>>) {
    if let Err(e) = accept_loop(stream, addr, m, kd).await {
        eprintln!("ERROR: {}", e);
    }
}

pub async fn accept_loop(
    stream: TcpStream,
    addr: SocketAddr,
    mut m: PageManager,
    kd: Arc<RwLock<KeyDir>>,
) -> io::Result<()> {
    let (reader, writer) = stream.into_split();
    let reader = BufReader::new(reader);
    let writer = BufWriter::new(writer);

    let mut conn = Connection::new(reader, writer);

    loop {
        let message = match conn.read().await? {
            Some(m) if m == Message::None => continue,
            Some(m) => m,
            None => continue,
        };

        let res = message.exec(&m, &kd).await;

        conn.write(res).await?;
    }
}
