use std::{io, net::SocketAddr, sync::Arc};

use crate::{
    serverv2::{connection::Connection, message::Message},
    storagev2::{
        disk::Disk,
        key_dir::{self, KeyDir},
        page_manager::PageCache,
    },
};
use tokio::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    signal,
    sync::RwLock,
};

const DB_FILE: &str = "main.db";

pub async fn run() {
    let disk = Disk::new(DB_FILE).await.expect("Failed to open db file");
    let (kd, latest) = key_dir::bootstrap(&disk).await;
    let kd = Arc::new(RwLock::new(kd));

    let m = PageCache::new(disk, 2, latest);

    let listener = TcpListener::bind("0.0.0.0:4444")
        .await
        .expect("Could not bind");

    let mut _m = m.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            eprintln!("signal error: {}", e);
        }

        _m.flush_current().await;
        std::process::exit(0);
    });

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tokio::spawn(accept(stream, addr, m.clone(), kd.clone()));
            }
            Err(e) => eprintln!("ERROR: {}", e),
        }
    }
}

async fn accept(stream: TcpStream, addr: SocketAddr, pc: PageCache, kd: Arc<RwLock<KeyDir>>) {
    if let Err(e) = accept_loop(stream, addr, pc, kd).await {
        eprintln!("ERROR: {}", e);
    }
}

async fn accept_loop(
    stream: TcpStream,
    _addr: SocketAddr,
    pc: PageCache,
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

        let res = message.exec(&pc, &kd).await;

        conn.write(res).await?;
    }
}
