use std::{collections::HashMap, io, net::SocketAddr, ops::Range, sync::Arc, time::Duration};

use hash_db::{
    serverv2::{connection::Connection, message::Message},
    storagev2::{
        disk::Disk,
        key_dir::{self, KeyDir},
        page_manager::PageCache,
        test::CleanUp,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::RwLock,
};
use turmoil::{
    net::{TcpListener, TcpStream},
    Builder,
};

macro_rules! clients {
    ( $sim:ident, [$( ($client:expr, $range:expr) ),*] ) => {
        $(
            $sim.client($client, async {
                let mut socket = TcpStream::connect(("server", 4444)).await?;

                let inserts = generate_inserts($range);
                let mut buf = [0; 256];
                for (k, v) in &inserts {
                    socket
                        .write(format!("insert {} {}\n", k, v).as_bytes())
                        .await?;
                    socket.flush().await?;

                    let n = socket.read(&mut buf).await?;

                    assert!(&buf[0..n] == b"Success\n");
                }

                for (k, v) in &inserts {
                    socket.write(format!("get {}\n", k).as_bytes()).await?;
                    socket.flush().await?;

                    let n = socket.read(&mut buf).await?;

                    let exp = format!("{} {}\n", k, v);
                    let got = &buf[0..n];

                    assert!(
                        got == exp.as_bytes(),
                        "\nExpected: {}\nGot: {}\n",
                        exp,
                        std::str::from_utf8(got).unwrap()
                    );
                }

                Ok(())
            });
        )*
    };
}

fn generate_inserts(range: Range<u16>) -> HashMap<String, String> {
    let mut ret = HashMap::with_capacity(range.len());

    for i in range {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);

        ret.insert(key, value);
    }

    ret
}

const DB_FILE: &str = "main.db";

// FIXME:
// 1. Replacer is not replacing - LRUK nodes are initialised as unevictable but nothing sets them to
//    be evictible. Causes database to lock up after filling read page buffer.
// 2. Setting LRUK nodes to be evictable by default gets rid of the locking, but there is then an
//    issue where entries are being written into pages they wouldn't fit. This maybe cause of
//    asserts failing in the simulation.
fn main() -> io::Result<()> {
    let _cu = CleanUp::file(DB_FILE);

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(240))
        .build();

    sim.host("server", || async {
        run().await;

        Ok(())
    });

    clients!(sim, [("A", 0..1000), ("B", 1000..2000), ("C", 3000..4000)]);

    if let Err(e) = sim.run() {
        eprintln!("ERROR: {}", e.to_string());
    } else {
        eprintln!("SUCCESS");
    }

    Ok(())
}

// Mostly the same, tcp listener/stream swapped out for turmoil and signal handler removed
pub async fn run() {
    let disk = Disk::new(DB_FILE).await.expect("Failed to open db file");
    let (kd, latest) = key_dir::bootstrap(&disk).await;
    let kd = Arc::new(RwLock::new(kd));

    let m = PageCache::new(disk, 2, latest);

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
