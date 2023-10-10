use std::{collections::HashMap, io, ops::Range, sync::Arc, time::Duration};

use hash_db::storagev2::test::CleanUp;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::ToSocketAddrs,
    sync::Notify,
    time::Instant,
};

macro_rules! client {
    ($id:expr, $addr:expr, $range:expr) => {
        tokio::spawn(async {
            let ins = Instant::now();

            let c = Client::new($id, $addr, $range);
            if let Err(e) = c.run().await {
                eprintln!("{} failed with error: {}", c.id, e);
            }

            eprintln!("{} finished in {:?}", c.id, ins.elapsed());
        });
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

struct Client<T: ToSocketAddrs> {
    id: u8,
    addr: T,
    range: Range<u16>,
}

impl<T: ToSocketAddrs> Client<T> {
    pub fn new(id: u8, addr: T, range: Range<u16>) -> Self {
        Self { id, addr, range }
    }

    pub async fn run(&self) -> io::Result<()> {
        let mut socket = tokio::net::TcpStream::connect(&self.addr).await?;

        let inserts = generate_inserts(self.range.clone());
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
    }
}

#[tokio::main]
pub async fn main() -> io::Result<()> {
    let _cu = CleanUp::file(DB_FILE);

    let notify = Arc::new(Notify::new());

    let sh_notify = notify.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = hash_db::serverv2::server::run() => {}
            _ = sh_notify.notified() => {
                eprintln!("shutting down server");
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;

    client!(0, "0.0.0.0:4444", 0..1000);
    client!(1, "0.0.0.0:4444", 1000..2000);
    client!(2, "0.0.0.0:4444", 3000..4000);

    tokio::time::sleep(Duration::from_secs(1)).await;
    notify.notify_one();

    Ok(())
}
