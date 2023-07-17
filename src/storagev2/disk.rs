use std::{io, os::fd::AsRawFd, path::Path};

use bytes::BytesMut;
use nix::sys::uio;
use tokio::fs::{File, OpenOptions};

use crate::storagev2::page::{Page, PageID};

pub struct Disk {
    file: File,
}

impl Disk {
    pub async fn new(file: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .await?;

        Ok(Self { file })
    }

    pub fn read_page<const SIZE: usize>(&self, page_id: PageID) -> io::Result<Page<SIZE>> {
        let offset = SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        let mut buf = BytesMut::zeroed(SIZE);
        let n = match uio::pread(fd, &mut buf, offset) {
            Ok(n) => {
                eprintln!("Read page {}: {} bytes", page_id, n);

                n
            }
            Err(e) => {
                eprintln!("Error reading page {}: {}", page_id, e);

                return Err(io::ErrorKind::Other.into());
            }
        };

        Ok(Page::from_bytes(page_id, buf, n))
    }

    pub fn write_page<const SIZE: usize>(&self, page: &Page<SIZE>) -> io::Result<()> {
        let offset = SIZE as i64 * i64::from(page.id);
        let fd = self.file.as_raw_fd();

        match uio::pwrite(fd, &page.data, offset) {
            Ok(n) => {
                eprintln!("Written page {}: {} bytes", page.id, n);

                Ok(())
            }
            Err(e) => {
                eprintln!("Error writing page {}: {}", page.id, e);

                Err(io::ErrorKind::Other.into())
            }
        }
    }
}
