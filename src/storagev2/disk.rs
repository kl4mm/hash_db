use std::{io, os::fd::AsRawFd, path::Path};

use nix::sys::uio;
use tokio::fs::{File, OpenOptions};

use crate::storagev2::page::{PageID, PAGE_SIZE};

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

    pub fn read_page(&self, page_id: PageID) -> io::Result<[u8; PAGE_SIZE]> {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        let mut buf = [0; PAGE_SIZE];
        match uio::pread(fd, &mut buf, offset) {
            Ok(_) => {}
            Err(e) => panic!("{e}"),
        }

        Ok(buf)
    }

    pub fn write_page(&self, page_id: PageID, data: &[u8; PAGE_SIZE]) {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        match uio::pwrite(fd, data, offset) {
            Ok(_) => {}
            Err(e) => panic!("{e}"),
        };
    }

    pub async fn len(&self) -> usize {
        self.file
            .metadata()
            .await
            .expect("error getting metadata")
            .len() as usize
    }
}
