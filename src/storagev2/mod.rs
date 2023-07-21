pub mod disk;
pub mod key_dir;
pub mod log;
pub mod page;
pub mod page_manager;
pub mod replacer;

#[cfg(test)]
mod test {
    pub enum Type {
        File,
        Dir,
    }

    pub struct CleanUp(&'static str, Type);

    impl CleanUp {
        pub fn file(file: &'static str) -> Self {
            Self(file, Type::File)
        }

        pub fn dir(dir: &'static str) -> Self {
            Self(dir, Type::Dir)
        }
    }

    impl Drop for CleanUp {
        fn drop(&mut self) {
            match self.1 {
                Type::File => {
                    if let Err(e) = std::fs::remove_file(self.0) {
                        eprintln!("ERROR: could not remove {} - {}", self.0, e);
                    }
                }
                Type::Dir => {
                    if let Err(e) = std::fs::remove_dir_all(self.0) {
                        eprintln!("ERROR: could not remove {} - {}", self.0, e);
                    }
                }
            }
        }
    }
}
