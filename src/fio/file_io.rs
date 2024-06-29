use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use log::error;
use parking_lot::RwLock;

use crate::error::Error::{
    FailedToOpenDataFile, FailedToReadFromDataFile, FailedToSyncDataFile, FailedToWriteIntoDataFile,
};
use crate::error::Result;
use crate::fio::IOManager;

pub struct FileIO {
    file_id: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => Ok(FileIO {
                file_id: Arc::new(RwLock::new(file)),
            }),
            Err(err) => {
                error!("failed to open data file: {err}");
                Err(FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.file_id.read();
        read_guard.read_at(buffer, offset).map_err(|e| {
            error!("read from data file error {}", e);
            FailedToReadFromDataFile
        })
    }

    fn write(&self, buffer: &[u8]) -> Result<usize> {
        let mut write_guard = self.file_id.write();
        write_guard.write(buffer).map_err(|e| {
            error!("write to data file error {}", e);
            FailedToWriteIntoDataFile
        })
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.file_id.read();
        read_guard.sync_all().map_err(|e| {
            error!("sync data file error {}", e);
            FailedToSyncDataFile
        })?;
        Ok(())
    }

    fn size(&self) -> u64 {
        let read_guard = self.file_id.read();
        let metadata = read_guard.metadata().unwrap();
        metadata.len()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio = {
            let fio_result = FileIO::new(path.clone());
            assert!(fio_result.is_ok());
            fio_result.unwrap()
        };

        unsafe {
            let result = fio.write(b"key-a");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        unsafe {
            let result = fio.write(b"key-bc");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 6);
        }

        {
            let result = fs::remove_file(path);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let fio = {
            let fio_result = FileIO::new(path.clone());
            assert!(fio_result.is_ok());
            fio_result.unwrap()
        };

        unsafe {
            let result = fio.write(b"key-a");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        unsafe {
            let result = fio.write(b"key-bc");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 6);
        }

        {
            let mut buf = [0_u8; 5];
            let result = fio.read(&mut buf, 0);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        {
            let mut buf = [0_u8; 5];
            let result = fio.read(&mut buf, 5);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        {
            let result = fs::remove_file(path);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let fio = {
            let fio_result = FileIO::new(path.clone());
            assert!(fio_result.is_ok());
            fio_result.unwrap()
        };

        {
            let result = fio.write(b"key-a");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        {
            let result = fio.write(b"key-bc");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 6);
        }

        {
            let result = fio.sync();
            assert!(result.is_ok());
        }

        {
            let result = fs::remove_file(path);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn repeat_open_one_file() {
        let path = PathBuf::from("/tmp/new.data");
        let file_handle1 = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path.clone());
        let file_handle2 = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path);
        println!("{:?}", file_handle1);
        println!("{:?}", file_handle2);
        file_handle1
            .unwrap()
            .write(b"abc")
            .expect("failed to write");
        file_handle2
            .unwrap()
            .write(b"xyz")
            .expect("failed to write");
    }
}
