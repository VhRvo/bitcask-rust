use crate::errors::Errors::{
    FailedToOpenDataFile, FailedToReadFromDataFile, FailedToSyncDataFile, FailedToWriteIntoDataFile,
};
use crate::errors::Result;
use crate::fio::IOManager;
use log::error;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

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
            Err(_) => Err(FailedToOpenDataFile),
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.file_id.read();
        let result = read_guard.read_at(buf, offset).map_err(|e| {
            error!("read from data file error {}", e);
            FailedToReadFromDataFile
        })?;
        Ok(result)
    }

    fn write(&self, but: &[u8]) -> Result<usize> {
        let mut write_guard = self.file_id.write();
        let result = write_guard.write(but).map_err(|e| {
            error!("write to data file error {}", e);
            FailedToWriteIntoDataFile
        })?;
        Ok(result)
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.file_id.read();
        read_guard.sync_all().map_err(|e| {
            error!("sync data file error {}", e);
            FailedToSyncDataFile
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio = {
            let fio_result = FileIO::new(path.clone());
            assert!(fio_result.is_ok());
            fio_result.unwrap()
        };

        unsafe {
            let result = fio.write("key-a".to_string().as_bytes_mut());
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        unsafe {
            let result = fio.write("key-bc".to_string().as_bytes_mut());
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
            let result = fio.write("key-a".to_string().as_bytes_mut());
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        unsafe {
            let result = fio.write("key-bc".to_string().as_bytes_mut());
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
            let result = fio.write("key-a".as_bytes());
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 5);
        }

        {
            let result = fio.write("key-bc".as_bytes());
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
}
