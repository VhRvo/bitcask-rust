use std::fs::OpenOptions;
use std::path::PathBuf;
use std::ptr::write;
use std::sync::Arc;

use log::error;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};

use crate::error::Error::{FailedToOpenDataFile, NotForMmap, ReadDataFileEof};
use crate::error::Result;
use crate::fio::file_io::FileIO;
use crate::fio::IOManager;

pub struct MmapIO {
    mmap: Arc<Mutex<Mmap>>,
}

impl MmapIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_name)
        {
            Ok(file) => {
                let map = unsafe { Mmap::map(&file).expect("failed to map the file") };
                Ok(MmapIO {
                    mmap: Arc::new(Mutex::new(map)),
                })
            }
            Err(err) => {
                error!("failed to open data file: {err}");
                Err(FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for MmapIO {
    fn read(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        let map_array = self.mmap.lock();
        let end = offset + buffer.len() as u64;
        if end > map_array.len() as u64 {
            return Err(ReadDataFileEof);
        }
        let value = &map_array[offset as usize..];
        buffer.copy_from_slice(value);
        Ok(value.len())
    }

    fn write(&self, _buffer: &[u8]) -> Result<usize> {
        Err(NotForMmap)
    }

    fn sync(&self) -> Result<()> {
        Err(NotForMmap)
    }

    fn size(&self) -> u64 {
        let map_array = self.mmap.lock();
        map_array.len() as u64
    }
}
