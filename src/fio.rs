use std::path::PathBuf;

use crate::error::Result;
use crate::fio::file_io::FileIO;
use crate::fio::mmap::MmapIO;
use crate::options::IOType;

pub(crate) mod file_io;
mod mmap;

/// IOManager
pub trait IOManager: Sync + Send {
    /// Read data from specific position of file
    fn read(&self, buffer: &mut [u8], offset: u64) -> Result<usize>;
    /// Write byte array to file
    fn write(&self, buffer: &[u8]) -> Result<usize>;
    /// Persistence of data
    fn sync(&self) -> Result<()>;
    /// 获取文件的大小
    fn size(&self) -> u64;
}

/// 根据文件名初始化 IOManager
pub fn new_io_manager(file_name: PathBuf, io_type: IOType) -> Result<Box<dyn IOManager>> {
    match io_type {
        IOType::StandardIO => {
            Ok(Box::new(FileIO::new(file_name)?))
        }
        IOType::MemoryMap => {
            Ok(Box::new(MmapIO::new(file_name)?))
        }
    }
}
