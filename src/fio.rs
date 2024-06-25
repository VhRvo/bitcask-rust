use std::path::PathBuf;

use crate::error::Result;
use crate::fio::file_io::FileIO;

pub(crate) mod file_io;

/// IOManager
pub trait IOManager: Sync + Send {
    /// Read data from specific position of file
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// Write byte array to file
    fn write(&self, but: &[u8]) -> Result<usize>;
    /// Persistence of data
    fn sync(&self) -> Result<()>;
}

/// 根据文件名初始化 IOManager
pub fn new_io_manager(file_name: PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
