pub(crate) mod file_io;

use crate::errors::Result;

/// IOManager
pub trait IOManager: Sync + Send {
    /// Read data from specific position of file
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// Write byte array to file
    fn write(&self, but: &[u8]) -> Result<usize>;
    /// Persistence of data
    fn sync(&self) -> Result<()>;
}
