use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::data::log_record::LogRecord;
use crate::errors::Result;
use crate::fio;

/// 数据文件
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    // 当前写偏移，记录该数据文件的写入位置
    write_offset: Arc<RwLock<u64>>,
    // IO 接口管理者
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    /// 创建或打开一个新的数据文件
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_offset(&self) -> u64 {
        // let read_guard = self.write_off.read();
        // *read_guard
        *self.write_offset.read()
    }

    pub fn get_file_id(&self) -> u32 {
        // let read_guard = self.file_id.read();
        // *read_guard
        *self.file_id.read()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, but: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
