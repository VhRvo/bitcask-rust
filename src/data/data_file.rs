use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::data::log_record::{
    LogRecord, LogRecordType, max_log_record_header_size, ReadLogRecord,
};
use crate::error::Error::{InvalidRecordCrc, ReadDataFileEof};
use crate::error::Result;
use crate::fio;
use crate::fio::new_io_manager;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

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
    pub fn new(__dir_path: PathBuf, __file_id: u32) -> Result<DataFile> {
        // 根据 path 和 id 构造出完成的文件名称
        let file_name = get_data_file_name(__dir_path, __file_id);
        // 初始化 IOManager
        let io_manager = new_io_manager(file_name)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(__file_id)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_write_offset(&self) -> u64 {
        // let read_guard = self.write_off.read();
        // *read_guard
        *self.write_offset.read()
    }

    pub fn set_write_offset(&self, offset: u64) {
        let mut write_guard = self.write_offset.write();
        *write_guard = offset;
    }

    pub fn get_file_id(&self) -> u32 {
        // let read_guard = self.file_id.read();
        // *read_guard
        *self.file_id.read()
    }

    /// 根据 offset 从数据文件中读取 LogRecord
    pub fn read_log_record(&self, __offset: u64) -> Result<ReadLogRecord> {
        // 先读取出 header 部分的数据
        let mut header_buffer = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buffer, __offset)?;
        // 取出 Type，在第一个字节
        let record_type = header_buffer.get_u8();
        // 取出 key 和 value 的长度
        let key_size = decode_length_delimiter(&mut header_buffer).unwrap();
        let value_size = decode_length_delimiter(&mut header_buffer).unwrap();
        // 如果 key 和 value 均为空，则说明读到了文件的末尾，直接返回
        if key_size == 0 && value_size == 0 {
            Err(ReadDataFileEof)
        } else {
            // 获取实际的 header 的大小
            let actual_header_size =
                length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
            // 读取实际的 key、value 和 crc
            let mut key_value_crc_buffer = BytesMut::zeroed(key_size + value_size + 4);
            self.io_manager.read(
                &mut key_value_crc_buffer,
                __offset + actual_header_size as u64,
            )?;

            let mut log_record = LogRecord {
                key: key_value_crc_buffer.get(..key_size).unwrap().to_vec(),
                value: key_value_crc_buffer
                    .get(key_size..key_value_crc_buffer.len() - 4)
                    .unwrap()
                    .to_vec(),
                record_type: LogRecordType::from_u8(record_type),
            };

            // 先前移动到最后 4 个字节
            key_value_crc_buffer.advance(key_size + value_size);
            // if key_value_crc_buffer.get_u32();
            if key_value_crc_buffer.get_u32() != log_record.get_crc() {
                Err(InvalidRecordCrc)
            } else {
                Ok(ReadLogRecord {
                    log_record,
                    size: (actual_header_size + key_size + value_size + 4) as u64,
                })
            }
        }
    }

    pub fn write(&self, __buf: &[u8]) -> Result<usize> {
        let bytes_len = self.io_manager.write(__buf)?;
        // 更新 write_offset 字段
        let mut write_offset = self.write_offset.write();
        *write_offset += bytes_len as u64;
        Ok(bytes_len)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

/// 构造文件名称
fn get_data_file_name(path: PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    path.join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());

        {
            let data_file_result = DataFile::new(dir_path.clone(), 0);
            assert!(data_file_result.is_ok());
            let data_file = data_file_result.unwrap();
            assert_eq!(data_file.get_file_id(), 0);
        }
        {
            let data_file_result = DataFile::new(dir_path.clone(), 0);
            assert!(data_file_result.is_ok());
            let data_file = data_file_result.unwrap();
            assert_eq!(data_file.get_file_id(), 0);
        }
        {
            let data_file_result = DataFile::new(dir_path.clone(), 660);
            assert!(data_file_result.is_ok());
            let data_file = data_file_result.unwrap();
            assert_eq!(data_file.get_file_id(), 660);
        }
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());
        let data_file_result = DataFile::new(dir_path.clone(), 100);
        assert!(data_file_result.is_ok());
        let data_file = data_file_result.unwrap();
        assert_eq!(data_file.get_file_id(), 100);

        {
            let write_result = data_file.write("aaa".as_bytes());
            assert!(write_result.is_ok());
            assert_eq!(write_result.unwrap(), 3);
        }
        {
            let write_result = data_file.write("bbb".as_bytes());
            assert!(write_result.is_ok());
            assert_eq!(write_result.unwrap(), 3);
        }
        {
            let write_result = data_file.write("ccc".as_bytes());
            assert!(write_result.is_ok());
            assert_eq!(write_result.unwrap(), 3);
        }
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());
        let data_file_result = DataFile::new(dir_path.clone(), 200);
        assert!(data_file_result.is_ok());
        let data_file = data_file_result.unwrap();
        assert_eq!(data_file.get_file_id(), 200);

        let sync_result = data_file.sync();
        assert!(sync_result.is_ok());
    }
}
