use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::data::log_record::{
    LogRecord, LogRecordPosition, LogRecordType, max_log_record_header_size, ReadLogRecord,
};
use crate::data::log_record::LogRecordType::NORMAL;
use crate::error::Error::{InvalidRecordCrc, ReadDataFileEof};
use crate::error::Result;
use crate::fio;
use crate::fio::new_io_manager;
use crate::options::IOType;

pub(crate) const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub(crate) const HINT_FILE_NAME: &str = "hint-index";
pub(crate) const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";
pub(crate) const SEQUENTIAL_NUMBER_FILE_NAME: &str = "sequential-number";

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
    pub fn new(dir_path: PathBuf, file_id: u32, io_type: IOType) -> Result<DataFile> {
        // 根据 path 和 id 构造出完成的文件名称
        let file_name = get_data_file_name(dir_path, file_id);
        // 初始化 IOManager
        let io_manager = new_io_manager(file_name, io_type)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
        // 根据 path 和 id 构造出完成的文件名称
        let file_name = dir_path.join(HINT_FILE_NAME);
        // 初始化 IOManager
        let io_manager = new_io_manager(file_name, IOType::StandardIO)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_merge_finished_file(dir_path: PathBuf) -> Result<DataFile> {
        // 根据 path 和 id 构造出完成的文件名称
        let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);
        // 初始化 IOManager
        let io_manager = new_io_manager(file_name, IOType::StandardIO)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn new_sequential_number_file(dir_path: PathBuf) -> Result<DataFile> {
        // 根据 path 和 id 构造出完成的文件名称
        let file_name = dir_path.join(SEQUENTIAL_NUMBER_FILE_NAME);
        // 初始化 IOManager
        let io_manager = new_io_manager(file_name, IOType::StandardIO)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn get_file_size(&self) -> u64 {
        self.io_manager.size()
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
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 先读取出 header 部分的数据
        let mut header_buffer = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buffer, offset)?;
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
                offset + actual_header_size as u64,
            )?;

            let log_record = LogRecord {
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

    pub fn write(&self, buffer: &[u8]) -> Result<usize> {
        let bytes_len = self.io_manager.write(buffer)?;
        // 更新 write_offset 字段
        let mut write_offset = self.write_offset.write();
        *write_offset += bytes_len as u64;
        Ok(bytes_len)
    }

    pub fn write_hint_record(&self, key: Vec<u8>, position: LogRecordPosition) -> Result<()> {
        let hint_record = LogRecord {
            key,
            value: position.encode(),
            record_type: NORMAL,
        };
        let encoded_record = hint_record.encode();
        self.write(&encoded_record)?;
        Ok(())
    }

    pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IOType) -> Result<()> {
        self.io_manager = new_io_manager(get_data_file_name(dir_path, self.get_file_id()), io_type)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

/// 构造文件名称
pub(crate) fn get_data_file_name(path: PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    path.join(name)
}

#[cfg(test)]
mod tests {
    use crate::data::log_record::LogRecordType::{DELETED, NORMAL};

    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());

        let insert = |file_id| {
            let data_file_result = DataFile::new(dir_path.clone(), file_id, IOType::StandardIO);
            assert!(data_file_result.is_ok());
            let data_file = data_file_result.unwrap();
            assert_eq!(data_file.get_file_id(), file_id);
        };
        insert(0);
        insert(0);
        insert(660);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());
        let data_file_result = DataFile::new(dir_path.clone(), 100, IOType::StandardIO);
        assert!(data_file_result.is_ok());
        let data_file = data_file_result.unwrap();
        assert_eq!(data_file.get_file_id(), 100);

        let insert = |value: &[u8]| {
            let write_result = data_file.write(value);
            assert!(write_result.is_ok());
            assert_eq!(write_result.unwrap(), 3);
        };
        insert(b"aaa");
        insert(b"bbb");
        insert(b"ccc");
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());
        let data_file_result = DataFile::new(dir_path.clone(), 200, IOType::StandardIO);
        assert!(data_file_result.is_ok());
        let data_file = data_file_result.unwrap();
        assert_eq!(data_file.get_file_id(), 200);

        let sync_result = data_file.sync();
        assert!(sync_result.is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        println!("temp directory: {:?}", dir_path.as_os_str());
        let data_file_result = DataFile::new(dir_path.clone(), 800, IOType::StandardIO);
        assert!(data_file_result.is_ok());
        let data_file = data_file_result.unwrap();
        assert_eq!(data_file.get_file_id(), 800);

        let first_offset = {
            let encoded = LogRecord {
                key: b"name".to_vec(),
                value: b"bit-cask-rust-kv".to_vec(),
                record_type: NORMAL,
            };
            let write_result = data_file.write(&encoded.encode());
            assert!(write_result.is_ok());

            // 从起始位置开始
            let read_result = data_file.read_log_record(0);
            assert!(read_result.is_ok());
            let ReadLogRecord { log_record, size } = read_result.unwrap();
            assert_eq!(1886608881, log_record.get_crc());
            assert_eq!(encoded.key, log_record.key);
            assert_eq!(encoded.value, log_record.value);
            assert_eq!(encoded.record_type, log_record.record_type);
            size
        };
        let second_offset = {
            let encoded = LogRecord {
                key: b"name_id".to_vec(),
                value: b"new-value".to_vec(),
                record_type: NORMAL,
            };
            let write_result = data_file.write(&encoded.encode());
            assert!(write_result.is_ok());

            // 从新的位置开始
            let read_result = data_file.read_log_record(first_offset);
            assert!(read_result.is_ok());
            let ReadLogRecord { log_record, size } = read_result.unwrap();
            assert_eq!(3220692689, log_record.get_crc());
            assert_eq!(encoded.key, log_record.key);
            assert_eq!(encoded.value, log_record.value);
            assert_eq!(encoded.record_type, log_record.record_type);
            size
        };
        {
            // 类型是 Deleted
            let encoded = LogRecord {
                key: b"name".to_vec(),
                value: Default::default(),
                record_type: DELETED,
            };
            let write_result = data_file.write(&encoded.encode());
            assert!(write_result.is_ok());

            // 从起始位置开始
            let read_result = data_file.read_log_record(first_offset + second_offset);
            assert!(read_result.is_ok());
            let ReadLogRecord {
                log_record,
                size: _,
            } = read_result.unwrap();
            assert_eq!(3993316699, log_record.get_crc());
            assert_eq!(encoded.key, log_record.key);
            assert_eq!(encoded.value, log_record.value);
            assert_eq!(encoded.record_type, log_record.record_type);
        }
    }
}
