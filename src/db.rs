use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPosition, LogRecordType};
use crate::data::log_record::LogRecordType::DELETED;
use crate::errors::Errors::{FailedToFindDataFile, FailedToUpdateIndex, KeyIsEmpty, KeyNotFound};
use crate::errors::Result;
use crate::index;
use crate::options::Options;

/// Store engine
/// bitcask 存储引擎实例结构体
pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    index: Box<dyn index::Indexer>,
}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            Err(KeyIsEmpty)
        } else {
            // 构造 LogRecord
            let mut record = LogRecord {
                key: key.to_vec(),
                value: value.to_vec(),
                record_type: LogRecordType::NORMAL,
            };

            // 追加写到活跃数据文件中
            let log_record_position = self.append_log_record(&mut record)?;
            let update_success = self.index.put(key.to_vec(), log_record_position);
            if update_success {
                Ok(())
            } else {
                Err(FailedToUpdateIndex)
            }
        }
    }

    /// 根据 key 获取对应的数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            Err(KeyIsEmpty)
        } else {
            // 从内存索引中获取 key 对应的数据信息
            // 如果 key 不存在，直接返回
            let position = self.index.get(key.to_vec()).ok_or(KeyNotFound)?;
            let active_file = self.active_file.read();
            let older_file = self.older_files.read();
            let log_record = if active_file.get_file_id() == position.file_id {
                active_file.read_log_record(position.offset)?
            } else {
                // older_file
                let data_file = older_file
                    .get(&position.file_id)
                    .ok_or(FailedToFindDataFile)?;
                data_file.read_log_record(position.offset)?
            };
            if log_record.record_type == DELETED {
                Err(KeyNotFound)
            } else {
                Ok(log_record.value.into())
            }
        }
    }

    /// 追加写数据到当前活跃文件中
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPosition> {
        let dir_path = self.options.dir_path.clone();
        // 编码输入数据
        let encoded_record = log_record.encode();
        let record_len = encoded_record.len() as u64;

        // 获取当前的活跃文件
        let active_file = self.active_file.write();
        // let record_len = active_file.

        // 判断活跃文件是否达到写入阈值
        if active_file.get_write_offset() + record_len > self.options.data_file_size {
            // 将当前文件持久化
            active_file.sync()?;
            let current_fid = active_file.get_file_id();
            // 将旧的数据文件存储到 map 中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, active_file.take());
            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_offset = active_file.get_write_offset();
        active_file.write(&encoded_record)?;

        // 根据配置项决定是否持久化
        if self.options.sync_writes {
            active_file.sync()?;
        }
        // 构造内存索引信息
        Ok(LogRecordPosition {
            file_id: active_file.get_file_id(),
            offset: write_offset,
        })
    }
}
