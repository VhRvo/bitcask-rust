use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};

use crate::batch::{
    log_record_key_with_sequential_number, NON_TRANSACTION_SEQUENTIAL_NUMBER, parse_log_record_key,
};
use crate::data::data_file::{DATA_FILE_NAME_SUFFIX, DataFile};
use crate::data::log_record::{
    LogRecord, LogRecordPosition, LogRecordType, ReadLogRecord, TransactionRecord,
};
use crate::error::Error::{
    DataDirectoryMaybeCorrupted, DataFileSizeIsTooSmall, DirPathIsEmpty,
    FailedToCreateDatabaseDirectory, FailedToFindDataFile, FailedToReadDataBaseDirectory,
    FailedToUpdateIndex, KeyIsEmpty, KeyIsNotFound, ReadDataFileEof,
};
use crate::error::Result;
use crate::index::Indexer;
use crate::index::new_indexer;
use crate::options::Options;

#[cfg(test)]
mod tests;

const INITIAL_FILE_ID: u32 = 0;

/// Store engine
/// bitcask 存储引擎实例结构体
pub struct Engine {
    pub(crate) options: Arc<Options>,
    // 当前活跃数据文件
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    // 旧的数据文件
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    // 数据内存索引
    pub(crate) index: Box<dyn Indexer>,
    // 数据库启动时的文件 id，只用于加载索引时使用，不能用于其他地方的更新或使用
    file_ids: Vec<u32>,
    // 事务提交保证串行化
    pub(crate) batch_commit_lock: Mutex<()>,
    // 事务序列号，全局递增
    pub(crate) sequential_number: AtomicUsize,
    // 防止多个线程同时 merge
    pub(crate) merging_lock: Mutex<()>,
}

impl Engine {
    // 打开 bitcask 存储引擎实例
    pub fn open(options: Options) -> Result<Self> {
        // 校验用户传递过来的配置项
        check_options(&options)?;
        // let options:
        // 判断数据目录是否存在，如果不存在的话，则创建目录
        let dir_path = options.dir_path.as_path();
        if !dir_path.is_dir() {
            fs::create_dir_all(dir_path).map_err(|err| {
                warn!("failed to create the database directory: {}", err);
                FailedToCreateDatabaseDirectory
            })?;
        }
        // 加载数据文件
        let mut data_files = load_data_files(dir_path)?;
        // 设置 file id 信息
        let file_ids = data_files
            .iter()
            .map(|data_file| data_file.get_file_id())
            .collect();

        // 将文件按照从大到小排列
        data_files.reverse();

        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            // 将旧的数据文件保存到 older_files 中
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        let active_file = match data_files.pop() {
            Some(data_file) => data_file,
            None => DataFile::new(PathBuf::from(dir_path), INITIAL_FILE_ID)?,
        };

        let index_type = options.index_type.clone();
        let mut engine = Self {
            options: Arc::new(options),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: new_indexer(index_type),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            sequential_number: AtomicUsize::new(1),
            merging_lock: Mutex::new(()),
        };

        // 从数据文件中加载索引
        let current_sequential_number = engine.load_index_from_data_files()?;
        engine
            .sequential_number
            .store(current_sequential_number + 1, Ordering::SeqCst);

        Ok(engine)
    }

    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            Err(KeyIsEmpty)
        } else {
            // 构造 LogRecord
            let mut record = LogRecord {
                key: log_record_key_with_sequential_number(
                    key.to_vec(),
                    NON_TRANSACTION_SEQUENTIAL_NUMBER,
                ),
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

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            Err(KeyIsEmpty)
        } else {
            let position = self.index.get(key.to_vec());
            if position.is_none() {
                Ok(())
            } else {
                let mut log_record = LogRecord {
                    key: log_record_key_with_sequential_number(
                        key.to_vec(),
                        NON_TRANSACTION_SEQUENTIAL_NUMBER,
                    ),
                    value: Default::default(),
                    record_type: LogRecordType::DELETED,
                };
                self.append_log_record(&mut log_record)?;
                let delete_success = self.index.delete(key.to_vec());
                if !delete_success {
                    Err(FailedToUpdateIndex)
                } else {
                    Ok(())
                }
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
            let position = self.index.get(key.to_vec()).ok_or(KeyIsNotFound)?;
            // 根据位置索引，获取文件中的 key 对应的 value
            self.get_by_position(position)
        }
    }

    pub fn get_by_position(&self, position: LogRecordPosition) -> Result<Bytes> {
        let active_file = self.active_file.read();
        let older_file = self.older_files.read();
        let log_record = if active_file.get_file_id() == position.file_id {
            active_file.read_log_record(position.offset)?.log_record
        } else {
            let data_file = older_file
                .get(&position.file_id)
                .ok_or(FailedToFindDataFile)?;
            data_file.read_log_record(position.offset)?.log_record
        };
        if log_record.record_type == LogRecordType::DELETED {
            Err(KeyIsNotFound)
        } else {
            Ok(log_record.value.into())
        }
    }

    /// 追加写数据到当前活跃文件中
    pub(crate) fn append_log_record(
        &self,
        log_record: &mut LogRecord,
    ) -> Result<LogRecordPosition> {
        let dir_path = self.options.dir_path.as_path();
        // 编码输入数据
        let encoded_record = log_record.encode();
        let record_len = encoded_record.len() as u64;

        // 获取当前的活跃文件
        let mut active_file = self.active_file.write();

        // 判断活跃文件是否达到写入阈值
        if active_file.get_write_offset() + record_len > self.options.data_file_size {
            // 将当前文件持久化
            active_file.sync()?;
            let current_fid = active_file.get_file_id();
            // 将旧的数据文件存储到 map 中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(PathBuf::from(dir_path), current_fid)?;
            older_files.insert(current_fid, old_file);
            // 打开新的数据文件
            let new_file = DataFile::new(PathBuf::from(dir_path), current_fid + 1)?;
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

    /// 从数据文件中加载内存索引
    /// 遍历数据文件中的内容，依次处理其中的记录
    fn load_index_from_data_files(&mut self) -> Result<usize> {
        // why don't directly mutate self?
        let mut current_sequential_number = NON_TRANSACTION_SEQUENTIAL_NUMBER;
        // 如果数据文件为空，则直接返回
        if self.file_ids.is_empty() {
            Ok(current_sequential_number)
        } else {
            // 暂存事务相关的数据
            let mut transaction_records: HashMap<usize, Vec<TransactionRecord>> =
                HashMap::<usize, Vec<_>>::new();

            let active_file = self.active_file.read();
            let older_files = self.older_files.read();

            // 遍历每个文件 id，取出对应的数据文件，并加载其中的数据
            for (ii, file_id) in self.file_ids.iter().enumerate() {
                let mut offset = 0;
                loop {
                    let read_log_record_result = if *file_id == active_file.get_file_id() {
                        active_file.read_log_record(offset)
                    } else {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    };
                    let ReadLogRecord { log_record, size } = match read_log_record_result {
                        Ok(result) => result,
                        Err(err) if err == ReadDataFileEof => break,
                        Err(err) => return Err(err),
                    };

                    // 构建内存索引
                    let log_record_position = LogRecordPosition {
                        file_id: *file_id,
                        offset,
                    };

                    // 解析 key，拿到实际的 key 和 sequential number
                    let (key, sequential_number) = parse_log_record_key(log_record.key);
                    if sequential_number == NON_TRANSACTION_SEQUENTIAL_NUMBER {
                        self.update_index(key, log_record.record_type, log_record_position)?;
                    } else {
                        if log_record.record_type == LogRecordType::TxnFINISHED {
                            let records = transaction_records.remove(&sequential_number).unwrap();
                            for record in records {
                                self.update_index(
                                    record.record.key,
                                    record.record.record_type,
                                    record.position,
                                )?;
                            }
                        } else {
                            let log_record = LogRecord { key, ..log_record };
                            transaction_records
                                .entry(sequential_number)
                                .or_default()
                                .push(TransactionRecord {
                                    record: log_record,
                                    position: log_record_position,
                                });
                        }
                    }
                    current_sequential_number = current_sequential_number.max(sequential_number);

                    // 递增 offset，下一次读取将从新的位置开始
                    offset += size;
                }

                // 设置活跃文件的 offset
                if ii == self.file_ids.len() - 1 {
                    active_file.set_write_offset(offset)
                };
            }
            Ok(current_sequential_number)
        }
    }

    fn update_index(
        &self,
        key: Vec<u8>,
        record_type: LogRecordType,
        log_record_position: LogRecordPosition,
    ) -> Result<()> {
        let update_success = match record_type {
            LogRecordType::NORMAL => self.index.put(key.to_vec(), log_record_position),
            LogRecordType::DELETED => self.index.delete(key.to_vec()),
            LogRecordType::TxnFINISHED => false,
        };
        if !update_success {
            return Err(FailedToUpdateIndex);
        }
        Ok(())
    }

    /// 关闭数据库，释放相关资源
    pub fn close(self) -> Result<()> {
        self.sync()
    }

    /// 持久化活跃文件
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }
}

fn check_options(options: &Options) -> Result<()> {
    let dir_path = options.dir_path.to_str();
    if dir_path.map(|path| path.len()).unwrap_or_default() == 0 {
        Err(DirPathIsEmpty)
    } else if options.data_file_size <= 0 {
        Err(DataFileSizeIsTooSmall)
    } else {
        Ok(())
    }
}

fn load_data_files(dir_path: &Path) -> Result<Vec<DataFile>> {
    // 读取数据目录
    let dir = fs::read_dir(dir_path).map_err(|_| FailedToReadDataBaseDirectory)?;

    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();
    for file in dir {
        if let Ok(entry) = file {
            // 拿到文件名
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            // 判断文件名称是否以 .data 结尾
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                // parse the file name
                let file_id = file_name
                    .split(".")
                    .next()
                    .unwrap()
                    .parse::<u32>()
                    .map_err(|_| DataDirectoryMaybeCorrupted)?;
                file_ids.push(file_id);
            }
            // 取出数据文件
        }
    }
    // 如果没有数据文件，则直接返回
    // 对文件 id 进行排序，从小到大进行加载
    file_ids.sort();
    // 遍历所有文件 id，依次打开对应的数据文件
    for file_id in file_ids {
        data_files.push(DataFile::new(PathBuf::from(dir_path), file_id)?);
    }
    Ok(data_files)
}
