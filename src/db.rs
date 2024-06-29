use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use fs2::FileExt;
use log::{error, warn};
use parking_lot::{Mutex, RwLock};
use prost::encoding::key_len;

use crate::batch::{
    log_record_key_with_sequential_number, NON_TRANSACTION_SEQUENTIAL_NUMBER, parse_log_record_key,
};
use crate::data::data_file::{
    DATA_FILE_NAME_SUFFIX, DataFile, HINT_FILE_NAME, MERGE_FINISHED_FILE_NAME,
    SEQUENTIAL_NUMBER_FILE_NAME,
};
use crate::data::log_record::{
    decode_log_record_position, LogRecord, LogRecordPosition, LogRecordType, ReadLogRecord,
    TransactionRecord,
};
use crate::error::Error::{
    DatabaseIsUsing, DataDirectoryMaybeCorrupted, DataFileSizeIsTooSmall, DirPathIsEmpty,
    FailedToCreateDatabaseDirectory, FailedToFindDataFile, FailedToParse,
    FailedToReadDataBaseDirectory, FailedToUpdateIndex, FileNotExists, InvalidMergeRatio,
    KeyIsEmpty, KeyIsNotFound, ReadDataFileEof,
};
use crate::error::Result;
use crate::index::Indexer;
use crate::index::new_indexer;
use crate::merge::load_merge_files;
use crate::options::{IndexType, IOType, Options};
use crate::utilities;

#[cfg(test)]
mod tests;

const INITIAL_FILE_ID: u32 = 0;
const SEQUENTIAL_NUMBER_KEY: &[u8] = b"sequential-number";
pub(crate) const FILE_LOCK_NAME: &str = "flock";

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
    // 是否存在存储事务序列号的文件
    pub(crate) sequential_number_file_exists: bool,
    // 是否是第一次初始化该目录
    pub(crate) is_initial: bool,
    // 文件锁保证只有一个数据库实例在数据目录上被打开
    lock_file: File,
    // 累计写入了多少字节
    bytes: Arc<AtomicUsize>,
    // 累计有多少空间可以 merge
    pub(crate) reclaim_size: Arc<AtomicU64>,
}

/// 统计存储引擎的相关信息
#[derive(Debug)]
pub struct Statistics {
    // key 的总数量
    pub(crate) key_count: usize,
    // 数据文件的总数量
    pub(crate) data_file_count: usize,
    // 可以回收的数据量
    pub(crate) reclaim_size: u64,
    // 数据目录占据的磁盘空间的大小
    pub(crate) disk_size: usize,
}

impl Engine {
    // 打开 bitcask 存储引擎实例
    pub fn open(options: Options) -> Result<Self> {
        // 校验用户传递过来的配置项
        check_options(&options)?;
        // let options:
        // 判断数据目录是否存在，如果不存在的话，则创建目录
        let dir_path = options.dir_path.as_path();
        let is_initial = if !dir_path.is_dir() {
            fs::create_dir_all(dir_path).map_err(|err| {
                warn!("failed to create the database directory: {}", err);
                FailedToCreateDatabaseDirectory
            })?;
            true
        } else if fs::read_dir(dir_path).unwrap().count() == 0 {
            true
        } else {
            false
        };

        // 判断数据目录是否已经被使用过
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir_path.join(FILE_LOCK_NAME))
            .unwrap();
        lock_file.try_lock_exclusive().map_err(|err| {
            warn!("database is using: {err}");
            DatabaseIsUsing
        })?;

        // 加载 merge 数据目录
        load_merge_files(PathBuf::from(dir_path))?;

        // 加载数据文件
        let mut data_files = load_data_files(dir_path, options.mmap_at_startup)?;
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
            None => DataFile::new(PathBuf::from(dir_path), INITIAL_FILE_ID, IOType::StandardIO)?,
        };

        let index_type = options.index_type.clone();
        let dir_path = PathBuf::from(dir_path);
        let mut engine = Self {
            options: Arc::new(options),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: new_indexer(index_type, dir_path),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            sequential_number: AtomicUsize::new(1),
            merging_lock: Mutex::new(()),
            sequential_number_file_exists: false,
            is_initial,
            lock_file,
            bytes: Arc::new(AtomicUsize::new(0)),
            reclaim_size: Arc::new(AtomicU64::new(0)),
        };

        // B+ 树不需要从数据文件中加载索引
        if IndexType::BPlusTree != index_type {
            // 从 hint 文件加载索引
            engine.load_index_from_hint_file()?;

            // 从数据文件中加载索引
            let current_sequential_number = engine.load_index_from_data_files()?;
            engine
                .sequential_number
                .store(current_sequential_number + 1, Ordering::SeqCst);
        } else {
            // 加载事务序列号
            if let Ok(sequential_number) = engine.load_sequential_number() {
                engine
                    .sequential_number
                    .store(sequential_number, Ordering::SeqCst);
                engine.sequential_number_file_exists = true;
            }

            // 设置当前活跃文件的数据偏移
            let active_file = engine.active_file.write();
            // why don't use file size directly instead of using write_offset?
            active_file.set_write_offset(active_file.get_file_size());
        }
        // 重置 IO 类型
        if engine.options.mmap_at_startup {
            engine.reset_io_type()?;
        }

        Ok(engine)
    }

    fn reset_io_type(&self) -> Result<()> {
        let mut active_file = self.active_file.write();
        active_file.set_io_manager(self.options.dir_path.clone(), IOType::StandardIO)?;
        let mut older_files = self.older_files.write();
        for (_, data_file) in older_files.iter_mut() {
            data_file.set_io_manager(self.options.dir_path.clone(), IOType::StandardIO)?;
        }
        Ok(())
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
            self.index
                .put(key.to_vec(), log_record_position)
                .map(|LogRecordPosition { size, .. }| {
                    self.reclaim_size.fetch_add(size, Ordering::SeqCst);
                    ()
                })
                .ok_or(FailedToUpdateIndex)
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
                self.index
                    .delete(key.to_vec())
                    .map(|LogRecordPosition { size, .. }| {
                        self.reclaim_size.fetch_add(size, Ordering::SeqCst);
                        ()
                    })
                    .ok_or(FailedToUpdateIndex)
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
            let old_file = DataFile::new(PathBuf::from(dir_path), current_fid, IOType::StandardIO)?;
            older_files.insert(current_fid, old_file);
            // 打开新的数据文件
            let new_file =
                DataFile::new(PathBuf::from(dir_path), current_fid + 1, IOType::StandardIO)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_offset = active_file.get_write_offset();
        active_file.write(&encoded_record)?;

        // 根据配置项决定是否持久化
        let previous = self.bytes.fetch_add(encoded_record.len(), Ordering::SeqCst);
        let need_sync = self.options.sync_writes
            || previous + encoded_record.len() > self.options.bytes_per_sync;

        if need_sync {
            active_file.sync()?;
            self.bytes.store(0, Ordering::SeqCst);
        }
        // 构造内存索引信息
        Ok(LogRecordPosition {
            file_id: active_file.get_file_id(),
            offset: write_offset,
            size: record_len,
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
            // 拿到最近未参与 merge 的文件 id
            let merge_finished_file = self.options.dir_path.join(MERGE_FINISHED_FILE_NAME);
            let smallest_non_merge_file_id = if merge_finished_file.is_file() {
                let merge_finished_file =
                    DataFile::new_merge_finished_file(self.options.dir_path.clone())?;
                let merge_finished_record = merge_finished_file.read_log_record(0)?;
                let value = String::from_utf8(merge_finished_record.log_record.value).unwrap();
                value.parse::<u32>().ok()
            } else {
                None
            };

            // 暂存事务相关的数据
            let mut transaction_records: HashMap<usize, Vec<TransactionRecord>> =
                HashMap::<usize, Vec<_>>::new();

            let active_file = self.active_file.read();
            let older_files = self.older_files.read();

            // 遍历每个文件 id，取出对应的数据文件，并加载其中的数据
            for (ii, file_id) in self.file_ids.iter().enumerate() {
                // if smallest_non_merge_file_id
                //     .map(|smallest_non_merge_file_id| *file_id < smallest_non_merge_file_id)
                //     .unwrap_or_default()
                if Some(*file_id) < smallest_non_merge_file_id {
                    continue;
                }

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
                        size,
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

    /// 从 hint 索引文件中加载索引
    fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);
        // 如果不存在，直接返回
        if !hint_file_name.is_file() {
            Ok(())
        } else {
            // let hint_file = DataFile::new_hint_file(hint_file_name)?;
            let hint_file = DataFile::new_hint_file(self.options.dir_path.clone())?;
            let mut offset = 0;
            loop {
                let ReadLogRecord { log_record, size } = match hint_file.read_log_record(offset) {
                    Ok(result) => result,
                    Err(err) if err == ReadDataFileEof => break,
                    Err(err) => return Err(err),
                };

                // 解码 value，拿到内存位置索引
                let log_record_position = decode_log_record_position(log_record.value)?;
                // 存储到内存索引中
                self.index.put(log_record.key, log_record_position);
                offset += size;
            }
            Ok(())
        }
    }

    fn update_index(
        &self,
        key: Vec<u8>,
        record_type: LogRecordType,
        log_record_position: LogRecordPosition,
    ) -> Result<()> {
        let update_success = match record_type {
            LogRecordType::NORMAL => self.index.put(key.to_vec(), log_record_position).map(
                |LogRecordPosition { size, .. }| {
                    self.reclaim_size.fetch_add(size, Ordering::SeqCst);
                },
            ),
            LogRecordType::DELETED => {
                self.index
                    .delete(key.to_vec())
                    .map(|LogRecordPosition { size, .. }| {
                        self.reclaim_size.fetch_add(size, Ordering::SeqCst);
                    })
            }
            LogRecordType::TxnFINISHED => None,
        };
        update_success.ok_or(FailedToUpdateIndex)
    }

    /// 关闭数据库，释放相关资源
    pub fn close(&self) -> Result<()> {
        // 如果数据目录不存在则直接返回（测试时会出现的情况）
        if !self.options.dir_path.is_dir() {
            return Ok(());
        }

        let sequential_number_file =
            DataFile::new_sequential_number_file(self.options.dir_path.clone())?;
        let sequential_number = self.sequential_number.load(Ordering::SeqCst);
        let record = LogRecord {
            key: SEQUENTIAL_NUMBER_KEY.to_vec(),
            value: sequential_number.to_string().into_bytes(),
            record_type: LogRecordType::NORMAL,
        };
        sequential_number_file.write(&record.encode())?;
        sequential_number_file.sync()?;

        // 释放文件锁
        self.lock_file.unlock().unwrap();
        self.sync()
    }

    /// 持久化活跃文件
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    // B+ 树索引模式下加载事务序列号
    fn load_sequential_number(&self) -> Result<usize> {
        let file_name = self.options.dir_path.join(SEQUENTIAL_NUMBER_FILE_NAME);
        if !file_name.is_file() {
            Err(FileNotExists)
        } else {
            let data_file = DataFile::new_sequential_number_file(self.options.dir_path.clone())?;
            let ReadLogRecord { log_record, .. } = data_file.read_log_record(0).map_err(|err| {
                error!("failed to read sequential number: {err}");
                err
            })?;
            // 加载后删除文件，避免追加写入
            fs::remove_file(file_name).unwrap();
            let value = String::from_utf8(log_record.value).unwrap();
            value.parse::<usize>().map_err(|err| {
                error!("parse failed: {err}");
                FailedToParse
            })
        }
    }

    /// 获取数据库统计信息
    pub(crate) fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics {
            key_count: self.list_keys()?.len(),
            data_file_count: self.older_files.read().len() + 1,
            reclaim_size: self.reclaim_size.load(Ordering::SeqCst),
            // disk_size: utilities::file::dir_disk_size(self.options.dir_path.clone())?,
            disk_size: todo!(),
        })
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            log::error!("error while close engin: {err}")
        }
    }
}

fn check_options(options: &Options) -> Result<()> {
    let dir_path = options.dir_path.to_str();
    if dir_path.map(|path| path.len()).unwrap_or_default() == 0 {
        Err(DirPathIsEmpty)
    } else if options.data_file_size <= 0 {
        Err(DataFileSizeIsTooSmall)
    } else if (0_f32..=1_f32).contains(&options.merge_ratio) {
        Err(InvalidMergeRatio)
    } else {
        Ok(())
    }
}

fn load_data_files(dir_path: &Path, need_mmap: bool) -> Result<Vec<DataFile>> {
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
        let io_type = if need_mmap {
            IOType::MemoryMap
        } else {
            IOType::StandardIO
        };
        data_files.push(DataFile::new(PathBuf::from(dir_path), file_id, io_type)?);
    }
    Ok(data_files)
}
