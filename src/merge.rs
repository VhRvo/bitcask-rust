use std::fs;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use log::error;

use crate::batch::{
    log_record_key_with_sequential_number, NON_TRANSACTION_SEQUENTIAL_NUMBER, parse_log_record_key,
};
use crate::data::data_file::{DATA_FILE_NAME_SUFFIX, DataFile, get_data_file_name, MERGE_FINISHED_FILE_NAME, SEQUENTIAL_NUMBER_FILE_NAME};
use crate::data::log_record::{LogRecord, ReadLogRecord};
use crate::data::log_record::LogRecordType::NORMAL;
use crate::db::{Engine, FILE_LOCK_NAME};
use crate::error::Error::{FailedToCreateDatabaseDirectory, FailedToReadDataBaseDirectory, MergeIsInProgress, MergeRatioUnreached, NotEnoughSpaceForMerge, ReadDataFileEof};
use crate::error::Result;
use crate::options::{IOType, Options};
use crate::utilities;

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FINISHED_KEY: &[u8] = b"merge.finished";

impl Engine {
    pub fn merge(&self) -> Result<()> {
        let lock = self.merging_lock.try_lock();
        match lock {
            None => Err(MergeIsInProgress),
            Some(_guard) => {
                // 如果数据库空，则直接返回
                if self.is_empty() {
                    return Ok(())
                }
                let reclaim_size = self.reclaim_size.load(Ordering::SeqCst);
                let total_size = utilities::file::dir_disk_size(self.options.dir_path.clone());
                if (reclaim_size as f32 / total_size as f32) < self.options.merge_ratio {
                    return Err(MergeRatioUnreached);
                }

                let available_size = utilities::file::available_disk_size();
                if total_size - reclaim_size <= available_size {
                    return Err(NotEnoughSpaceForMerge);
                }

                let merge_files = self.rotate_merge_files()?;
                let merge_path = get_merge_path(self.options.dir_path.clone());
                // 如果目录已经存在，则先进行删除
                if merge_path.is_dir() {
                    fs::remove_dir_all(merge_path.clone()).unwrap();
                }
                // 创建 merge 数据目录
                fs::create_dir_all(merge_path.clone()).map_err(|err| {
                    error!("failed to create merge path {err}");
                    FailedToCreateDatabaseDirectory
                })?;

                // 打开临时用于 merge 的 bitcask 实例
                let merge_db_options = Options {
                    dir_path: merge_path.clone(),
                    data_file_size: self.options.data_file_size,
                    ..Options::default()
                };
                let merge_engine = Engine::open(merge_db_options)?;

                // 打开 hint 文件存储索引
                let hint_file = DataFile::new_hint_file(merge_path.clone())?;

                // 依次处理每个数据文件，重写有效数据
                for data_file in merge_files.iter() {
                    let mut offset = 0;
                    loop {
                        let ReadLogRecord {
                            mut log_record,
                            size,
                        } = match data_file.read_log_record(offset) {
                            Ok(result) => result,
                            Err(err) if err == ReadDataFileEof => break,
                            Err(err) => return Err(err),
                        };

                        // parse to get the key
                        let (key, _) = parse_log_record_key(log_record.key.clone());
                        if let Some(index_position) = self.index.get(key.clone()) {
                            // 如果文件 id 和偏移 offset 均相等，则说明是一条有效的数据
                            if index_position.file_id == data_file.get_file_id()
                                && index_position.offset == offset
                            {
                                // 去除事务的标识
                                log_record.key = log_record_key_with_sequential_number(
                                    key.clone(),
                                    NON_TRANSACTION_SEQUENTIAL_NUMBER,
                                );
                                let log_record_position =
                                    merge_engine.append_log_record(&mut log_record)?;

                                // 写 hint 索引
                                hint_file.write_hint_record(key.clone(), log_record_position)?;
                            }
                        }
                        offset += size;
                    }
                }
                // sync 保证持久化
                merge_engine.sync()?;
                hint_file.sync()?;

                // 拿到最近未参与 merge 的文件 id
                let smallest_non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
                let merge_finished_file = DataFile::new_merge_finished_file(merge_path.clone())?;
                let merge_finished_record = LogRecord {
                    key: MERGE_FINISHED_KEY.to_vec(),
                    value: smallest_non_merge_file_id.to_string().into_bytes(),
                    record_type: NORMAL,
                };
                let encoded_record = merge_finished_record.encode();
                merge_finished_file.write(&encoded_record)?;
                merge_finished_file.sync()?;
                Ok(())
            }
        }
    }

    fn is_empty(&self) -> bool {
        let active_file = self.active_file.read();
        let older_files = self.active_file.read();
        active_file.get_write_offset() == 0 && older_files.len() == 0
    }

    fn rotate_merge_files(&self) -> Result<Vec<DataFile>> {
        // 取出旧的数据文件的 id
        let mut merge_file_ids = Vec::new();
        let mut older_files = self.older_files.write();
        for fid in older_files.keys() {
            merge_file_ids.push(*fid);
        }

        // 设置一个新的活跃文件用于写入
        let mut active_file = self.active_file.write();
        // 保证数据文件的持久性
        active_file.sync()?;
        let active_file_id = active_file.get_file_id();
        let new_active_file = DataFile::new(self.options.dir_path.clone(), active_file_id + 1, IOType::StandardIO)?;
        *active_file = new_active_file;

        // 加到久的数据文件当中
        // todo: use mem::replace
        let old_file = DataFile::new(self.options.dir_path.clone(), active_file_id, IOType::StandardIO)?;
        older_files.insert(active_file_id, old_file);
        // 加到待 merge 的文件 id 列表中
        merge_file_ids.push(active_file_id);

        // 排序，依次进行 merge
        merge_file_ids.sort();

        let mut merge_files = Vec::new();
        for file_id in merge_file_ids {
            let data_file = DataFile::new(self.options.dir_path.clone(), file_id, IOType::StandardIO)?;
            merge_files.push(data_file);
        }
        Ok(merge_files)
    }
}

// 获取临时的用于 merge 的数据目录
fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_name = std::format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}

/// 加载 merge 数据目录
pub(crate) fn load_merge_files(dir_path: PathBuf) -> Result<()> {
    let merge_path = get_merge_path(dir_path.clone());
    // 没有发生过 merge，直接返回
    if !merge_path.is_dir() {
        Ok(())
    } else {
        let directory = fs::read_dir(merge_path.clone()).map_err(|err| {
            error!("failed to read merge directory {err}");
            FailedToReadDataBaseDirectory
        })?;

        // 查找是否有标识 merge 完成的文件
        let mut merge_file_names = Vec::new();
        let mut merge_finished = false;
        for file in directory {
            if let Ok(entry) = file {
                let file_name = entry.file_name();
                let file_name = file_name.to_str().unwrap();
                if file_name.ends_with(MERGE_FINISHED_FILE_NAME) {
                    merge_finished = true;
                }
                if file_name.ends_with(SEQUENTIAL_NUMBER_FILE_NAME) {
                    continue;
                }
                if file_name.ends_with(FILE_LOCK_NAME) {
                    continue;
                }
                // 数据文件为空，则直接返回
                let metadata = entry.metadata().unwrap();
                if file_name.ends_with(DATA_FILE_NAME_SUFFIX) && metadata.len() == 0 {
                    continue;
                }
                merge_file_names.push(entry.file_name());
            }
        }
        // merge 没有完成，直接返回
        if !merge_finished {
            fs::remove_dir_all(merge_path.clone()).unwrap();
            Ok(())
        } else {
            // 打开标识 merge 完成的文件，取出未参与 merge 的文件 id
            let merge_finished_file = DataFile::new_merge_finished_file(merge_path.clone())?;
            let merge_finished_record = merge_finished_file.read_log_record(0)?;
            let smallest_non_merge_file_id =
                String::from_utf8(merge_finished_record.log_record.value)
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();

            // 删除旧的数据文件
            for file_id in 0..smallest_non_merge_file_id {
                let file = get_data_file_name(dir_path.clone(), file_id);
                if file.is_file() {
                    fs::remove_file(file).unwrap();
                }
            }

            for file_name in merge_file_names {
                let source_path = merge_path.join(file_name.clone());
                let destination_path = dir_path.join(file_name.clone());
                fs::rename(source_path, destination_path).unwrap()
            }

            // 最后删除临时 merge 的目录
            fs::remove_dir_all(merge_path.clone()).unwrap();

            Ok(())
        }
    }
}
