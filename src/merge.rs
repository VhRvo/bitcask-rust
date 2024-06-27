use std::path::PathBuf;

use crate::data::data_file::DataFile;
use crate::db::Engine;
use crate::error::Error::MergeIsInProgress;
use crate::error::Result;

const MERGE_DIR_NAME: &str = "merge";

impl Engine {
    pub fn merge(&self) -> Result<()> {
        let lock = self.merging_lock.try_lock();
        match lock {
            None => Err(MergeIsInProgress),
            Some(guard) => {
                let merge_files = self.rotate_merge_files()?;

                ;
                todo!()
            }
        }
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
        let new_active_file = DataFile::new(self.options.dir_path.clone(), active_file_id + 1)?;
        *active_file = new_active_file;

        // 加到久的数据文件当中
        // todo: use mem::replace
        let old_file = DataFile::new(self.options.dir_path.clone(), active_file_id)?;
        older_files.insert(active_file_id, old_file);
        // 加到待 merge 的文件 id 列表中
        merge_file_ids.push(active_file_id);

        // 排序，依次进行 merge
        merge_file_ids.sort();

        let mut merge_files = Vec::new();
        for file_id in merge_file_ids {
            let data_file = DataFile::new(self.options.dir_path.clone(), *file_id)?;
            merge_files.push(data_file);
        }
        Ok(merge_files)
    }
}

// 获取临时的用于 merge 的数据目录
fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_name = std::format!("{}-{}", file_name, MERGE_DIR_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}