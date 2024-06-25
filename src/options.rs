use std::path::PathBuf;

pub struct Options {
    /// directory of database
    pub dir_path: PathBuf,
    /// size of database
    pub data_file_size: u64,
    pub sync_writes: bool,
    pub index_type: IndexType,
}

#[derive(Copy, Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rust"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}
