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
