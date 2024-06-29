use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    /// directory of database
    pub dir_path: PathBuf,
    /// size of database
    pub data_file_size: u64,
    pub sync_writes: bool,
    /// 累积到多少字节后进行持久化
    pub(crate) bytes_per_sync: usize,
    pub index_type: IndexType,
    // 是否用 mmap 打开数据库
    pub mmap_at_startup: bool,
    // 执行数据文件 merge 的阈值
    pub merge_ratio: f32,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum IndexType {
    BTree,
    SkipList,
    BPlusTree,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rust"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            bytes_per_sync: 0,
            index_type: IndexType::BTree,
            mmap_at_startup: true,
            merge_ratio: 0.5,
        }
    }
}

pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

pub struct WriteBatchOptions {
    // 一个批次中中的最大数据量
    pub max_batch_num: usize,
    // 提交时是否进行 sync 持久化
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}

#[derive(Copy, Clone)]
pub enum IOType {
    StandardIO,
    MemoryMap,
}
