use std::path::PathBuf;

use bytes::Bytes;

use crate::data::log_record::LogRecordPosition;
use crate::error::Result;
use crate::options::{IndexType, IteratorOptions};

pub mod btree;
pub mod skiplist;
mod bptree;

/// The abstract index interface
pub trait Indexer: Sync + Send {
    /// According to the key, store position of data in the index
    fn put(&self, key: Vec<u8>, position: LogRecordPosition) -> bool;
    /// According to the key, find position of data from the index
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPosition>;
    /// According to the key, remove position of data from the index
    fn delete(&self, key: Vec<u8>) -> bool;
    /// Return an iterator
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
    fn list_keys(&self) -> Result<Vec<Bytes>>;
}

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        IndexType::SkipList => Box::new(skiplist::SkipList::new()),
        IndexType::BPlusTree => Box::new(bptree::BPlusTree::new(dir_path))
    }
}

/// 抽象索引迭代器
pub trait IndexIterator: Sync + Send {
    /// 重新回到迭代器的起点
    fn rewind(&mut self);
    /// 根据传入的 key，找到第一个大于或者等于目标的 key，从此 key 开始遍历
    fn seek(&mut self, key: Vec<u8>);
    /// 跳转到下一个 key，返回 None 代表迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPosition)>;
}
