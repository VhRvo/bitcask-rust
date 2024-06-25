use crate::data::log_record::LogRecordPosition;

pub mod btree;
pub mod skiplist;

/// The abstract index interface
pub trait Indexer: Sync + Send {
    /// According to the key, store position of data in the index
    fn put(&self, key: Vec<u8>, pos: LogRecordPosition) -> bool;
    /// According to the key, find position of data from the index
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPosition>;
    /// According to the key, remove position of data from the index
    fn delete(&self, key: Vec<u8>) -> bool;
}
