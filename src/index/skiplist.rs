use crate::data::log_record::LogRecordPosition;
use crate::index::Indexer;

pub struct SkipList {}

impl SkipList {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Indexer for SkipList {
    fn put(&self, __key: Vec<u8>, __position: LogRecordPosition) -> bool {
        todo!()
    }

    fn get(&self, __key: Vec<u8>) -> Option<LogRecordPosition> {
        todo!()
    }

    fn delete(&self, __key: Vec<u8>) -> bool {
        todo!()
    }
}