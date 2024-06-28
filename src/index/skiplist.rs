use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::data::log_record::LogRecordPosition;
use crate::index::{Indexer, IndexIterator};
use crate::iterator::GenericIterator;
use crate::options::IteratorOptions;

pub struct SkipList {
    skip_map: Arc<SkipMap<Vec<u8>, LogRecordPosition>>,
}

impl SkipList {
    pub(crate) fn new() -> Self {
        Self {
            skip_map: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, position: LogRecordPosition) -> bool {
        self.skip_map.insert(key, position);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPosition> {
        self.skip_map.get(&key).map(|entry| *entry.value())
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        self.skip_map.remove(&key).is_some()
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items: Vec<_> = self.skip_map
            .iter()
            .map(|item| (item.key().clone(), item.value().clone()))
            .collect();
        if options.reverse {
            items.reverse();
        }
        Box::new(GenericIterator {
            items,
            current_index: 0,
            options,
        })
    }

    fn list_keys(&self) -> crate::error::Result<Vec<Bytes>> {
        Ok(self.skip_map
            .iter()
            .map(|entry| Bytes::copy_from_slice(entry.key()))
            .collect())
    }
}


// SkipList 索引迭代器
// pub struct SkipListIterator {
//     items: Vec<(Vec<u8>, LogRecordPosition)>,
//     current_index: usize,
//     options: IteratorOptions,
// }

// impl IndexIterator for SkipListIterator {
//     fn rewind(&mut self) {
//         self.current_index = 0;
//     }
//
//     fn seek(&mut self, key: Vec<u8>) {
//         let result = if self.options.reverse {
//             self.items
//                 .binary_search_by(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key).reverse())
//         } else {
//             self.items
//                 .binary_search_by(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key))
//         };
//         // let comparator: Box<dyn Fn(_) -> Ordering> = if self.options.reverse {
//         //     Box::new(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key).reverse())
//         // } else {
//         //     Box::new(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key))
//         // };
//         // let result = self.items.binary_search_by(comparator);
//         self.current_index = result.unwrap_or_else(|index| index);
//     }
//
//     fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPosition)> {
//         loop {
//             self.current_index += 1;
//             let item = self.items.get(self.current_index)?;
//             if item.0.starts_with(&self.options.prefix) {
//                 return Some((&item.0, &item.1));
//             }
//         }
//     }
// }

