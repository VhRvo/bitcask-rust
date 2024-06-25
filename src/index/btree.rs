use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::data::log_record::LogRecordPosition;
use crate::index::{Indexer, IndexIterator};
use crate::options::IteratorOptions;

/// BTree Indexer
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPosition>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPosition) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPosition> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key).is_some()
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items: Vec<_> = read_guard
            .iter()
            .map(|item| (item.0.clone(), item.1.clone()))
            .collect();
        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            current_index: 0,
            options,
        })
    }
}

/// BTree 索引迭代器
pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPosition)>,
    current_index: usize,
    options: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.current_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        let comparator: Box<dyn Fn(_) -> Ordering> = if self.options.reverse {
            Box::new(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key).reverse())
        } else {
            Box::new(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key))
        };
        let result = self.items.binary_search_by(comparator);
        self.current_index = result.unwrap_or_else(|index| index);
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPosition)> {
        loop {
            let item = self.items.get(self.current_index);
            self.current_index += 1;
            if let Some(item) = item {
                if item.0.starts_with(&self.options.prefix) {
                    return Some((&item.0, &item.1));
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let btree = BTree::new();
        {
            let result = btree.put(
                "".as_bytes().to_vec(),
                LogRecordPosition {
                    file_id: 1,
                    offset: 10,
                },
            );
            assert!(result);
        }
        {
            let result = btree.put(
                "abc".as_bytes().to_vec(),
                LogRecordPosition {
                    file_id: 2,
                    offset: 20,
                },
            );
            assert!(result);
        }
    }

    #[test]
    fn test_btree_get() {
        let btree = BTree::new();
        {
            let result = btree.put(
                "".as_bytes().to_vec(),
                LogRecordPosition {
                    file_id: 1,
                    offset: 10,
                },
            );
            assert!(result);
        }
        {
            let result = btree.put(
                "abc".as_bytes().to_vec(),
                LogRecordPosition {
                    file_id: 2,
                    offset: 20,
                },
            );
            assert!(result);
        }
        {
            let position = btree.get("".as_bytes().to_vec());
            println!("{:?}", position);
            assert!(position.is_some());
            assert_eq!(position.unwrap().file_id, 1);
            assert_eq!(position.unwrap().offset, 10);
        }
        {
            let position = btree.get("abc".as_bytes().to_vec());
            println!("{:?}", position);
            assert!(position.is_some());
            assert_eq!(position.unwrap().file_id, 2);
            assert_eq!(position.unwrap().offset, 20);
        }
    }

    #[test]
    fn test_btree_delete() {
        let btree = BTree::new();
        {
            let result = btree.put(
                "".as_bytes().to_vec(),
                LogRecordPosition {
                    file_id: 1,
                    offset: 10,
                },
            );
            assert!(result);
        }
        {
            let result = btree.put(
                "abc".as_bytes().to_vec(),
                LogRecordPosition {
                    file_id: 2,
                    offset: 20,
                },
            );
            assert!(result);
        }
        {
            let result = btree.delete("".as_bytes().to_vec());
            assert!(result)
        }
        {
            let result = btree.delete("abc".as_bytes().to_vec());
            assert!(result);
        }
        {
            let result = btree.delete("non-existed".as_bytes().to_vec());
            assert!(!result);
        }
    }

    #[test]
    fn test_btree_iterator_seek() {
        let btree = BTree::new();

        // 无数据时
        let mut iterator = btree.iterator(IteratorOptions::default());
        iterator.seek("aa".as_bytes().to_vec());
        assert!(iterator.next().is_none());

        // 一条数据时
        btree.put(
            "ccdeb".as_bytes().to_vec(),
            LogRecordPosition {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iterator = btree.iterator(IteratorOptions::default());
        iterator.seek("aa".as_bytes().to_vec());
        assert!(iterator.next().is_some());

        let mut iterator = btree.iterator(IteratorOptions::default());
        iterator.seek("zz".as_bytes().to_vec());
        assert!(iterator.next().is_none());

        btree.put(
            "bbedd".as_bytes().to_vec(),
            LogRecordPosition {
                file_id: 1,
                offset: 10,
            },
        );
        btree.put(
            "aaedga".as_bytes().to_vec(),
            LogRecordPosition {
                file_id: 1,
                offset: 10,
            },
        );
        btree.put(
            "cadde".as_bytes().to_vec(),
            LogRecordPosition {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iterator = btree.iterator(IteratorOptions::default());
        iterator.seek("b".as_bytes().to_vec());
        while let Some(item) = iterator.next() {
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }

        let mut iterator = btree.iterator(IteratorOptions::default());
        iterator.seek("bbedd".as_bytes().to_vec());
        assert_eq!(
            Some(&"bbedd".as_bytes().to_vec()),
            iterator.next().map(|item| item.0)
        );

        let mut iterator = btree.iterator(IteratorOptions::default());
        iterator.seek("zz".as_bytes().to_vec());
        assert!(iterator.next().is_none());

        let mut iterator_option = IteratorOptions::default();
        iterator_option.reverse = true;
        let mut iterator = btree.iterator(iterator_option);
        iterator.seek("bb".as_bytes().to_vec());

        assert_eq!(
            Some(&"aaedga".as_bytes().to_vec()),
            iterator.next().map(|item| item.0)
        );
        assert!(iterator.next().is_none());
    }
}
