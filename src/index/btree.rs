use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::data::log_record::LogRecordPosition;
use crate::index::Indexer;

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
}
