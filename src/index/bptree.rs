use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use jammdb::{DB, Error};

use crate::data::log_record::{decode_log_record_position, LogRecordPosition};
use crate::index::{Indexer, IndexIterator};
use crate::iterator::GenericIterator;
use crate::options::IteratorOptions;

const BPTREE_INDEX_FILE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bitcask-index";

pub struct BPlusTree {
    bptree: Arc<DB>,
}

impl BPlusTree {
    pub fn new(dir_path: PathBuf) -> Self {
        let bptree =
            DB::open(dir_path.join(BPTREE_INDEX_FILE_NAME)).expect("failed to open b+ tree");
        let bptree = Arc::new(bptree);
        let tx = bptree.tx(true).expect("failed to start a transaction");
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();
        Self { bptree }
    }
}

impl Indexer for BPlusTree {
    fn put(&self, key: Vec<u8>, position: LogRecordPosition) -> bool {
        let tx = self.bptree.tx(true).expect("failed to start a transaction");
        let bucket = tx
            .get_bucket(BPTREE_BUCKET_NAME)
            .expect("failed to get a bucket");
        bucket
            .put(key, position.encode())
            .expect("failed to put value in b+ tree");
        tx.commit().unwrap();
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPosition> {
        let tx = self.bptree.tx(true).expect("failed to start a transaction");
        let bucket = tx
            .get_bucket(BPTREE_BUCKET_NAME)
            .expect("failed to get a bucket");
        bucket
            .get_kv(key)
            .and_then(|data| decode_log_record_position(data.value().to_vec()).ok())
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let tx = self.bptree.tx(true).expect("failed to start a transaction");
        let bucket = tx
            .get_bucket(BPTREE_BUCKET_NAME)
            .expect("failed to get a bucket");
        if let Err(err) = bucket.delete(key) {
            if err == Error::KeyValueMissing {
                return false;
            }
        }
        tx.commit().unwrap();
        true
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let tx = self
            .bptree
            .tx(false)
            .expect("failed to start a transaction");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        let mut items: Vec<_> = bucket
            .cursor()
            .into_iter()
            .map(|data| {
                let key = data.key().to_vec();
                let position = decode_log_record_position(data.kv().value().to_vec())
                    .expect("failed to decode log record position in b+ tree");
                (key, position)
            })
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
        let tx = self.bptree.tx(true).expect("failed to start a transaction");
        let bucket = tx
            .get_bucket(BPTREE_BUCKET_NAME)
            .expect("failed to get a bucket");
        Ok(bucket
            .cursor()
            .into_iter()
            .map(|data| Bytes::copy_from_slice(data.key()))
            .collect())
    }
}
