use std::collections::HashMap;
use std::mem;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::data::log_record::{LogRecord, LogRecordPosition, LogRecordType};
use crate::data::log_record::LogRecordType::{DELETED, NORMAL};
use crate::db::Engine;
use crate::error::Error::{ExceedMaximumBatchNumber, KeyIsEmpty, ShouldNotUseWriteBatch};
use crate::error::Result;
use crate::options::IndexType::BPlusTree;
use crate::options::WriteBatchOptions;

const TXN_FIN_KEY: &[u8] = b"txn-finished";
pub(crate) const NON_TRANSACTION_SEQUENTIAL_NUMBER: usize = 0;

/// 批量写操作，保证原子性
pub struct WriteBatch<'a> {
    // 暂存用户写入的数据
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    //
    pub(crate) engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        if self.options.index_type == BPlusTree
            && !self.sequential_number_file_exists
            && !self.is_initial
        {
            Err(ShouldNotUseWriteBatch)
        } else {
            Ok(WriteBatch {
                pending_writes: Arc::new(Mutex::new(HashMap::new())),
                engine: self,
                options,
            })
        }
    }
}

impl WriteBatch<'_> {
    /// 批量写数据
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            Err(KeyIsEmpty)
        } else {
            // 暂存数据
            let log_record = LogRecord {
                key: key.to_vec(),
                value: value.to_vec(),
                record_type: NORMAL,
            };
            let mut pending_writes_guard = self.pending_writes.lock();
            pending_writes_guard.insert(key.to_vec(), log_record);
            Ok(())
        }
    }

    pub fn delete(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            Err(KeyIsEmpty)
        } else {
            let index_position = self.engine.index.get(key.to_vec());
            let mut pending_writes_guard = self.pending_writes.lock();
            let key = key.to_vec();
            if index_position.is_none() && pending_writes_guard.contains_key(&key) {
                pending_writes_guard.remove(&key);
            } else {
                // 暂存数据
                let log_record = LogRecord {
                    key: key.clone(),
                    value: value.to_vec(),
                    record_type: DELETED,
                };
                pending_writes_guard.insert(key, log_record);
            }
            Ok(())
        }
    }

    /// 提交数据，将数据写到文件中，并更新内存索引
    pub fn commit(&self) -> Result<()> {
        let mut pending_write_guard = self.pending_writes.lock();
        if pending_write_guard.len() > self.options.max_batch_num {
            Err(ExceedMaximumBatchNumber)
        } else {
            // 全局锁保证事务提交串行化
            let _lock = self.engine.batch_commit_lock.lock();
            // 获取全局事务序列号
            let sequential_number = self.engine.sequential_number.fetch_add(1, Ordering::SeqCst);
            let mut positions = HashMap::new();

            // 开始写数据到数据文件当中
            // There is no need to maintain item in the future of this function,
            // so use iter_mut and mem::take() to decrease the use of clone.
            for (key, item) in pending_write_guard.iter_mut() {
                let mut log_record = LogRecord {
                    key: log_record_key_with_sequential_number(mem::take(&mut item.key), sequential_number),
                    value: mem::take(&mut item.value),
                    record_type: item.record_type,
                };

                let position = self.engine.append_log_record(&mut log_record)?;
                positions.insert(key.clone(), position);
            }

            // 写最后一条数据标识事务完成
            let mut finished_log_record = LogRecord {
                key: log_record_key_with_sequential_number(TXN_FIN_KEY.to_vec(), sequential_number),
                value: Default::default(),
                record_type: LogRecordType::TxnFINISHED,
            };
            self.engine.append_log_record(&mut finished_log_record)?;

            if self.options.sync_writes {
                self.engine.sync()?;
            }

            // 数据全部写完之后更新内存索引
            // Shouldn't access other fields of item whose were taken before.
            for (key, item) in mem::take(pending_write_guard.deref_mut()).into_iter() {
                // todo: use vector and .zip() to replace the hash map
                let record_position = positions.get(&key).unwrap();
                if item.record_type == LogRecordType::NORMAL {
                    self.engine.index.put(key, *record_position).map(
                        |LogRecordPosition { size, .. }| {
                            self.engine.reclaim_size.fetch_add(size, Ordering::SeqCst);
                        },
                    );
                } else if item.record_type == LogRecordType::DELETED {
                    self.engine.index.delete(key).map(
                        |LogRecordPosition { size, .. }| {
                            self.engine.reclaim_size.fetch_add(size, Ordering::SeqCst);
                        },
                    );
                }
            }

            // 清空暂存数据
            // mem::take() do this job
            // pending_write_guard.clear();
            Ok(())
        }
    }
}

// 编码 sequential number 和 key
pub(crate) fn log_record_key_with_sequential_number(
    key: Vec<u8>,
    sequential_number: usize,
) -> Vec<u8> {
    let mut encoded_key = BytesMut::new();
    encode_length_delimiter(sequential_number, &mut encoded_key).unwrap();
    encoded_key.extend_from_slice(&key);
    encoded_key.to_vec()
}

// 解析出 LogRecord 的 key，拿到实际的 key 和 sequential number
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buffer = BytesMut::new();
    buffer.put_slice(&key);
    let sequential_number = decode_length_delimiter(&mut buffer).unwrap();
    (buffer.to_vec(), sequential_number)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    use crate::{options::Options, utilities};
    use crate::error::Error;

    use super::*;

    #[test]
    fn test_write_batch_1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        // 写数据之后未提交
        let put_res1 = wb.put(
            utilities::rand_kv::get_test_key(1),
            utilities::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            utilities::rand_kv::get_test_key(2),
            utilities::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());

        let res1 = engine.get(utilities::rand_kv::get_test_key(1));
        assert_eq!(Error::KeyIsNotFound, res1.err().unwrap());

        // 事务提交之后进行查询
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        let res2 = engine.get(utilities::rand_kv::get_test_key(1));
        assert!(res2.is_ok());

        // 验证事务序列号
        let seq_no = wb.engine.sequential_number.load(Ordering::SeqCst);
        assert_eq!(2, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch_2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        let put_res1 = wb.put(
            utilities::rand_kv::get_test_key(1),
            utilities::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            utilities::rand_kv::get_test_key(2),
            utilities::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        let put_res3 = wb.put(
            utilities::rand_kv::get_test_key(1),
            utilities::rand_kv::get_test_value(10),
        );
        assert!(put_res3.is_ok());

        let commit_res2 = wb.commit();
        assert!(commit_res2.is_ok());

        // 重启之后进行校验
        engine.close().expect("failed to close");
        // std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys();
        assert_eq!(2, keys.ok().unwrap().len());

        // 验证事务序列号
        let seq_no = engine2.sequential_number.load(Ordering::SeqCst);
        assert_eq!(3, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    // #[test]
    // fn test_write_batch_3() {
    //     let mut opts = Options::default();
    //     opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-3");
    //     opts.data_file_size = 64 * 1024 * 1024;
    //     let engine = Engine::open(opts.clone()).expect("failed to open engine");

    //     let keys = engine.list_keys();
    //     println!("key len {:?}", keys);

    //     // let mut wb_opts = WriteBatchOptions::default();
    //     // wb_opts.max_batch_num = 10000000;
    //     // let wb = engine.new_write_batch(wb_opts).expect("failed to create write batch");

    //     // for i in 0..=1000000 {
    //     //     let put_res = wb.put(util::rand_kv::get_test_key(i), util::rand_kv::get_test_value(10));
    //     //     assert!(put_res.is_ok());
    //     // }

    //     // wb.commit();
    // }

    #[test]
    fn test1() {
        let sequential_number = 0x1234567812345678;
        println!("seq no: {}", sequential_number);
        let key = b"abcdefg".to_vec();
        println!("key: {:?}", key);
        let with = log_record_key_with_sequential_number(key.clone(), sequential_number);
        println!("with: {:?}", with);
        let (without, number) = parse_log_record_key(with);
        println!("without: {without:?}");
        println!("number: {number:?}");
    }

    #[test]
    fn test2() {
        // let sequential_number = 42;
        // println!("seq no: {}", sequential_number);
        let key = b"ABCDEFG".to_vec();
        println!("key: {:?}", key);
        // let with = log_record_key_with_sequential_number(key.clone(), sequential_number);
        // println!("with: {:?}", with);
        let (without, number) = parse_log_record_key(key);
        println!("without: {without:?}");
        println!("number: {number:?}");
    }
}
