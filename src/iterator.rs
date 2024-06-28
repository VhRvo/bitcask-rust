use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::data::log_record::LogRecordPosition;
use crate::db::Engine;
use crate::error::Result;
use crate::index::IndexIterator;
use crate::options::IteratorOptions;

pub struct Iterator<'a> {
    index_iterator: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, iterator_options: IteratorOptions) -> Iterator {
        Iterator {
            index_iterator: Arc::new(RwLock::new(self.index.iterator(iterator_options))),
            engine: self,
        }
    }

    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    /// 对数据库中的所有数据执行函数操作，函数返回 false 时终止
    pub fn fold<F>(&self, f: F) -> Result<()>
        where
            Self: Sized,
            F: Fn(Bytes, Bytes) -> bool,
    {
        let mut iterator = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iterator.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
    }
}

impl Iterator<'_> {
    pub fn rewind(&mut self) {
        self.index_iterator.write().rewind();
    }

    pub fn seek(&mut self, key: Vec<u8>) {
        self.index_iterator.write().seek(key);
    }

    pub fn next(&mut self) -> Option<(Bytes, Bytes)> {
        let mut write_guard = self.index_iterator.write();
        let result = write_guard.next();
        result.map(|item| {
            let value = self
                .engine
                .get_by_position(*item.1)
                .expect("failed to get value from data file");
            (Bytes::copy_from_slice(item.0), value)
        })
    }
}

pub struct GenericIterator {
    pub(crate) items: Vec<(Vec<u8>, LogRecordPosition)>,
    pub(crate) current_index: usize,
    pub(crate) options: IteratorOptions,
}


impl IndexIterator for GenericIterator {
    fn rewind(&mut self) {
        self.current_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        let result = if self.options.reverse {
            self.items
                .binary_search_by(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key).reverse())
        } else {
            self.items
                .binary_search_by(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key))
        };
        // let comparator: Box<dyn Fn(_) -> Ordering> = if self.options.reverse {
        //     Box::new(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key).reverse())
        // } else {
        //     Box::new(|item: &(Vec<_>, LogRecordPosition)| item.0.cmp(&key))
        // };
        // let result = self.items.binary_search_by(comparator);
        self.current_index = result.unwrap_or_else(|index| index);
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPosition)> {
        loop {
            self.current_index += 1;
            let item = self.items.get(self.current_index)?;
            if item.0.starts_with(&self.options.prefix) {
                return Some((&item.0, &item.1));
            }
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_iterator_seek() {
        // let mut options
    }
}
