use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

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

#[cfg(test)]
mod tests {
    #[test]
    fn test_iterator_seek() {
        // let mut options
    }
}
