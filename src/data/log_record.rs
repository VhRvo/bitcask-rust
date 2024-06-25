use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

/// The information of data position, describes which position the data stored
/// 数据位置索引信息，描述数据存储到哪个位置
#[derive(Copy, Clone, Debug)]
pub struct LogRecordPosition {
    // 表示数据的存放文件
    pub(crate) file_id: u32,
    // 表示数据在文件中的存放位置
    pub(crate) offset: u64,
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum LogRecordType {
    // 正常 put 的数据类型
    NORMAL = 1,
    // 被删除数据的标识，墓碑值
    DELETED = 2,
}

impl LogRecordType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("unknown log record type"),
        }
    }
}

/// LogRecord: 写入到数据文件的记录
/// 之所以叫日志（Record），是因为数据文件中的数据是追加写入的，类似日志的格式
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    // encode 对 LogRecord 进行编码，返回字节数组
    // +---------+-----------------+-----------------+----------+----------+---------+
    // ｜ type   | key size        | value size      | key      | value    | crc     |
    // +---------+-----------------+---------------- +----------+----------+---------+
    //   1 byte   variable(max 5)   variable(max 5)   variable   variable   4 bytes
    pub fn encode(&self) -> Vec<u8> {
        self.encode_and_get_src().0
    }

    pub fn get_crc(&self) -> u32 {
        self.encode_and_get_src().1
    }

    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }

    fn encode_and_get_src(&self) -> (Vec<u8>, u32) {
        let mut buffer = BytesMut::with_capacity(self.encoded_length());

        buffer.put_u8(self.record_type as u8);

        encode_length_delimiter(self.key.len(), &mut buffer).unwrap();
        encode_length_delimiter(self.value.len(), &mut buffer).unwrap();

        buffer.extend_from_slice(&self.key);
        buffer.extend_from_slice(&self.value);

        // 计算并存储 CRC 校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buffer);
        let crc = hasher.finalize();
        buffer.put_u32(crc);
        println!("crc: {crc:}");

        (buffer.to_vec(), crc)
    }
}

/// 从数据文件中读取的 log_record 和其 size 信息
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) log_record: LogRecord,
    pub(crate) size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_and_crc() {
        // 正常编码
        {
            let record = LogRecord {
                key: "name".as_bytes().to_vec(),
                value: "bitcask_rust".as_bytes().to_vec(),
                record_type: LogRecordType::NORMAL,
            };
            let (encoded, crc) = record.encode_and_get_src();
            assert!(encoded.len() > 5);
            println!("{:?}", encoded);
            assert_eq!(4124891005, crc);
        }
        // Value 为空
        {
            let record = LogRecord {
                key: "name".as_bytes().to_vec(),
                value: Default::default(),
                record_type: LogRecordType::NORMAL,
            };
            let (encoded, crc) = record.encode_and_get_src();
            assert!(encoded.len() > 5);
            println!("{:?}", encoded);
            assert_eq!(3756865478, crc);
        }
        // 类型为 Deleted 的数据
        {
            let record = LogRecord {
                key: "name".as_bytes().to_vec(),
                value: "bitcask_rust".as_bytes().to_vec(),
                record_type: LogRecordType::DELETED,
            };
            let (encoded, crc) = record.encode_and_get_src();
            assert!(encoded.len() > 5);
            println!("{:?}", encoded);
            assert_eq!(1451905492, crc);
        }
    }
}

/// 获取 LogRecord Header 部分的最长长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}
