/// The information of data position, describes which position the data stored
/// 数据位置索引信息，描述数据存储到哪个位置
#[derive(Copy, Clone, Debug)]
pub struct LogRecordPosition {
    // 表示数据的存放文件
    pub(crate) file_id: u32,
    // 表示数据在文件中的存放位置
    pub(crate) offset: u64,
}

#[derive(Eq, PartialEq)]
pub enum LogRecordType {
    // 正常 put 的数据类型
    NORMAL = 1,
    // 被删除数据的标识，墓碑值
    DELETED = 2,
}

/// LogRecord: 写入到数据文件的记录
/// 之所以叫日志（Record），是因为数据文件中的数据是追加写入的，类似日志的格式
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}

/// 从数据文件中读取的 log_record 和其 size 信息
pub struct ReadLogRecord {
    pub(crate) log_record: LogRecord,
    pub(crate) size: u64,
}
