// The information of data position, describes which position the data stored
#[derive(Copy, Clone, Debug)]
pub struct LogRecordPosition {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
