use std::result;

use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    #[error("failed to read from data file")]
    FailedToReadFromDataFile,
    #[error("failed to write into data file")]
    FailedToWriteIntoDataFile,
    #[error("failed to sync data file")]
    FailedToSyncDataFile,
    #[error("failed to open data file")]
    FailedToOpenDataFile,
    #[error("failed to update memory index")]
    FailedToUpdateIndex,
    #[error("failed to find data file")]
    FailedToFindDataFile,
    #[error("failed to create the database directory")]
    FailedToCreateDatabaseDirectory,
    #[error("failed to read the database directory")]
    FailedToReadDataBaseDirectory,
    #[error("the key is empty")]
    KeyIsEmpty,
    #[error("the key is not found int database")]
    KeyIsNotFound,
    #[error("the directory path in database can be empty")]
    DirPathIsEmpty,
    #[error("the size of data file in database must be non-zero")]
    DataFileSizeIsTooSmall,
    #[error("the data directory maybe corrupted")]
    DataDirectoryMaybeCorrupted,
    #[error("read data file EOF")]
    ReadDataFileEof,
    #[error("invalid crc value, log record maybe corrupted")]
    InvalidRecordCrc,
}

pub type Result<T> = result::Result<T, Error>;
