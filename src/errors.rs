use std::result;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedToReadFromDataFile,
    #[error("failed to write into data file")]
    FailedToWriteIntoDataFile,
    #[error("failed to sync data file")]
    FailedToSyncDataFile,
    #[error("failed to open data file")]
    FailedToOpenDataFile,
}
pub type Result<T> = result::Result<T, Errors>;
