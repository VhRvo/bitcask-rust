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
    #[error("failed to update memory index")]
    FailedToUpdateIndex,
    #[error("failed to find data file")]
    FailedToFindDataFile,
    #[error("the key is empty")]
    KeyIsEmpty,
    #[error("the key is not found int database")]
    KeyNotFound,
}
pub type Result<T> = result::Result<T, Errors>;
