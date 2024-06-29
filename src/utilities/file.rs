use std::{fs, io};
use std::path::PathBuf;

// 磁盘数据目录的大小
pub(crate) fn dir_disk_size(dir_path: PathBuf) -> u64 {
    fs_extra::dir::get_size(dir_path).unwrap_or_default()
}

// 获取磁盘剩余空间容量
pub(crate) fn available_disk_size() -> u64 {
    fs2::available_space(PathBuf::from("/")).unwrap_or_default()
}

// 拷贝数据目录
pub(crate) fn copy_directory(
    source: PathBuf,
    destination: PathBuf,
    excludes: &[&str],
) -> io::Result<()> {
    if !destination.exists() {
        fs::create_dir_all(&destination)?;
    }

    for directory_entry in fs::read_dir(source)? {
        let entry = directory_entry?;
        let source_path = entry.path();

        if excludes
            .iter()
            .any(|&exclude| source_path.ends_with(exclude))
        {
            continue;
        }

        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(source_path, destination_path, excludes)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}
