use bytes::Bytes;

pub(crate) fn get_test_key(number: usize) -> Bytes {
    Bytes::from(std::format!("bitcask-rust-key-{:09}", number))
}

pub(crate) fn get_test_value(number: usize) -> Bytes {
    Bytes::from(std::format!("bitcask-rust-value-{:09}", number))
}

#[test]
fn test_get_test_key_value() {
    for ii in 0..=10 {
        assert!(get_test_key(ii).len() > 0);
        assert!(get_test_value(ii).len() > 0);
    }
}
