use bytes::Bytes;

use bitcask_rust::db;
use bitcask_rust::options::Options;

fn main() {
    let options = Options::default();
    let engine = db::Engine::open(options).expect("failed to open bitcask engine");
    {
        let result = engine.put(Bytes::from("name"), Bytes::from("bitcask-rust"));
        assert!(result.is_ok());
    }
    {
        let result = engine.get(Bytes::from("name"));
        assert!(result.is_ok());
        println!("value = {:?}", String::from_utf8(result.unwrap().to_vec()))
    }
    {
        let result = engine.delete(Bytes::from("value"));
        assert!(result.is_ok());
    }
}
