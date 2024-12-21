#[cfg(test)]
mod tests {
    use crate::etl::extract_json::*;
    use crossbeam::channel;
    //   use std::fs::File;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_temp_jsonl_file(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        let mut encoder =
            flate2::write::GzEncoder::new(temp_file.as_file_mut(), flate2::Compression::default());
        encoder.write_all(content.as_bytes()).unwrap();
        encoder.finish().unwrap();
        temp_file
    }

    fn create_temp_json_file(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file
    }

    #[test]
    fn test_stream_jsonl_valid() {
        let content = r#"{"name": "John", "age": 30}
{"name": "Jane", "age": 25}
{"name": "Bob", "age": 35}"#;

        let temp_file = create_temp_jsonl_file(content);
        let (tx, rx) = channel::unbounded();

        let handle = stream_jsonl(temp_file.path().to_str().unwrap(), tx).unwrap();

        let mut received = Vec::new();
        while let Ok(line) = rx.recv() {
            received.push(line);
        }

        handle.join().unwrap();

        assert_eq!(received.len(), 3);
        assert!(received[0].contains("John"));
        assert!(received[1].contains("Jane"));
        assert!(received[2].contains("Bob"));
    }

    #[test]
    fn test_stream_json_array() {
        let content = r#"[
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25},
            {"name": "Bob", "age": 35}
        ]"#;

        let temp_file = create_temp_json_file(content);
        let (tx, rx) = channel::unbounded();

        stream_json_array(temp_file.path().to_str().unwrap(), tx).unwrap();

        let mut received = Vec::new();
        while let Ok(line) = rx.recv() {
            received.push(line);
        }
        println!("{received:?}");
        assert_eq!(received.len(), 3);
        assert!(received[0].contains("John"));
        assert!(received[1].contains("Jane"));
        assert!(received[2].contains("Bob"));
    }

    #[test]
    fn test_stream_json_array_object() {
        let content = r#"{"data": [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25},
            {"name": "Bob", "age": 35}
        ]}"#;

        let temp_file = create_temp_json_file(content);
        let (tx, rx) = channel::unbounded();

        stream_json_array_object(temp_file.path().to_str().unwrap(), tx).unwrap();

        let mut received = Vec::new();
        while let Ok(line) = rx.recv() {
            received.push(line);
        }
        println!("{received:?}");
        assert_eq!(received.len(), 3);
        assert!(received[0].contains("John"));
        assert!(received[1].contains("Jane"));
        assert!(received[2].contains("Bob"));
    }

    #[test]
    fn test_stream_jsonl_empty_file() {
        let temp_file = create_temp_jsonl_file("");
        let (tx, _rx) = channel::unbounded();

        let handle = stream_jsonl(temp_file.path().to_str().unwrap(), tx).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_stream_json_array_empty() {
        let content = "[]";
        let temp_file = create_temp_json_file(content);
        let (tx, rx) = channel::unbounded();

        stream_json_array(temp_file.path().to_str().unwrap(), tx).unwrap();

        let received: Vec<_> = rx.try_iter().collect();
        assert_eq!(received.len(), 0);
    }

    #[test]
    fn test_stream_json_array_object_empty() {
        let content = r#"{"data": []}"#;
        let temp_file = create_temp_json_file(content);
        let (tx, rx) = channel::unbounded();

        stream_json_array_object(temp_file.path().to_str().unwrap(), tx).unwrap();

        let received: Vec<_> = rx.try_iter().collect();
        println!("{received:?}");
        assert_eq!(received.len(), 0);
    }

    #[test]
    fn test_invalid_file_path() {
        let (tx, _rx) = channel::unbounded();
        let (tx2, _rx2) = channel::unbounded();
        let (tx3, _rx3) = channel::unbounded();

        assert!(stream_jsonl("nonexistent_file.json.gz", tx).is_err());
        assert!(stream_json_array("nonexistent_file.json", tx2).is_err());
        assert!(stream_json_array_object("nonexistent_file.json", tx3).is_err());
    }

    #[test]
    fn test_malformed_json() {
        let content = r#"[
            {"name": "John", "age": 30},
            {"name": "Jane", age": 25},  // Missing quote
            {"name": "Bob", "age": 35}
        ]"#;

        let temp_file = create_temp_json_file(content);
        let (tx, _rx) = channel::unbounded();

        assert!(stream_json_array(temp_file.path().to_str().unwrap(), tx).is_err());
    }
}
