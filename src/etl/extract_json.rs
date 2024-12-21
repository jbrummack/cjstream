pub use crossbeam::channel::Sender;
pub use crossbeam::channel::*;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{self, BufRead};
use std::io::{BufReader, Read};
use std::thread::{self, JoinHandle};
use struson::reader::{JsonReader, JsonStreamReader};

#[allow(dead_code)]
pub fn stream_jsonl(path: &str, tx: Sender<String>) -> std::io::Result<JoinHandle<()>> {
    let file = File::open(path)?;
    let producer = thread::spawn(move || {
        let decoder = GzDecoder::new(file);
        let reader = io::BufReader::with_capacity(16, decoder); //new(&mut decoder);
        let lines = reader.lines();
        for line in lines {
            if let Ok(line) = line {
                let _ = tx.send(line);
            }
        }
    });
    Ok(producer)
}

#[allow(dead_code)]
pub fn stream_json_array(
    path: &str,
    sink: Sender<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let file2 = File::open(path)?;
    let mut reader = BufReader::new(file2);
    let mut bposition: u64 = 0;
    let mut jreader = JsonStreamReader::new(file);
    let mut objects: u64 = 0;

    jreader.begin_array()?;
    while let Ok(true) = jreader.has_next() {
        objects += 1;
        if let Some(position) = jreader.current_position(false).data_pos {
            let offset = position - bposition;
            let mut buffer = vec![0; offset as usize];
            reader.read_exact(&mut buffer)?;
            loop {
                let trailing = buffer.pop();
                if let Some(t) = trailing {
                    if t.is_ascii_whitespace() {
                        buffer.pop();
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&buffer);
            if objects != 1 {
                sink.send(text.into())?;
            }
            bposition = position;
        }
        jreader.begin_object()?;
        while let Ok(true) = jreader.has_next() {
            match jreader.skip_name() {
                Ok(_) => (),
                Err(err) => {
                    println!("Name: {err}");
                }
            }
            match jreader.skip_value() {
                Ok(_) => (),
                Err(err) => {
                    println!("Value: {err}");
                }
            }
        }

        jreader.end_object()?;
    }

    //Read the last object because the reader doesnt has_next() anymore
    if let Some(position) = jreader.current_position(false).data_pos {
        let offset = position - bposition;
        let mut buffer = vec![0; offset as usize];
        reader.read_exact(&mut buffer)?;
        loop {
            let trailing = buffer.pop();
            if let Some(t) = trailing {
                if t.is_ascii_whitespace() {
                    buffer.pop();
                    let text = String::from_utf8_lossy(&buffer);
                    if objects != 1 {
                        sink.send(text.into())?;
                    }
                    break;
                }
            }
            if buffer.is_empty() {
                break;
            }
        }
    }

    jreader.end_array()?;
    println!("Parsed {objects} objects! ");
    Ok(())
}

#[allow(dead_code)]
pub fn stream_json_array_object(
    path: &str,
    sink: Sender<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let file2 = File::open(path)?;
    let mut reader = BufReader::new(file2);
    let mut bposition: u64 = 0;
    let mut jreader = JsonStreamReader::new(file);
    let mut objects: u64 = 0;

    jreader.begin_object()?;
    println!("{jreader:?}");
    match jreader.next_name() {
        Ok(ok) => println!("{ok:?}"),
        Err(err) => {
            println!("Name: {err}");
        }
    }
    jreader.begin_array()?;
    while let Ok(true) = jreader.has_next() {
        objects += 1;
        if let Some(position) = jreader.current_position(false).data_pos {
            //println!("{bposition}-{:?}", position);
            let offset = position - bposition;
            //println!("Offset: {offset}");
            let mut buffer = vec![0; offset as usize];
            reader.read_exact(&mut buffer)?;
            loop {
                let trailing = buffer.pop();
                if let Some(t) = trailing {
                    if t.is_ascii_whitespace() {
                        buffer.pop();
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&buffer);
            if objects != 1 {
                println!("SENT IN LOOP");
                sink.send(text.into())?;
            }

            bposition = position;
        }
        jreader.begin_object()?;
        while let Ok(true) = jreader.has_next() {
            match jreader.skip_name() {
                Ok(_) => (),
                Err(err) => {
                    println!("Name: {err}");
                }
            }
            match jreader.skip_value() {
                Ok(_) => (),
                Err(err) => {
                    println!("Value: {err}");
                }
            }
        }
        jreader.end_object()?;
    }
    //Read the last object because the reader doesnt has_next() anymore
    if let Some(position) = jreader.current_position(false).data_pos {
        let offset = position - bposition;
        let mut buffer = vec![0; offset as usize];
        reader.read_exact(&mut buffer)?;
        loop {
            if buffer.is_empty() {
                break;
            }
            let trailing = buffer.pop();
            if let Some(t) = trailing {
                if t.is_ascii_whitespace() {
                    buffer.pop();
                    let text = String::from_utf8_lossy(&buffer);
                    if objects != 0 {
                        println!("SENT IN END, objects: {objects}");
                        sink.send(text.into())?;
                    }
                    break;
                }
            }
        }
    }

    jreader.end_array()?;
    jreader.end_object()?;

    println!("Parsed {objects} objects! ");
    Ok(())
}
