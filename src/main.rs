extern crate csv;
use walkdir::{DirEntry, WalkDir};
use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader};


// struct G4 {
//     id: u32,
//     t1: u32,
//     t2: u32,
//     t3: u32,
//     t4: u32,
//     ts: u8,
//     gs: u8,
//     seq: String,
//     annotation: Option<String>,
//     flag: Option<u8>,
// }



// fn is_hidden(entry: &DirEntry) -> bool {
//     entry.file_name()
//          .to_str()
//          .map(|s| s.starts_with("."))
//          .unwrap_or(false)
// }

fn to_csv(entry: &DirEntry) -> Result<(), Box<dyn Error>> {
    let write_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(entry.path().with_extension("csv"))
        .expect("write file");
    let mut wtr = csv::Writer::from_writer(write_file);
    let read_file = OpenOptions::new()
        .read(true)
        .open(entry.path())
        .expect("read_file");
    let reader = BufReader::new(read_file);
    let mut index =0;
    let mut lines_iter = reader.lines().map(|l| l.unwrap());

    for _ in 0..3{
        lines_iter.next();
    }

    for line in lines_iter {
        index +=1;
        let mut row: Vec<String> = Vec::new();
        let mut count:u8 = 0;

        for part in line.split_whitespace() {
            if count < 7{
                row.push(part.to_string());
            } else if count == 7{
                row.push(part.to_string());
                row.push(String::from(" "));
            } else {
                row[8] = row.get(8).unwrap().to_string() + " "+ part;  
            }
            count+=1;
        }
        if row.len()<8{
        println!("Unlegal Data: '{}' in {}: {}row",&line,&entry.path().display(),&index);
            continue;
        }
        wtr.write_record(row).expect(&line);
    }
    Ok(())


}

fn main() {
    
    let current_dir = env::current_dir().unwrap();

    let walker = WalkDir::new(&current_dir).into_iter();
    let mut count = 0;
    for entry in walker.filter_map(|e| e.ok().filter(|e| e.file_name().to_str().map(|s| s.ends_with("txt")).unwrap_or(false))) {
        to_csv(&entry).expect("fuck");
        count+=1;
    }
    println!("There are {} .txt files.",count)
}
