extern crate csv;

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::{prelude::*, BufReader};
use std::path::PathBuf;
use walkdir::{DirEntry, WalkDir};

lazy_static! {
    static ref HM: HashMap<&'static str, u8> = {
        let m = HashMap::from([
            ("txt", 1),
            ("add_gene.txt", 2),
            ("add_gene.2.txt", 3),
            ("c.txt", 4),
            ("c.add_gene.txt", 5),
            ("c.add_gene.2.txt", 6)
        ]);
        m
    };
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let root_path = &args[1];
    println!("Start from: {}", root_path);

    for organism in WalkDir::new(root_path)
        .max_depth(1)
        .into_iter()
        .filter_entry(|entry| !is_hidden(entry))
        .skip(1)
    {
        println!(
            "Completed {}",
            handle(organism.unwrap().into_path(), root_path)
        );
    }
    println!("Done");
}

fn handle(organism: PathBuf, root_path: &String) -> String {
    let organism_name = organism.file_name().unwrap().to_str().unwrap();
    let walkdir = WalkDir::new(&organism);
    let file_map = analyse_files(walkdir);
    let new_path = PathBuf::from(root_path).join(".csv_files");
    println!("Start to handle {}", organism_name);
    if !fs::metadata(&new_path).is_ok() {
        fs::create_dir(&new_path).unwrap();
    }
    for (genetic_material_name, postfixes) in file_map {
        let postfixes = postfixes.iter().map(|e| e.as_str());
        let mut raw: u8 = 0;
        let mut raw_file = "";
        let mut complement: u8 = 0;
        let mut complement_file = "";
        for file in postfixes {
            let file = file;
            let score = HM.get(file).unwrap();
            if file.contains("r") {
                continue;
            } else if file.contains("c") {
                if score > &mut complement {
                    complement = *score;
                    complement_file = file;
                }
            } else {
                if score > &mut raw {
                    raw = *score;
                    raw_file = file;
                }
            }
        }

        let file_name: String = format!(
            "{}${}",
            organism_name,
            genetic_material_name.replace("-", "_")
        );
        let raw_file = build_path(
            format!("{}.{}", &genetic_material_name, &raw_file),
            &organism,
        );
        let complement_file = build_path(
            format!("{}.{}", &genetic_material_name, &complement_file),
            &organism,
        );
        if raw != 0 {
            to_csv(raw_file, new_path.join(format!("{}$raw.csv", &file_name)));
        }
        if complement != 0 {
            to_csv(
                complement_file,
                new_path.join(format!("{}$c.csv", &file_name)),
            );
        }
    }
    organism_name.to_string()
}
fn build_path(s: String, child_path: &PathBuf) -> PathBuf {
    let mut path = child_path.clone();
    path.push(s);
    path
}
fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn is_txt(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.ends_with(".txt"))
        .unwrap_or(false)
}

fn to_csv(input: PathBuf, output: PathBuf) {
    let mut wtr = csv::Writer::from_path(&output).unwrap();
    let read_file = OpenOptions::new()
        .read(true)
        .open(&input)
        .expect("read_file fail");

    let mut index = 0;
    let lines_iter = BufReader::new(read_file).lines().skip(3);
    wtr.write_record(&["ID", "T1", "T2", "T3", "T4", "TS", "GS", "SEQ", "Gene"])
        .expect("header fail");
    for line in lines_iter {
        index += 1;
        let mut row: Vec<String> = Vec::new();
        let mut count: u8 = 0;
        let line = line.unwrap();

        for part in line.split_whitespace() {
            if count < 7 {
                row.push(part.to_string());
            } else if count == 7 {
                row.push(part.to_string());
                row.push(String::new());
            } else {
                row[8] = row.get(8).unwrap().to_string() + " " + part;
            }
            count += 1;
        }
        // check data length
        if row.len() != 9 {
            println!(
                "Unlegal Data: '{}' in {}: {}row",
                &line,
                &input.display(),
                &index
            );
            continue;
        }
        wtr.write_record(row).expect(&line);
    }
    wtr.flush().expect("flush");
}

fn analyse_files(walkdir: WalkDir) -> HashMap<String, Vec<String>> {
    let mut file_map: HashMap<String, Vec<String>> = HashMap::new();
    let walkdir = walkdir
        .max_depth(1)
        .into_iter()
        .filter(|e| is_txt(e.as_ref().unwrap()));

    for entry in walkdir {
        //let path = entry.unwrap().path();
        let file_name = entry
            .unwrap()
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let file_parts: Vec<&str> = file_name.splitn(2, ".").collect();
        if let Some(files) = file_map.get_mut(file_parts[0]) {
            files.push(file_parts[1].to_string());
        } else {
            file_map.insert(file_parts[0].to_string(), vec![file_parts[1].to_string()]);
        }
    }
    file_map
}
