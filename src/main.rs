extern crate csv;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt::format;
use std::fs;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use walkdir::{DirEntry, WalkDir};
use walkdir::{Error, Result};

lazy_static! {
    static ref HM: HashMap<&'static str, u8> = {
        let m = HashMap::from([
            ("txt", 1),
            ("add_gene.txt", 2),
            ("add_gene.2.txt", 3),
            ("c.txt", 4),
            ("c.add_gene.txt", 5),
            ("c.add_gene.2.txt", 6),
            ("c.r.txt", 7),
            ("c.r.add_gene.txt", 8),
            ("c.r.add_gene.2.txt", 9),
        ]);
        m
    };
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn to_csv(input: PathBuf, output: PathBuf) -> Result<()> {
    let mut wtr = csv::Writer::from_path(output).unwrap();
    let read_file = OpenOptions::new()
        .read(true)
        .open(&input)
        .expect("read_file fail");

    let mut index = 0;
    let lines_iter = BufReader::new(read_file).lines().skip(3);
    wtr.write_record(&["T1", "T2", "T3", "T4", "TS", "GS", "SEQ", "Annotation"])
        .expect("header fail");
    for line in lines_iter {
        index += 1;
        let mut row: Vec<String> = Vec::new();
        let mut count: u8 = 0;
        let line = line.unwrap();

        for part in line.split_whitespace().skip(1) {
            if count < 6 {
                row.push(part.to_string());
            } else if count == 6 {
                row.push(part.to_string());
                row.push(String::new());
            } else {
                row[7] = row.get(7).unwrap().to_string() + " " + part;
            }
            count += 1;
        }
        if row.len() != 8 {
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
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    println!("Start: {}", path);


    for directory in WalkDir::new(path)
        .max_depth(1)
        .into_iter()
        .filter_entry(|entry| !is_hidden(entry))
        .skip(1)
    {
        sort(directory.unwrap().into_path());
    }
}

fn sort(root: PathBuf) -> Result<()> {
    let root_name = root.file_name().unwrap().to_str().unwrap();
    let mut file_map: HashMap<String, Vec<String>> = HashMap::new();

    let mut map:HashMap<String, [u8;3]> = HashMap::new();
    for entry in WalkDir::new(&root)
        .max_depth(1)
        .into_iter()
        .filter_entry(|e| !is_hidden(e)).filter(|e| e.as_ref().unwrap().path().is_file())
    {
        let path = entry?.into_path();
        let file_name = &path.file_name().unwrap().to_str().unwrap();
        let file_parts: Vec<&str> = file_name.splitn(2, ".").collect();
        let a = file_parts[0].to_owned();
        let b = file_parts[1].to_owned();
        // println!("{} {}",a, b);
        // let score = *(HM.get(b.as_str()).unwrap());
        // match score {
        //     1|2|3 => {
        //         if score > map.get(&a).unwrap()[0] {
        //             map.insert(a, [score,0,0]);
        //         }
        //     },
        //     4|5|6 => {
        //         if score > map.get(&a).unwrap()[1] {
        //             map.insert(a, [0,score,0]);
        //         }
        //     },
        //     7|8|9 => {
        //         if score > map.get(&a).unwrap()[2] {
        //             map.insert(a, [0,0,score]);
        //         }   
        //     },
        //     _ => {}
        // }
                
        // map.get_mut("chromosome-1-1").unwrap()[0]=9;
        // println!("{:?}",map);
        

        if let Some(files) = file_map.get_mut(&a) {
            files.push(b);
        } else {
            file_map.insert(a, vec![b]);
        }
    }

    for (folder_name, files) in file_map {
        let new_path = &root.join(&folder_name);
        
        let mut upstream: u8 = 0;
        let mut upstream_file = String::new();
        let mut complement: u8 = 0;
        let mut complement_file = String::new();
        let mut r_c: u8 = 0;
        let mut r_c_file = String::new();
        // let files = files.iter().map(|x| x.to_path_buf());

        for file in files {
            // let file_name = &file.file_name().unwrap().to_str().unwrap();
            // let second_part = file_name.splitn(2, ".").skip(1).next().unwrap();
            let score = HM.get(file.as_str()).unwrap();
            if file.contains("r") {
                if score > &mut r_c {
                    r_c = *score;
                    r_c_file = file;
                }
            } else if file.contains("c") {
                if score > &mut complement {
                    complement = *score;
                    complement_file = file;
                }
            } else {
                if score > &mut upstream {
                    upstream = *score;
                    upstream_file = file;
                }
            }
        }

        if !fs::metadata(new_path).is_ok() {
            fs::create_dir(new_path).unwrap();
        }

        let first_name: String = format!("{}-{}", root_name, folder_name);

        let upstream_file = build_path(format!("{}.{}",&folder_name,&upstream_file), &root);
        let complement_file = build_path(format!("{}.{}",&folder_name,&complement_file), &root);
        let r_c_file = build_path(format!("{}.{}",&folder_name,&r_c_file), &root);

        if upstream != 0 {
            to_csv(upstream_file, new_path.join(format!("{}.csv", first_name))).unwrap();
        }
        if complement != 0 {
            to_csv(
                complement_file,
                new_path.join(format!("{}-c.csv", first_name)),
            )
            .unwrap();
        }
        if r_c != 0 {
            to_csv(r_c_file, new_path.join(format!("{}-cr.csv", first_name))).unwrap();
        }
    }
    Ok(())
}

fn build_path(s: String, root_path: &PathBuf) -> PathBuf {
    let mut path = root_path.clone();
    path.push(s);
    path
}
