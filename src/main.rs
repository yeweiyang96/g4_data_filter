extern crate csv;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::{csv::ReaderBuilder, error::ArrowError, ipc::writer::FileWriter};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{prelude::*, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use walkdir::{DirEntry, WalkDir};

lazy_static! {
    static ref HM: HashMap<&'static str, u8> = {
        let m = HashMap::from([
            ("txt", 1),
            ("add_gene.txt", 2),
            ("add_gene.2.txt", 3),
            ("c.txt", 4),
            ("c.add_gene.txt", 5),
            ("c.add_gene.2.txt", 6),
            // ("c.r.txt", 7),
            // ("c.r.add_gene.txt", 8),
            // ("c.r.add_gene.2.txt", 9),
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
    // insert one row to taxonomy TABLE
    if !insert_name(organism_name){
        println!("Failed: Insert {} to taxonomy TABLE", organism_name);
    }

    // filemap is (one chromosome,[all files in this chromosome])
    for (genetic_material_name, postfixes) in file_map {
        let postfixes = postfixes.iter().map(|e| e.as_str());
        let mut raw: u8 = 0;
        let mut raw_file = "";
        let mut complement: u8 = 0;
        let mut complement_file = "";
        // let mut r_c: u8 = 0;
        // let mut r_c_file = "";

        for file in postfixes {
            let file = file;
            let score = HM.get(file).unwrap();
            if file.contains("r") {
                continue;
                // if score > &mut r_c {
                //     r_c = *score;
                //     r_c_file = file;
                // }
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

        let file_name: String = format!("{}${}", organism_name, genetic_material_name.replace("-", "_"));
        let raw_file = build_path(format!("{}.{}", &genetic_material_name, &raw_file), &organism);
        let complement_file = build_path(format!("{}.{}", &genetic_material_name, &complement_file), &organism);
        // let r_c_file = build_path(format!("{}.{}", &genetic_material_name, &r_c_file), &organism);
        if raw != 0 {
            to_csv(
                raw_file,
                new_path.join(format!("{}$raw.csv", &file_name)),
            );
        }
        if complement != 0 {
            to_csv(
                complement_file,
                new_path.join(format!("{}$c.csv", &file_name)),
            );
        }
        // if r_c != 0 {
        //     to_csv(r_c_file, new_path.join(format!("{}$rc.csv", &file_name)));
        // }

        // insert one genome with bio name to genome_list TABLE
        insert_genome(organism_name, &genetic_material_name.replace("-", "_"));
    }
    organism_name.to_string()
}

fn build_path(s: String, child_path: &PathBuf) -> PathBuf {
    let mut path = child_path.clone();
    path.push(s);
    path
}

fn import_csv(csv_file: PathBuf) {
    let table_name = csv_file
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
        .replace(".csv", "");
    let create_table = String::from(format!("CREATE TABLE IF NOT EXISTS {} (`ID` UInt32, `T1` UInt32, `T2` UInt32, `T3` UInt32, `T4` UInt32, `TS` UInt32, `GS` UInt32, `SEQ` String, `Gene` String) ENGINE = MergeTree() PRIMARY KEY ID ORDER BY ID SETTINGS index_granularity = 8192, index_granularity_bytes = 0;",table_name));
    let import_data = String::from(format!(
        "INSERT INTO {} FROM INFILE '{}' FORMAT CSV;",
        table_name,
        csv_file.display()
    ));

    let sql = format!("{}{}", &create_table, &import_data);
    if !execute_sql(&sql) {
        println!("Failed: {}", &sql);
    }
    match to_arrow_ipc(csv_file) {
        Ok(_) => {},
        Err(e) => println!("Convert Failed: {}", e),
        
    }
}
// insert one row to taxonomy TABLE
fn insert_name(name: &str) -> bool {
    let sql = format!("INSERT INTO `taxonomy` VALUES ('{}','',[]);", name);
    execute_sql(&sql)
}
//每上传一条染色体,往taxonomy表的对应的物种的genomes里插入一个染色体的名字
fn insert_genome(name: &str, genome: &String) {
    let sql = format!(
        "ALTER TABLE `taxonomy`
    UPDATE genomes = arrayConcat(genomes, ['{}'])
    WHERE `abbreviation` = '{}';",
        genome, name
    );
    if !execute_sql(&sql) {
        println!("Failed: {}", &sql);
    }
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
    wtr.write_record(&[
        "ID",
        "T1",
        "T2",
        "T3",
        "T4",
        "TS",
        "GS",
        "SEQ",
        "Gene",
    ])
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
    import_csv(output);
    
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

fn to_arrow_ipc(csv_file: PathBuf) -> Result<(), ArrowError> {
    let schema = Schema::new(vec![
        Field::new("ID", DataType::UInt32, false),
        Field::new("T1", DataType::UInt32, false),
        Field::new("T2", DataType::UInt32, false),
        Field::new("T3", DataType::UInt32, false),
        Field::new("T4", DataType::UInt32, false),
        Field::new("TS", DataType::UInt32, false),
        Field::new("GS", DataType::UInt32, false),
        Field::new("SEQ", DataType::Utf8, false),
        Field::new("Gene", DataType::Utf8, false),
    ]);
    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new(schema_ref).has_header(true);
    // .with_delimiter(opts.delimiter as u8);
    let file = File::open(&csv_file).unwrap();
    let reader = builder.build(file).unwrap();
    let path = csv_file
    .file_name()
    .unwrap()
    .to_str()
    .unwrap()
    .to_string()
    .replace(".csv", ".arrow");
    let out_path = String::from("./arrow/") + &path;
    let output = File::create(out_path)
        .map(|f| Box::new(f) as Box<dyn Write>)
        .unwrap();

    let mut writer = FileWriter::try_new(output, reader.schema().as_ref()).unwrap();

    for batch in reader {
        match batch {
            Ok(batch) => writer.write(&batch).unwrap(),
            Err(error) => return Err(error),
        }
    }
    writer.finish()
}

fn execute_sql(sql: &str) -> bool {
    let output = Command::new("/Users/wangzekun/clickhouse/clickhouse")
        .arg("client")
        .args(["-d", "g4"])
        .arg("-q")
        .arg(sql)
        .output()
        .expect("Failed to execute command: execute_sql");
    output.status.success()
}