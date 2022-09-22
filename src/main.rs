use clap::{App, Arg};
use std::fs;
use std::io;
use std::io::stdin;
use std::io::{stdout, Write};
use std::path::Path;
extern crate prettytable;
use prettytable::{Cell, Row, Table};
extern crate csv;
use sqlite::Type;
use std::fs::OpenOptions;
extern crate regex;
use regex::Regex;
use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};




use sqlite;

fn main() {
    let dir_opt: Arg = Arg::new("dir")
        .short('d') 
        .long("dir") 
        .takes_value(true)
        .default_value("./"); 

    let sync_opt: Arg = Arg::new("sync") 
        .short('s') 
        .long("sync") 
        .takes_value(true)
        .default_value("true"); 

    let app: App = App::new("My Application")
        .author("Author's name")
        .version("v1.0.0")
        .about("Application short description.")
        .arg(dir_opt)
        .arg(sync_opt);

    let matches = app.get_matches();
    let csv_files = if let Some(dir) = matches.value_of("dir") { read_dir(dir).unwrap()} else { Vec::new() };
    let is_sync = if let Some(sync) = matches.value_of("sync") { sync == "true" } else { true };

    let connection = sqlite::open(":memory:").unwrap();

    for file in csv_files {
        println!("{:?}", file);
        let file_name = Path::new(&file)
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let (header_data, record_data_list) = read_csv(&file);
        create_table(&connection, &file_name, &header_data);
        create_connect_file_table(&connection, &file_name, &file);
        insert_records(&connection, &file_name, &header_data, record_data_list);
    }

    println!("{:?}", sqlite::version());
    let mut rl = Editor::<()>::new().unwrap();

    loop {
        let readline = rl.readline("QUERY>> ");
        // let mut query = input_query(&rl);
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let mut query = String::from(line.as_str());
                query.retain(|c| c != ';');

                let query_vec: Vec<&str> = query.split(' ').collect();
                let ope = query_vec[0].to_uppercase();
                if ope == "SELECT" {
                    select_table(&connection, query);
                } else if ope == "INSERT" {
                    insert_table(&connection, &query, is_sync);
                } else if ope == "UPDATE" {
                    update_table(&connection, query, is_sync);
                } else if ope == "DELETE" {
                    delete_table(&connection, query, is_sync);
                } else {
                    connection.execute(query).unwrap();
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
}

fn update_table(connection: &sqlite::Connection, update_query: String, is_sync: bool) {
    let update_query = if is_sync == true { update_query + " RETURNING *" } else { update_query };
    let mut stmt = connection.prepare(update_query).unwrap().into_cursor();
    println!("{:?}", stmt.try_next().unwrap());
}

fn insert_table(connection: &sqlite::Connection, query: &String, is_sync: bool) {
    let insert_query = if is_sync == true { String::from(query) + " RETURNING *" } else { String::from(query) };
    let mut stmt = connection.prepare(insert_query).unwrap().into_cursor();
    let result = stmt.try_next().unwrap().unwrap();

    if is_sync == true {
        let insert_query_vec: Vec<&str> = query.split(' ').collect();
        let re = Regex::new(r"\(.*?\)").unwrap();
        let table_name = re.replace_all(insert_query_vec[2], "");


        let mut record = Vec::new();
        for (i, r) in result.iter().enumerate() {
            if i != 0 {
                let type_kind = r.kind();
                match type_kind {
                    Type::Integer => {
                        record.push(r.as_integer().unwrap().to_string());
                    },
                    Type::String => {
                        record.push(String::from(r.as_string().unwrap()));
                    },
                    Type::Binary => {
                        record.push(r.as_binary().unwrap().escape_ascii().to_string());
                    },
                    Type::Float => {
                        record.push(r.as_float().unwrap().to_string());
                    },
                    Type::Null => {
                        record.push(String::from(""));
                    },
                }
            }
        }

        let file_name = get_file_name_in_connection_table(connection, table_name.to_string());
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_name)
            .unwrap();


        let mut wtr = csv::Writer::from_writer(file);

        wtr.write_record(&record).unwrap();


        // let row_number = result[0].as_integer().unwrap();
        // let mut rdr = csv::Reader::from_path(table_name).unwrap();
        // for result in rdr.records() {
        //     let record = result.unwrap();
        //     for (i, r) in record.iter().enumerate() {
        //         if i + 1 == row_number as usize {
        //
        //         }
        //     }
        // }
    }
}

fn get_file_name_in_connection_table(connection: &sqlite::Connection, table_name: String) -> String {
    println!("{:?}", table_name);
    let mut cursor = connection
    .prepare(format!("SELECT * FROM cql_connect_file_table WHERE table_name = '{}'", table_name))
    .unwrap()
    .into_cursor();

    let row = cursor.next().unwrap().unwrap();
    let path = row.get::<String, _>(1);
    let re = Regex::new(r"\[.*?\]").unwrap();
    println!("path = {:?}", path.as_str());

    let brackets_file_name = re.find(path.as_str()).unwrap().as_str();

    println!("file_name = {:?}", brackets_file_name);
    let brackets_re = Regex::new(r"\[|\]").unwrap();
    let file_name = brackets_re.replace_all(brackets_file_name, "");
    println!("file_name = {:?}", file_name);

    return file_name.to_string();


}

fn delete_table(connection: &sqlite::Connection, delete_query: String, is_sync: bool) {
    let delete_query = if is_sync == true { delete_query + " RETURNING *" } else { delete_query };
    let mut stmt = connection.prepare(delete_query).unwrap().into_cursor();
    println!("{:?}", stmt.try_next().unwrap());
}

fn select_table(connection: &sqlite::Connection, select_query: String) {
    let mut columns = Vec::new();
    let mut records = Vec::new();
    let mut index = 0;
    connection
        .iterate(select_query, |pairs| {
            let mut record = Vec::new();
            index += 1;
            let count = pairs.iter().count();
            for &(column, value) in pairs.iter() {
                println!("{:?}", value);
                let value = if value == None { "" } else { value.unwrap() };
                record.push(Cell::new(&value.to_string()));
                if index == 1 {
                    columns.push(Cell::new(&column.to_string()));
                }
                if columns.len() == count && record.len() == columns.len() {
                    records.push(record);
                    record = Vec::new();
                }
            }
            true
        })
        .unwrap();

    let mut table = Table::new();
    let mut table_data = Vec::new();

    table_data.push(columns);
    for record in records {
        table_data.push(record);
    }

    for data in table_data {
        table.add_row(Row::new(data));
    }
    table.printstd();
}

fn create_connect_file_table(connection: &sqlite::Connection, file_name: &String, path: &String) {
    println!("{:?}", file_name);
    println!("{:?}", path);
    // let re = Regex::new(r"\.").unwrap();
    // let path = re.replace_all(path, r"\.");
    // println!("{}", Path::new(&path).display());
    let path = String::from("[") + path + "]";
    println!("{:?}", path);
    let create_table_query = &String::from(
            "CREATE TABLE IF NOT EXISTS cql_connect_file_table (table_name TEXT, path TEXT);"
    );

    connection.execute(create_table_query).unwrap();

    let insert_query = String::from(format!(
            "INSERT INTO cql_connect_file_table (table_name, path) VALUES ('{}', '{}')",
            file_name, path
            ));
    println!("{:?}", insert_query);
    connection.execute(insert_query).unwrap();

    select_table(connection, String::from("SELECT * FROM cql_connect_file_table"));

}

fn create_table(connection: &sqlite::Connection, table_name: &String, columns_data: &Vec<String>) {
    let mut column_data = Vec::new();
    for (i, c) in columns_data.iter().enumerate() {
        if i == 0 {
            column_data.push(c.to_string() + " INTEGER PRIMARY KEY AUTOINCREMENT");
        } else {
            column_data.push(c.to_string() + " TEXT");
        }
    }
    let columns = column_data.join(", ");

    println!("{:?}", table_name);
    println!("{:?}", columns);
    println!(" CREATE TABLE IF NOT EXISTS {table_name} ({columns}); ");
    let create_table_query = &String::from(format!(
        " CREATE TABLE IF NOT EXISTS {} ({});",
        table_name, columns
    ));
    println!("{:?}", create_table_query);

    connection.execute(create_table_query).unwrap();
}

fn insert_records(
    connection: &sqlite::Connection,
    table_name: &String,
    header_data: &Vec<String>,
    record_data_list: Vec<Vec<String>>,
) {
    let headers = header_data
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let mut insert_query = "".to_string();
    for (i, record_data) in record_data_list.iter().enumerate() {
        let placeholders = (i+1).to_string() + ", " + record_data
            .iter()
            .map(|x| if x == "" { "NULL" } else {x} )
            .collect::<Vec<_>>()
            .join(", ").as_str();

        let query = String::from(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name, headers, placeholders
        ));

        insert_query.push_str(query.as_str());
        insert_query.push_str(" ; ");
    }
    println!("{:?}", insert_query);
    connection.execute(insert_query).unwrap();
}

fn input_query() -> String {
    let mut word = String::new();
    print!("QUERY> ");
    stdout().flush().unwrap();
    stdin().read_line(&mut word).ok();
    return word.trim().to_string();
}

fn read_csv(path: &String) -> (Vec<String>, Vec<Vec<String>>) {
    let mut header_data = Vec::new();
    let mut record_data_list = Vec::new();

    let mut rdr = csv::Reader::from_path(path).unwrap();
    let headers = rdr.headers().unwrap().clone();

    header_data.push("cql_row_id".to_string());
    for header in headers.iter() {
        header_data.push(header.to_string());
    }

    for result in rdr.records() {
        let record = result.unwrap();
        let mut s = Vec::new();
        for r in record.iter() {
            s.push(r.to_string());
        }
        record_data_list.push(s);
    }

    return (header_data, record_data_list);
}

fn read_dir<P: AsRef<Path>>(path: P) -> io::Result<Vec<String>> {
    Ok(fs::read_dir(path)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().ok()?.is_file() {
                let is_csv = entry.path().extension()? == "csv";
                if is_csv {
                    return Some(entry.file_name().to_string_lossy().into_owned());
                } else {
                    return None;
                }
            } else {
                None
            }
        })
        .collect())
}
