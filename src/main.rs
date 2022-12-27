#[macro_use]
extern crate prettytable;

use table::Table;

use std::env;
use std::fs::File;
use std::io::{BufWriter, Read};

use database::Database;
use parser::create::CreateQuery;
use parser::insert::InsertQuery;
use parser::select::SelectQuery;

use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

mod database;
mod parser;
mod table;

enum MetaCommand {
    Exit,
    ListTables,
    PrintData,
    Persist,
    Restore,
    Unknown(String),
}

impl MetaCommand {
    fn new(command: String) -> MetaCommand {
        match command.as_ref() {
            ".exit" => MetaCommand::Exit,
            ".tables" => MetaCommand::ListTables,
            ".data" => MetaCommand::PrintData,
            ".persist" => MetaCommand::Persist,
            ".restore" => MetaCommand::Restore,
            _ => MetaCommand::Unknown(command),
        }
    }
}

enum DbCommand {
    Insert(String),
    Delete(String),
    Update(String),
    CreateTable(String),
    Select(String),
    Unknown(String),
}

impl DbCommand {
    fn new(command: String) -> DbCommand {
        let v = command.split(" ").collect::<Vec<&str>>();
        match v[0] {
            "insert" => DbCommand::Insert(command),
            "update" => DbCommand::Update(command),
            "delete" => DbCommand::Delete(command),
            "create" => DbCommand::CreateTable(command),
            "select" => DbCommand::Select(command),
            _ => DbCommand::Unknown(command),
        }
    }
}

enum CommandType {
    MetaCommand(MetaCommand),
    DbCommand(DbCommand),
}

fn get_command_type(cmd: &String) -> CommandType {
    match cmd.starts_with(".") {
        true => CommandType::MetaCommand(MetaCommand::new(cmd.to_owned())),
        false => CommandType::DbCommand(DbCommand::new(cmd.to_owned())),
    }
}

fn handle_meta_command(cmd: MetaCommand, db: &mut Database) {
    match cmd {
        MetaCommand::Exit => std::process::exit(0),
        MetaCommand::ListTables => {
            if db.tables.is_empty() {
                println!("No tables found");
            }
            for table in &db.tables {
                table.print_table();
            }
        }
        MetaCommand::PrintData => {
            for table in &db.tables {
                table.print_table_data();
            }
        }
        MetaCommand::Persist => {
            let mut buffered_writer = BufWriter::new(File::create("dbfile1.bin").unwrap());
            bincode::serialize_into(&mut buffered_writer, &db)
                .expect("Error while trying to serialize to binary data");
        }
        MetaCommand::Restore => {
            let mut file = File::open("dbfile1.bin").unwrap();
            let decoded_db: Database = bincode::deserialize_from(&mut file).unwrap();
            *db = decoded_db;
        }
        MetaCommand::Unknown(cmd) => println!("Unrecognized meta command {}", cmd),
    }
}

fn process_command(query: String, db: &mut Database) {
    let dialect = MySqlDialect {};
    let statements = &Parser::parse_sql(&dialect, &query).unwrap();

    for s in statements {
        println!("{:?}", s);
        match s {
            Statement::CreateTable { .. } => {
                let cq = CreateQuery::new(s).unwrap();
                db.tables.push(Table::new(cq));
            }
            Statement::Insert { .. } => {
                let iq = InsertQuery::new(s);
                match iq {
                    Ok(iq) => {
                        let table_name = iq.table_name;
                        let columns = iq.columns;
                        let values = iq.values;
                        println!("cols = {:?}\n vals = {:?}", columns, values);
                        match db.table_exists(table_name.to_string()) {
                            true => {
                                let db_table = db.get_table_mut(table_name.to_string());
                                match columns.iter().all(|c| db_table.column_exist(c.to_string())) {
                                    true => {
                                        for value in &values {
                                            match db_table
                                                .does_violate_unique_constraint(&columns, value)
                                            {
                                                Err(err) => println!(
                                                    "Unique key constaint violation: {}",
                                                    err
                                                ),
                                                Ok(()) => {
                                                    db_table.insert_row(&columns, &values);
                                                }
                                            }
                                        }
                                    }
                                    false => {
                                        println!("Cannot insert, some of the columns do not exist");
                                    }
                                }
                            }
                            false => println!("Table doesn't exist"),
                        }
                    }
                    Err(err) => println!("Error while trying to parse insert statement: {}", err),
                }
            }
            Statement::Query(_q) => {
                let select_query = SelectQuery::new(&s);
                match select_query {
                    Ok(mut sq) => match db.table_exists((&sq.from).to_string()) {
                        true => {
                            let db_table = db.get_table((&sq.from).to_string());

                            let cloned_projection = sq.projection.clone();

                            for p in &cloned_projection {
                                if p == "*" {
                                    let new_projections = db_table
                                        .columns
                                        .iter()
                                        .map(|c| c.name.to_string())
                                        .collect::<Vec<String>>();
                                    sq.insert_projections(new_projections);
                                }
                            }

                            for col in &sq.projection {
                                if !db_table.column_exist((&col).to_string()) {
                                    println!(
                                        "Cannot execute query, cannot find column {} in table {}",
                                        col, db_table.name
                                    );
                                    return;
                                }
                            }

                            println!("sq = {:?}", &sq);
                            db_table.execute_select_query(&sq);
                        }
                        false => {
                            println!("Cannot execute query the table {} doesn't exists", sq.from);
                        }
                    },
                    Err(error) => {
                        println!("{}", error);
                    }
                }
            }
            _ => {
                println!("Not a insert, create table or select query");
            }
        }
    }
}

use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut command = String::new();
    let mut db = Database::new();

    for arg in args {
        match File::open(arg) {
            Ok(mut file) => {
                let mut query = String::new();
                file.read_to_string(&mut query).unwrap();

                let dialect = MySqlDialect {};
                let mut tokenizer = Tokenizer::new(&dialect, &query);
                match tokenizer.tokenize() {
                    Ok(t) => {
                        println!("{:?}", t);
                    }
                    Err(e) => println!("err {:?}", e),
                }
                process_command(query, &mut db);

                println!("query processed");
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }

    let mut rl = Editor::<()>::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(command) => {
                rl.add_history_entry(command.as_str());
                match get_command_type(&command.trim().to_owned()) {
                    CommandType::DbCommand(_) => {
                        process_command(command.trim().to_string(), &mut db);
                    }
                    CommandType::MetaCommand(cmd) => {
                        handle_meta_command(cmd, &mut db);
                    }
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")
}
