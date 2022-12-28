use crate::table::Table;

use std::fs::File;
use std::io::BufWriter;

use crate::database::Database;
use crate::parser::create::CreateQuery;
use crate::parser::insert::InsertQuery;
use crate::parser::select::SelectQuery;

use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub enum MetaCommand {
    Exit,
    ListTables,
    PrintData,
    Persist(String),
    Restore(String),
    Unknown(String),
}

impl MetaCommand {
    fn trim_command(command: &str, meta_command: &str) -> String {
        let command = command.replace(meta_command, "");
        let command = command.replace('\'', "");
        let command = command.replace('\"', "");
        command.trim().to_string()
    }

    pub fn new(command: String) -> MetaCommand {
        match command.as_ref() {
            ".exit" => MetaCommand::Exit,
            ".tables" => MetaCommand::ListTables,
            ".data" => MetaCommand::PrintData,
            _ => {
                if command.starts_with(".persist") {
                    let trimmed_command = Self::trim_command(&command, ".persist");
                    MetaCommand::Persist(trimmed_command)
                } else if command.starts_with(".restore") {
                    let trimmed_command = Self::trim_command(&command, ".restore");
                    MetaCommand::Restore(trimmed_command)
                } else {
                    MetaCommand::Unknown(command)
                }
            }
        }
    }
}

pub enum DbCommand {
    Insert(String),
    Delete(String),
    Update(String),
    CreateTable(String),
    Select(String),
    Unknown(String),
}

impl DbCommand {
    pub fn new(command: String) -> DbCommand {
        let v = command.split(' ').collect::<Vec<&str>>();
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

pub enum CommandType {
    MetaCommand(MetaCommand),
    DbCommand(DbCommand),
}

pub fn get_command_type(cmd: &String) -> CommandType {
    match cmd.starts_with('.') {
        true => CommandType::MetaCommand(MetaCommand::new(cmd.to_owned())),
        false => CommandType::DbCommand(DbCommand::new(cmd.to_owned())),
    }
}

pub fn handle_meta_command(cmd: MetaCommand, db: &mut Database) {
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
        MetaCommand::Persist(file_path) => {
            let mut buffered_writer = BufWriter::new(
                File::create(file_path).expect("Could not create or write to file to persist to"),
            );
            bincode::serialize_into(&mut buffered_writer, &db)
                .expect("Error while trying to serialize to binary data");
        }
        MetaCommand::Restore(file_path) => {
            let mut file =
                File::open(file_path).expect("Could not open or find file to restore from");
            let decoded_db: Database = bincode::deserialize_from(&mut file).unwrap();
            *db = decoded_db;
        }
        MetaCommand::Unknown(cmd) => eprintln!("Unrecognized meta command {cmd}"),
    }
}

pub fn process_command(query: String, db: &mut Database) {
    let dialect = MySqlDialect {};
    let statements = &Parser::parse_sql(&dialect, &query).unwrap();

    for statement in statements {
        println!("{:?}", statement);
        match statement {
            Statement::CreateTable { .. } => {
                let create_query = CreateQuery::new(statement).unwrap();
                db.tables.push(Table::new(create_query));
            }
            Statement::Insert { .. } => {
                let insert_query = InsertQuery::new(statement);
                match insert_query {
                    Ok(InsertQuery {
                        table_name,
                        columns,
                        values,
                        ..
                    }) => {
                        println!("cols = {:?}\n vals = {:?}", columns, values);
                        match db.table_exists(table_name.to_string()) {
                            true => {
                                let db_table = db.get_table_mut(table_name.to_string());
                                match columns
                                    .iter()
                                    .all(|c| db_table.column_exists(c.to_string()))
                                {
                                    true => {
                                        for value in &values {
                                            match db_table
                                                .does_violate_unique_constraint(&columns, value)
                                            {
                                                Err(err) => eprintln!(
                                                    "Unique key constaint violation: {err}"
                                                ),
                                                Ok(()) => {
                                                    db_table.insert_row(&columns, &values);
                                                }
                                            }
                                        }
                                    }
                                    false => {
                                        eprintln!(
                                            "Cannot insert, some of the columns do not exist"
                                        );
                                    }
                                }
                            }
                            false => eprintln!("Table doesn't exist"),
                        }
                    }
                    Err(err) => eprintln!("Error while trying to parse insert statement: {err}"),
                }
            }
            Statement::Query(_) => {
                let select_query = SelectQuery::new(statement);
                match select_query {
                    Ok(mut sq) => match db.table_exists(sq.from.to_string()) {
                        true => {
                            let db_table = db.get_table(sq.from.to_string());

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
                                if !db_table.column_exists((&col).to_string()) {
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
                            eprintln!("Cannot execute query the table {} doesn't exists", sq.from)
                        }
                    },
                    Err(error) => eprintln!("{error}"),
                }
            }
            _ => {
                println!("Not a insert, create table or select query");
            }
        }
    }
}
