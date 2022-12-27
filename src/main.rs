use std::env;
use std::fs::File;
use std::io::Read;

use database::Database;

use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::Tokenizer;

mod command;
mod database;
mod parser;
mod table;

use command::{get_command_type, handle_meta_command, process_command, CommandType};

use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
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
