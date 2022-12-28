# Writing an SQL database in Rust Part 0

In this series we're going to write a basic SQL database in Rust. We're going to offload most of the complexity into other libraries, so this series will have less code than a full fledged from scratch SQL database, but in exchange, it'll be easier to add in new features.

But in this part we can start out with the major parts of an SQL Database and how to implement each part.

A database takes commands (like insert, delete, create) and then applies them to some in-memory representation of a table. Finally, the in-memory database structure is persisted safely to disk, so it can be reloaded at any time.

Therefore, we must parse SQL, create a data structure that is efficient at representing and querying various data types, serialize that to disk and deserialize it from disk.

Let's start off with a basic REPL that can take our commands.

## A SQL REPL

Let's start out by creating a REPL that can accept input and echo it back to us:

Create a new rust project with cargo and `cargo add rustyline` to add rustyline to our project.

Adding this example code gets us started:

Create new file: src/main.rs

```rs
use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};

fn main() -> Result<()> {
    let mut rl = Editor::<()>::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(command) => {
                println!("{command}");
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
```

Great, we have a REPL. Let's start out by implementing the most critical command of them all: `exit`.

Let's create a new commands file to store our commands.

Create new file: src/commands.rs

```rs
pub enum MetaCommand {
    Exit,
}
```

We make a way to exit the repl:

```rs
impl MetaCommand {
    pub fn new(command: String) -> MetaCommand {
        match command.as_ref() {
            ".exit" => MetaCommand::Exit,
        }
    }
}

pub fn handle_meta_command(cmd: MetaCommand) {
    match cmd {
        MetaCommand::Exit => std::process::exit(0),
    }
}
```

And we hook it up to our repl.

Edit three lines in src/main.rs

```rs
Ok(command) => {
    if command.starts_with('.') {
      handle_meta_command(command);
    } else {
      println!("{command}");
    }
}
```

And now, we can exit the repl by writing `.exit`.

## Parsing Insert Queries

Let's move onto a more interesting part of the database: parsing queries.

We'll add `sqlparser` as a dependency to our project with `cargo add sqlparser`.

What does an insert query need? A table to insert into, columns to insert into, and values to insert.

Create new file src/parser.rs:

```rs
pub mod insert;
```

Create new file src/parser/insert.rs:

```rs
pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<String>>,
}
```


```rs
use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value, Values};

impl InsertQuery {
    pub fn new(statement: &Statement) -> Result<InsertQuery, String> {
        let mut tname: Option<String> = None;
        let mut columns: Vec<String> = vec![];
        let mut all_vals: Vec<Vec<String>> = vec![];

        if let Statement::Insert {
            table_name,
            columns: cols,
            source,
            ..
        } = statement
        {
            tname = Some(table_name.to_string());
            for c in cols {
                columns.push(c.to_string());
            }
            let Query { body, .. } = &**source;
            if let SetExpr::Values(values) = body.as_ref() {
                let Values { rows, .. } = values;
                for row in rows {
                    let mut value_set: Vec<String> = vec![];
                    for expr in row {
                        match expr {
                            Expr::Value(v) => match v {
                                Value::Number(n, _) => {
                                    value_set.push(n.to_string());
                                }
                                Value::Boolean(b) => match b {
                                    true => value_set.push("true".to_string()),
                                    false => value_set.push("false".to_string()),
                                },
                                Value::SingleQuotedString(sqs) => {
                                    value_set.push(sqs.to_string());
                                }
                                Value::Null => {
                                    value_set.push("Null".to_string());
                                }
                                _ => {}
                            },
                            Expr::Identifier(i) => {
                                value_set.push(i.to_string());
                            }
                            _ => {}
                        }
                    }
                    all_vals.push(value_set);
                }
            }
        }

        match tname {
            Some(t) => Ok(InsertQuery {
                table_name: t,
                columns,
                values: all_vals,
            }),
            None => Err(String::from("Cannot parse insert query")),
        }
    }
}
```
