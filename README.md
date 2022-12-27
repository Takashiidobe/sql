# Writing an SQL database in Rust

Three steps are required in writing an SQL database in Rust:

1. Parsing the SQL statement into an AST.
2. Executing the statement on an in-memory representation of the table.
3. Persisting that data safely on disk.

To parse the SQL, [SqlParser](https://github.com/sqlparser-rs/sqlparser-rs) is used.
To represent tables, Rust's BTreeMaps are used.
To save the data on disk, [SSTables](https://github.com/ikatson/rust-sstb) is used.
