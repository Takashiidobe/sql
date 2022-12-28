use prettytable::{row, Cell, Row, Table as PTable};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::result::Result;

use crate::parser::{
    create::CreateQuery,
    select::{Binary, Expression, Operator, SelectQuery},
};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum DataType {
    Int,
    Str,
    Float,
    Bool,
    Invalid,
}

impl DataType {
    pub fn new(cmd: String) -> DataType {
        match cmd.to_lowercase().as_ref() {
            "int" => DataType::Int,
            "string" => DataType::Str,
            "float" => DataType::Float,
            "double" => DataType::Float,
            "bool" => DataType::Bool,
            _ => {
                println!("Invalid data type given {}", cmd);
                DataType::Invalid
            }
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::Int => f.write_str("Int"),
            DataType::Str => f.write_str("Str"),
            DataType::Float => f.write_str("Float"),
            DataType::Bool => f.write_str("Boolean"),
            DataType::Invalid => f.write_str("Invalid"),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ColumnHeader {
    pub name: String,
    pub datatype: DataType,
    pub is_indexed: bool,
    pub index: ColumnIndex,
    pub is_primary_key: bool,
}

impl ColumnHeader {
    pub fn new(name: String, datatype: String, is_primary_key: bool) -> ColumnHeader {
        let dt = DataType::new(datatype);
        let index = match dt {
            DataType::Int => ColumnIndex::Int(BTreeMap::new()),
            DataType::Str => ColumnIndex::Str(BTreeMap::new()),
            DataType::Bool => ColumnIndex::Bool(BTreeMap::new()),
            DataType::Float | DataType::Invalid => ColumnIndex::None,
        };

        ColumnHeader {
            name,
            datatype: dt,
            is_indexed: is_primary_key,
            index,
            is_primary_key,
        }
    }

    pub fn get_mut_index(&mut self) -> &mut ColumnIndex {
        &mut self.index
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ColumnData {
    Int(Vec<i32>),
    Str(Vec<String>),
    Float(Vec<f32>),
    Bool(Vec<bool>),
    None,
}

impl ColumnData {
    fn get_serialized_col_data(&self) -> Vec<String> {
        match self {
            ColumnData::Int(cd) => cd.iter().map(|v| v.to_string()).collect(),
            ColumnData::Float(cd) => cd.iter().map(|v| v.to_string()).collect(),
            ColumnData::Str(cd) => cd.iter().map(|v| v.to_string()).collect(),
            ColumnData::Bool(cd) => cd.iter().map(|v| v.to_string()).collect(),
            ColumnData::None => panic!("Found None in columns"),
        }
    }

    fn get_serialized_col_data_by_index(&self, indices: &[usize]) -> Vec<String> {
        let mut selected_data = vec![];
        match self {
            ColumnData::Int(cd) => {
                indices
                    .iter()
                    .for_each(|i| selected_data.push((cd[*i]).to_string()));
            }
            ColumnData::Float(cd) => {
                indices
                    .iter()
                    .for_each(|i| selected_data.push((cd[*i]).to_string()));
            }
            ColumnData::Str(cd) => {
                indices
                    .iter()
                    .for_each(|i| selected_data.push((cd[*i]).to_string()));
            }
            ColumnData::Bool(cd) => {
                indices
                    .iter()
                    .for_each(|i| selected_data.push((cd[*i]).to_string()));
            }
            ColumnData::None => panic!("Found None in columns"),
        }
        selected_data
    }

    fn work<A, B, C, D>(
        &self,
        search_term: &str,
        scanned_vals: &mut Vec<usize>,
        func1: A,
        func2: B,
        func3: C,
        func4: D,
    ) -> Vec<usize>
    where
        A: Fn(i32, i32) -> bool,
        B: Fn(f32, f32) -> bool,
        C: Fn(&String, &String) -> bool,
        D: Fn(bool, bool) -> bool,
    {
        match self {
            ColumnData::Int(cd) => {
                let search_term = search_term.parse::<i32>().unwrap();

                for (idx, i) in cd.iter().enumerate() {
                    if func1(*i, search_term) {
                        scanned_vals.push(idx);
                    }
                }
                scanned_vals.to_vec()
            }
            ColumnData::Float(cd) => {
                let search_term = search_term.parse::<f32>().unwrap();

                for (idx, i) in cd.iter().enumerate() {
                    if func2(*i, search_term) {
                        scanned_vals.push(idx);
                    }
                }
                scanned_vals.to_vec()
            }
            ColumnData::Str(cd) => {
                let search_term = search_term.parse::<String>().unwrap();

                for (idx, i) in cd.iter().enumerate() {
                    if func3(i, &search_term) {
                        scanned_vals.push(idx);
                    }
                }
                scanned_vals.to_vec()
            }
            ColumnData::Bool(cd) => {
                let search_term = search_term.parse::<bool>().unwrap();
                for (idx, i) in cd.iter().enumerate() {
                    if func4(*i, search_term) {
                        scanned_vals.push(idx);
                    }
                }
                scanned_vals.to_vec()
            }
            ColumnData::None => panic!("Found None in columns"),
        }
    }

    fn get_serialized_col_data_by_scanning(&self, expr: &Expression) -> Vec<usize> {
        let search_term = (expr.right).to_string();
        let mut scanned_vals = vec![];
        match &expr.op {
            Operator::Binary(binary_op) => match binary_op {
                Binary::NotEq => Self::work(
                    self,
                    &search_term,
                    &mut scanned_vals,
                    |a, b| a != b,
                    |a, b| a != b,
                    |a, b| a != b,
                    |a, b| a != b,
                ),
                Binary::Eq => self.work(
                    &search_term,
                    &mut scanned_vals,
                    |a, b| a == b,
                    |a, b| a == b,
                    |a, b| a == b,
                    |a, b| a == b,
                ),
                Binary::Gt => self.work(
                    &search_term,
                    &mut scanned_vals,
                    |a, b| a > b,
                    |a, b| a > b,
                    |a, b| a > b,
                    |a, b| a & !b,
                ),
                Binary::Lt => self.work(
                    &search_term,
                    &mut scanned_vals,
                    |a, b| a < b,
                    |a, b| a < b,
                    |a, b| a < b,
                    |a, b| !a & b,
                ),
                Binary::LtEq => self.work(
                    &search_term,
                    &mut scanned_vals,
                    |a, b| a <= b,
                    |a, b| a <= b,
                    |a, b| a <= b,
                    |a, b| a <= b,
                ),
                Binary::GtEq => self.work(
                    &search_term,
                    &mut scanned_vals,
                    |a, b| a >= b,
                    |a, b| a >= b,
                    |a, b| a >= b,
                    |a, b| a >= b,
                ),
            },
        }
    }

    fn count(&self) -> usize {
        match self {
            ColumnData::Int(cd) => cd.len(),
            ColumnData::Float(cd) => cd.len(),
            ColumnData::Str(cd) => cd.len(),
            ColumnData::Bool(cd) => cd.len(),
            ColumnData::None => panic!("Found None in columns"),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ColumnIndex {
    Int(BTreeMap<i32, usize>),
    Str(BTreeMap<String, usize>),
    Bool(BTreeMap<bool, usize>),
    None,
}

impl ColumnIndex {
    fn get_idx_data(&self, val: &String) -> Result<Option<&usize>, String> {
        match self {
            ColumnIndex::Int(index) => match val.parse::<i32>() {
                Ok(val) => Ok(index.get(&val)),
                Err(e) => Err(e.to_string()),
            },
            ColumnIndex::Bool(index) => match val.parse::<bool>() {
                Ok(val) => Ok(index.get(&val)),
                Err(e) => Err(e.to_string()),
            },
            ColumnIndex::Str(index) => Ok(index.get(val)),
            ColumnIndex::None => Ok(None),
        }
    }

    fn get_indexes_from_op<T: Clone>(val: T, op: Binary) -> (Bound<T>, Bound<T>) {
        match op {
            Binary::Eq => (Included(val.clone()), Included(val)),
            Binary::NotEq => (Excluded(val.clone()), Excluded(val)),
            Binary::Gt => (Excluded(val), Unbounded),
            Binary::GtEq => (Included(val), Unbounded),
            Binary::Lt => (Unbounded, Excluded(val)),
            Binary::LtEq => (Unbounded, Included(val)),
        }
    }

    fn get_idx_data_by_range(&self, val: &String, op: Binary) -> Result<Vec<usize>, String> {
        let mut indexes: Vec<usize> = vec![];
        match self {
            ColumnIndex::Int(index) => match val.parse::<i32>() {
                Ok(val) => {
                    for (_, idx) in index.range(Self::get_indexes_from_op::<i32>(val, op)) {
                        indexes.push(*idx);
                    }
                    Ok(indexes)
                }
                Err(e) => Err(e.to_string()),
            },

            ColumnIndex::Bool(index) => match val.parse::<bool>() {
                Ok(val) => {
                    for (_, idx) in index.range(Self::get_indexes_from_op::<bool>(val, op)) {
                        indexes.push(*idx);
                    }
                    Ok(indexes)
                }
                Err(e) => Err(e.to_string()),
            },
            ColumnIndex::Str(index) => {
                for (_, idx) in
                    index.range(Self::get_indexes_from_op::<String>(val.to_string(), op))
                {
                    indexes.push(*idx);
                }
                Ok(indexes)
            }
            ColumnIndex::None => Ok(indexes),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Table {
    pub columns: Vec<ColumnHeader>,
    pub name: String,
    pub rows: HashMap<String, ColumnData>,
}

impl Table {
    pub fn new(cq: CreateQuery) -> Table {
        let table_name = cq.table_name;
        let columns = cq.columns;

        let mut table_cols: Vec<ColumnHeader> = vec![];
        let mut table_data: HashMap<String, ColumnData> = HashMap::new();
        for c in &columns {
            table_cols.push(ColumnHeader::new(
                c.name.to_string(),
                c.datatype.to_string(),
                c.is_pk,
            ));

            match DataType::new(c.datatype.to_string()) {
                DataType::Int => table_data.insert(c.name.to_string(), ColumnData::Int(vec![])),
                DataType::Float => table_data.insert(c.name.to_string(), ColumnData::Float(vec![])),
                DataType::Str => table_data.insert(c.name.to_string(), ColumnData::Str(vec![])),
                DataType::Bool => table_data.insert(c.name.to_string(), ColumnData::Bool(vec![])),
                DataType::Invalid => table_data.insert(c.name.to_string(), ColumnData::None),
            };
        }

        Table {
            columns: table_cols,
            name: table_name,
            rows: table_data,
        }
    }

    pub fn get_column(&self, col_name: String) -> &ColumnHeader {
        self.columns
            .iter()
            .filter(|c| c.name == col_name)
            .collect::<Vec<&ColumnHeader>>()
            .first()
            .unwrap()
    }

    pub fn does_violate_unique_constraint(
        &self,
        cols: &[String],
        values: &[String],
    ) -> Result<(), String> {
        for c in &self.columns {
            if c.is_primary_key {
                let col_idx = &c.index;
                for (idx, name) in cols.iter().enumerate() {
                    if *name == c.name {
                        let val = &values[idx];

                        match col_idx {
                            ColumnIndex::Int(index) => {
                                if let true = index.contains_key(
                                    &val.parse::<i32>().expect("Couldn't parse value to an int."),
                                ) {
                                    return Err(format!(
                                        "Error: unique constraint violation for column {}.
                            Value {} already exists for column {}",
                                        *name, val, *name
                                    ));
                                } else {
                                    return Ok(());
                                }
                            }
                            ColumnIndex::Bool(index) => {
                                if let true = index.contains_key(
                                    &val.parse::<bool>()
                                        .expect("couldn't parse value to be a boolean"),
                                ) {
                                    return Err(format!(
                                        "Error: unique constraint violation for column {}.
                            Value {} already exists for column {}",
                                        *name, val, *name
                                    ));
                                } else {
                                    return Ok(());
                                }
                            }
                            ColumnIndex::Str(index) => {
                                if let true = index.contains_key(val) {
                                    return Err(format!(
                                        "Error: unique constraint violation for column {}.
                            Value {} already exists for column {}",
                                        *name, val, *name
                                    ));
                                } else {
                                    return Ok(());
                                }
                            }
                            ColumnIndex::None => {
                                return Err(format!("Error: cannot find index for column {name}"));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn insert_row(&mut self, cols: &[String], values: &[Vec<String>]) {
        (0..cols.len()).for_each(|i| {
            let key = &cols[i];
            let table_col_data = self.rows.get_mut(&key.to_string()).unwrap();

            let mut column_headers = self
                .columns
                .iter_mut()
                .filter(|c| c.name == *key)
                .collect::<Vec<&mut ColumnHeader>>();

            let col_index = column_headers
                .first_mut()
                .expect("Couldn't find column to insert row")
                .get_mut_index();

            values.iter().for_each(|value| {
                let val = &value[i];
                match table_col_data {
                    ColumnData::Int(c_vec) => {
                        let val = val.parse::<i32>().unwrap();
                        c_vec.push(val);
                        if let ColumnIndex::Int(index) = col_index {
                            index.insert(val, table_col_data.count() - 1);
                        }
                    }
                    ColumnData::Bool(c_vec) => {
                        let val = val.parse::<bool>().unwrap();
                        c_vec.push(val);
                        if let ColumnIndex::Bool(index) = col_index {
                            index.insert(val, table_col_data.count() - 1);
                        }
                    }
                    ColumnData::Str(c_vec) => {
                        let val = val.to_string();
                        c_vec.push(val.clone());
                        if let ColumnIndex::Str(index) = col_index {
                            index.insert(val, table_col_data.count() - 1);
                        }
                    }
                    ColumnData::Float(c_vec) => {
                        c_vec.push(val.parse::<f32>().unwrap());
                    }
                    ColumnData::None => panic!("None data Found"),
                }
            });
        });
    }

    fn select_data(&self, columns_to_fetch: &[String], indexes: &[usize]) -> Vec<Vec<String>> {
        let mut data = vec![];
        for col in columns_to_fetch {
            let row = self.rows.get(col).unwrap();
            data.push(row.get_serialized_col_data_by_index(indexes));
        }
        data
    }

    fn execute_select_query_without_index(&self, sq: &SelectQuery) -> Vec<Vec<String>> {
        let expr = sq.where_expressions.first();
        match expr {
            Some(where_expr) => {
                let col = self.get_column(where_expr.left.to_string());
                let row = self.rows.get(&col.name).unwrap();

                let indices = row.get_serialized_col_data_by_scanning(where_expr);

                sq.projection
                    .iter()
                    .map(|col_name| {
                        self.rows
                            .get(&col_name.to_string())
                            .expect("The searched column doesn't exist")
                    })
                    .map(|col_data| col_data.get_serialized_col_data_by_index(&indices))
                    .collect::<Vec<Vec<String>>>()
            }

            None => {
                panic!("Invalid where expression given");
            }
        }
    }

    fn execute_select(
        &self,
        sq: &SelectQuery,
        where_expr: &Expression,
        op: &Binary,
        col: &ColumnHeader,
    ) -> Vec<Vec<String>> {
        match &col
            .index
            .get_idx_data_by_range(&where_expr.right, op.clone())
        {
            Ok(indexes) => self.select_data(&sq.projection, indexes),
            Err(e) => {
                panic!("Error while trying to retrieve value from index: {}", e);
            }
        }
    }

    pub fn execute_select_query(&self, sq: &SelectQuery) {
        let mut data: Vec<Vec<String>> = vec![];

        let expr = sq.where_expressions.first();
        match expr {
            Some(where_expr) => {
                let col = self.get_column(where_expr.left.to_string());

                if col.is_indexed {
                    println!("Executing select expression with index");
                    match &where_expr.op {
                        Operator::Binary(bop) => match bop {
                            Binary::Eq => match &col.index.get_idx_data(&where_expr.right) {
                                Ok(v) => {
                                    let matched_indexes = match v {
                                        Some(idx) => vec![**idx],
                                        None => vec![],
                                    };
                                    data = self.select_data(&sq.projection, &matched_indexes);
                                }
                                Err(e) => {
                                    println!(
                                        "Error while trying to retrieve value from index: {e}"
                                    );
                                }
                            },
                            Binary::Gt
                            | Binary::Lt
                            | Binary::LtEq
                            | Binary::GtEq
                            | Binary::NotEq => {
                                data = self.execute_select(sq, where_expr, bop, col);
                            }
                        },
                    }
                } else {
                    println!("Executing select expression without index");
                    data = self.execute_select_query_without_index(sq);
                }
            }
            None => {
                println!("In none block");
                for col in &sq.projection {
                    let row = self.rows.get(col).unwrap();
                    let column = row.get_serialized_col_data();
                    data.push(column);
                }
            }
        }

        let rotated_data = Self::rotate_2d_vec(&data);
        Self::pretty_print(&rotated_data, &sq.projection);
    }

    pub fn print_table(&self) {
        let mut table = PTable::new();
        table.add_row(row!["Column Name", "Data Type"]);

        for col in &self.columns {
            table.add_row(row![col.name, col.datatype]);
        }

        table.printstd();
    }

    pub fn print_table_data(&self) {
        let mut p_table = PTable::new();

        let cnames = self
            .columns
            .iter()
            .map(|col| col.name.to_string())
            .collect::<Vec<String>>();

        let header_row = Row::new(
            cnames
                .iter()
                .map(|col| Cell::new(col))
                .collect::<Vec<Cell>>(),
        );

        let first_col_data = self.rows.get(&self.columns.first().unwrap().name).unwrap();
        let num_rows = first_col_data.count();
        let mut print_table_rows: Vec<Row> = vec![Row::new(vec![]); num_rows];

        for col_name in &cnames {
            let col_val = self
                .rows
                .get(col_name)
                .expect("Can't find any rows with the given column");
            let columns: Vec<String> = col_val.get_serialized_col_data();

            for i in 0..num_rows {
                print_table_rows[i].add_cell(Cell::new(&columns[i]));
            }
        }

        p_table.add_row(header_row);
        for row in print_table_rows {
            p_table.add_row(row);
        }

        p_table.printstd();
    }

    pub fn column_exists(&self, column: String) -> bool {
        self.columns.iter().any(|col| col.name == column)
    }

    fn rotate_2d_vec(data: &[Vec<String>]) -> Vec<Vec<&String>> {
        match data.first() {
            None => vec![vec![]],
            _ => {
                let number_of_rows = data.first().unwrap().len();
                let number_of_cols = data.len();
                let mut ret_data: Vec<Vec<&String>> = vec![vec![]; number_of_rows];

                (0..number_of_rows).for_each(|row_idx| {
                    (0..number_of_cols).for_each(|col_idx| {
                        ret_data[row_idx].push(&data[col_idx][row_idx]);
                    });
                });

                ret_data
            }
        }
    }

    fn pretty_print(data: &[Vec<&String>], header: &[String]) {
        let mut p_table = PTable::new();

        p_table.add_row(Row::new(
            header.iter().map(|h| Cell::new(h)).collect::<Vec<Cell>>(),
        ));

        for row in data {
            p_table.add_row(Row::new(
                row.iter().map(|c| Cell::new(c)).collect::<Vec<Cell>>(),
            ));
        }
        p_table.printstd();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{ast::Statement, dialect::MySqlDialect, parser::Parser};
    #[test]
    fn tests_creating_a_table() {
        let command =
            String::from("CREATE TABLE users (id int, name string, bounty float, unknown unknown)");
        let dialect = MySqlDialect {};
        let statement = &Parser::parse_sql(&dialect, &command).unwrap()[0];
        if let Statement::CreateTable { .. } = statement {
            let cq = CreateQuery::new(statement).unwrap();
            let table = Table::new(cq);
            let expected_column_names = vec![
                "id".to_string(),
                "name".to_string(),
                "bounty".to_string(),
                "unknown".to_string(),
            ];
            let expected_column_types = vec![
                "Int".to_string(),
                "Str".to_string(),
                "Float".to_string(),
                "Invalid".to_string(),
            ];

            let column_names = table
                .columns
                .iter()
                .map(|c| c.name.to_string())
                .collect::<Vec<String>>();

            let column_types = table
                .columns
                .iter()
                .map(|c| c.datatype.to_string())
                .collect::<Vec<String>>();

            assert_eq!(table.name, "users");
            assert_eq!(column_names, expected_column_names);
            assert_eq!(column_types, expected_column_types);
        }
    }

    #[test]
    fn tests_unique_constraint_violation_on_primary_key() {
        let command = String::from("CREATE TABLE users (id int PRIMARY KEY, name string)");

        let dialect = MySqlDialect {};
        let statement = &Parser::parse_sql(&dialect, &command).unwrap()[0];
        if let Statement::CreateTable { .. } = statement {
            let cq = CreateQuery::new(statement).unwrap();
            let mut table = Table::new(cq);
            let cols = vec!["id".to_string(), "name".to_string()];
            let val = vec!["1".to_string(), "tahmid".to_string()];
            table.does_violate_unique_constraint(&cols, &val).unwrap();
            table.insert_row(&cols, &[val.clone()]);
            assert!(table.does_violate_unique_constraint(&cols, &val).is_err());
        }
    }
}
