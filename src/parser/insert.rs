use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value, Values};

pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<String>>,
}

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
            match &**source {
                Query { body, .. } => {
                    if let SetExpr::Values(values) = body.as_ref() {
                        if let Values { rows, .. } = values {
                            for i in rows {
                                let mut value_set: Vec<String> = vec![];
                                for e in i {
                                    match e {
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
