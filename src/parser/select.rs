use sqlparser::ast::{
    BinaryOperator, Expr, Ident,
    SelectItem::{ExprWithAlias, QualifiedWildcard, UnnamedExpr, Wildcard},
    SetExpr, Statement, TableFactor, Value,
};

#[derive(Debug, PartialEq)]
pub enum Binary {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug)]
pub enum Operator {
    Binary(Binary),
}

// Only binary operators for now
#[derive(Debug)]
pub struct Expression {
    pub left: String,
    pub right: String,
    pub op: Operator,
}

#[derive(Debug)]
pub struct SelectQuery {
    pub from: String,
    pub projection: Vec<String>,
    pub where_expressions: Vec<Expression>,
}

impl SelectQuery {
    fn add_ops(
        op: &BinaryOperator,
        where_expressions: &mut Vec<Expression>,
        col_name: &Ident,
        n: &String,
    ) {
        match op {
            sqlparser::ast::BinaryOperator::Eq => {
                where_expressions.push(Expression {
                    left: col_name.to_string(),
                    right: n.to_string(),
                    op: Operator::Binary(Binary::Eq),
                });
            }
            sqlparser::ast::BinaryOperator::Gt => {
                where_expressions.push(Expression {
                    left: col_name.to_string(),
                    right: n.to_string(),
                    op: Operator::Binary(Binary::Gt),
                });
            }
            sqlparser::ast::BinaryOperator::Lt => {
                where_expressions.push(Expression {
                    left: col_name.to_string(),
                    right: n.to_string(),
                    op: Operator::Binary(Binary::Lt),
                });
            }
            sqlparser::ast::BinaryOperator::LtEq => {
                where_expressions.push(Expression {
                    left: col_name.to_string(),
                    right: n.to_string(),
                    op: Operator::Binary(Binary::LtEq),
                });
            }
            sqlparser::ast::BinaryOperator::GtEq => {
                where_expressions.push(Expression {
                    left: col_name.to_string(),
                    right: n.to_string(),
                    op: Operator::Binary(Binary::GtEq),
                });
            }
            _ => {
                panic!("cannot parse select query");
            }
        };
    }

    pub fn new(statement: &Statement) -> Result<SelectQuery, String> {
        let mut table_name: Option<String> = None;
        let mut projection: Vec<String> = vec![];
        let mut where_expressions: Vec<Expression> = vec![];

        match statement {
            Statement::Query(bq) => match &*(bq).body {
                SetExpr::Select(select) => {
                    for p in &(select).projection {
                        match p {
                            UnnamedExpr(exp) => match exp {
                                Expr::Identifier(i) => {
                                    projection.push(i.to_string());
                                }
                                _ => {
                                    println!(
                                        "Failing to parse expression in the where clause.\
                                         It's probably not an identifier or a value.\
                                         Cannot parse nested expressions :( ."
                                    );
                                }
                            },
                            QualifiedWildcard(obj_name, _) => {
                                println!("Found qualified wildcard in the expression. Wildcard name is  {}", obj_name);
                            }
                            Wildcard(_) => {
                                projection.push("*".to_string());
                            }
                            ExprWithAlias { expr, .. } => match expr {
                                Expr::Identifier(i) => {
                                    projection.push(i.to_string());
                                }
                                _ => {
                                    println!("Detected expression with alias. Cannot parse expression with alias.");
                                }
                            },
                        }
                    }

                    for f in &select.from {
                        match &f.relation {
                            TableFactor::Table { name, alias, .. } => {
                                table_name = Some(name.to_string());
                                match alias {
                                    Some(alias) => println!("alias = {}", alias),
                                    None => println!("No table alias"),
                                }
                            }
                            _ => println!("Nested join or derived tables"),
                        }
                    }

                    match &select.selection {
                        Some(where_expression) => {
                            println!("where expression in select.rs = {:?}", where_expression);
                            if let Expr::BinaryOp { left, op, right } = where_expression {
                                if let Expr::Identifier(col_name) = &(**left) {
                                    if let Expr::Value(v) = &(**right) {
                                        if let Value::Number(n, _) = v {
                                            Self::add_ops(op, &mut where_expressions, col_name, n);
                                        }
                                        if let Value::NationalStringLiteral(n) = v {
                                            Self::add_ops(op, &mut where_expressions, col_name, n);
                                        }

                                        if let Value::SingleQuotedString(n) = v {
                                            Self::add_ops(op, &mut where_expressions, col_name, n);
                                        }
                                    }

                                    if let Expr::Identifier(v) = &(**right) {
                                        let n = &v.to_string();
                                        Self::add_ops(op, &mut where_expressions, col_name, n);
                                    }
                                };
                            }
                        }
                        None => {}
                    }
                }
                _ => {
                    println!("don't care");
                }
            },
            _ => {
                println!("don't care");
            }
        }

        match table_name {
            Some(name) => Ok(SelectQuery {
                from: name,
                projection,
                where_expressions,
            }),
            None => Err(
                "Error while trying to parse select statement. Cannot extract table name"
                    .to_string(),
            ),
        }
    }

    pub fn insert_projections(&mut self, projection: Vec<String>) -> &mut SelectQuery {
        self.projection = projection;
        self
    }
}
