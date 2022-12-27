use sqlparser::ast::{ColumnOption, DataType, Statement};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct ParsedColumn {
    pub name: String,
    pub datatype: String,
    pub is_pk: bool,
    pub is_nullable: bool,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct CreateQuery {
    pub table_name: String,
    pub columns: Vec<ParsedColumn>,
}

impl CreateQuery {
    pub fn new(statement: &Statement) -> Result<CreateQuery, String> {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name;
                let mut parsed_columns: Vec<ParsedColumn> = vec![];

                for col in columns {
                    let name = col.name.to_string();
                    let datatype = match &col.data_type {
                        DataType::SmallInt(_) | DataType::Int(_) | DataType::BigInt(_) => "int",
                        DataType::Boolean => "bool",
                        DataType::Text | DataType::Varchar(_) | DataType::String => "string",
                        DataType::Float(_)
                        | DataType::Double
                        | DataType::DoublePrecision
                        | DataType::Decimal(_) => "float",
                        _ => {
                            println!("could not match type");
                            "invalid"
                        }
                    };

                    let mut is_pk: bool = false;
                    for column_option in &col.options {
                        if let ColumnOption::Unique { is_primary } = column_option.option {
                            is_pk = is_primary;
                        }
                    }

                    parsed_columns.push(ParsedColumn {
                        name,
                        datatype: datatype.to_string(),
                        is_pk,
                        is_nullable: false,
                    });
                }

                Ok(CreateQuery {
                    table_name: table_name.to_string(),
                    columns: parsed_columns,
                })
            }
            _ => Err("Error parsing query".to_string()),
        }
    }
}
