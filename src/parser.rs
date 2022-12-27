use sqlparser::ast::Statement;

pub mod create;
pub mod insert;
pub mod select;

pub trait QueryType<T> {
    fn query(statement: &Statement) -> Result<T, String>;
}
