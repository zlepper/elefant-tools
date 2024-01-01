#[derive(Debug, Eq, PartialEq)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
}

impl PostgresColumn {
    pub fn get_simplified_data_type(&self) -> SimplifiedDataType {
        match self.data_type.as_str() {
            "bigint"|"integer"|"smallint"|"real"|"double precision" => SimplifiedDataType::Number,
            "boolean" => SimplifiedDataType::Bool,
            _ => SimplifiedDataType::Text,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SimplifiedDataType {
    Number,
    Text,
    Bool,
}
