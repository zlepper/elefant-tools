use std::collections::HashMap;

fn main() {
    println!("Hello, world!");
}



pub struct GenerateRoot {
    /// A seed to ensure the exact same values are generated each run. 
    /// If not specified a random seed will be generated
    pub seed: Option<String>,
    /// How much to scale the number of rows to generate by. 
    pub count_scale: Option<u32>,
    
    /// Specific configuration for each table to generate data for
    pub tables: HashMap<String, TableConfiguration>
}

pub struct TableConfiguration {
    /// How to generate rows and how many
    pub rows: RowGenerationTarget,
    
    /// How each column in the table should be generated
    pub columns: HashMap<String, ColumnConfiguration>
}

pub enum RowGenerationTarget {
    /// Generate enough rows that the table will have this many rows when done. 
    /// This will _not_ remove rows, only add them.
    FinalCount(u64),
    /// Generate this many new rows, irrespective of how many rows are already 
    /// in the table. 
    NewRows(u64)
}

pub enum ColumnConfiguration {
    /// Always set the column value to `null`
    SetNull,
    
    /// Use the default value for the column. For example for `auto increment` columns
    /// Postgres will generate a value as a normal column insert.
    UseDefaultValue,
    
    /// Pick values at random from the given list of values. You can skew the odds by repeating 
    /// a value multiple times.
    PickRandom(Vec<String>),
    
    /// Generate a skewed random set of values
    RandomSkew {
        
        /// The values to pick between
        values: Vec<String>,
        
        /// How many times a value can appear at minimum. If set to less than 0, 
        /// a values might not ever appear. 
        min_skew: i32,
        
        /// How many times a values should appear at most
        max_skew: i32
    },
    
    /// Values can be picked at random from another table in the system.
    RandomRelation {
        /// The name of the table to grab values from
        from_table: String,
        
        /// The name of the column to grab values from
        from_column: String,
    }
    
}












