# Query Method Refactoring Plan: Binary vs Simple Mode Control

## Project Objective

Refactor the elefant-client query API to provide explicit control over PostgreSQL query modes with improved type safety:
- **`query_simple(&str)`** method that always uses text mode (PostgreSQL simple query protocol)
- **`query(Statement)`** method that always uses binary mode (always prepare statements, extended query protocol)
- **Split result types**: `SimpleQueryResult` (text mode) and `QueryResult` (binary mode) for compile-time format guarantees
- **Split FromSql traits**: `FromSqlBinary` and `FromSqlText` for compile-time type/format compatibility
- Update all tests to cover both binary and simple/text mode scenarios

## Current State Analysis

### Current Query Behavior
- **No parameters** → Simple query protocol → Text mode results
- **Has parameters** → Extended protocol (Parse/Bind/Execute) → Binary mode results
- **Format decision**: Automatic based on parameter presence
- **No user control**: Over binary vs text mode selection

### Current API Surface
```rust
// Current unified approach (format depends on parameters)
impl Statement for str {
    fn send<'postgres_client, C: ElefantAsyncReadWrite>(
        &self,
        client: &'postgres_client mut PostgresClient<C>,
        parameters: &[&(dyn ToSql)],
    ) -> impl Future<Output = Result<QueryResult<'postgres_client, C>, ElefantClientError>>;
}

// Current convenience methods
impl PostgresClient<C> {
    pub async fn read_single_value<T: FromSql<'_>>(&mut self, query: &str, parameters: &[&(dyn ToSql)]) -> Result<T, ElefantClientError>
    pub async fn execute_non_query(&mut self, query: &str, parameters: &[&(dyn ToSql)]) -> Result<(), ElefantClientError>
}
```

### Target API Design
```rust
// Simplified Statement trait - just prepares
pub trait Statement {
    fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> impl Future<Output = Result<PreparedQuery, ElefantClientError>>;
}

// New client API with type safety
impl PostgresClient<C> {
    // Simple mode - only accepts &str, returns text-mode result
    pub async fn query_simple(&mut self, query: &str) -> Result<SimpleQueryResult<'_, C>, ElefantClientError>
    
    // Binary mode - accepts any Statement, returns binary-mode result  
    pub async fn query<S: Statement>(&mut self, statement: S, parameters: &[&(dyn ToSql)]) -> Result<QueryResult<'_, C>, ElefantClientError>
}

// Split FromSql traits for compile-time format safety
pub trait FromSqlBinary<'a> { /* binary format only */ }
pub trait FromSqlText<'a> { /* text format only */ }

// Type-safe result extraction
impl SimpleQueryResult<'_, C> {
    pub fn get<T: FromSqlText<'_>>(&self, index: usize) -> Result<T, ElefantClientError>
}

impl QueryResult<'_, C> {
    pub fn get<T: FromSqlBinary<'_>>(&self, index: usize) -> Result<T, ElefantClientError>
}
```

### Key Files Requiring Changes
1. **`postgres_client/statements.rs`** - Simplified Statement trait (just prepare)
2. **`postgres_client/query.rs`** - Split result types and new FromSql traits
3. **`postgres_client/easy_client.rs`** - New client API methods
4. **`src/types/from_sql_row.rs`** - Split FromSql trait definitions
5. **All type modules** in `src/types/` - Implement both FromSqlBinary and FromSqlText
6. **All type tests** in `src/types/` - Test both binary and simple modes

## Implementation Plan

### Phase 1: Split FromSql Traits & Core Type System Changes

#### 1.1 Create New FromSql Traits (`src/types/from_sql_row.rs`)

**Current State**: Single `FromSql` trait with runtime format detection

**Target State**: Split traits for compile-time format safety

```rust
// Shared base trait for common functionality
pub trait FromSqlBase<'a>: Sized {
    fn from_sql_null() -> Result<Self, Box<dyn Error + Sync + Send>> {
        Err("Cannot convert NULL to this type".into())
    }
    fn accepts_postgres_type(oid: i32) -> bool;
}

// Binary format trait
pub trait FromSqlBinary<'a>: FromSqlBase<'a> {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>>;
}

// Text format trait  
pub trait FromSqlText<'a>: FromSqlBase<'a> {
    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>>;
}

// Compatibility: Keep old FromSql trait as combination of both (for gradual migration)
pub trait FromSql<'a>: FromSqlBinary<'a> + FromSqlText<'a> {}
impl<'a, T: FromSqlBinary<'a> + FromSqlText<'a>> FromSql<'a> for T {}
```

**Key Benefits**:
- **Shared functionality**: `FromSqlBase` eliminates code duplication for `from_sql_null()` and `accepts_postgres_type()`
- **Compile-time safety**: `&[u8]` can implement only `FromSqlBinary`, preventing text mode usage
- **Clear intent**: Separate traits make format requirements explicit
- **Backward compatibility**: Old `FromSql` trait still works during migration

### Phase 2: Split Result Types & Query Execution (`query.rs`)

#### 2.1 Create Separate Result Types with Shared Implementation

**Current State**: Single `QueryResult` with runtime format detection

**Target State**: Separate result types for compile-time format safety, maximizing code reuse

```rust
// Shared base struct containing common functionality
pub(crate) struct QueryResultBase<'postgres_client, C> {
    client: &'postgres_client mut PostgresClient<C>,
    // Common fields and methods shared between both result types
}

// Simple query result - text mode only
pub struct SimpleQueryResult<'postgres_client, C> {
    base: QueryResultBase<'postgres_client, C>,
    // No prepared_query_result since simple queries don't prepare
}

// Prepared query result - binary mode only  
pub struct QueryResult<'postgres_client, C> {
    base: QueryResultBase<'postgres_client, C>,
    prepared_query_result: Rc<PreparedQueryResult>,
}

impl<'postgres_client, C> QueryResultBase<'postgres_client, C> {
    // Shared methods that work for both simple and prepared queries
    pub fn next_result_set(&mut self) -> impl Future<Output = Result<QueryResultSet<'postgres_client, '_, C>, ElefantClientError>> {
        // Common result set iteration logic
    }
    
    pub fn collect_to_vec<T>(&mut self) -> impl Future<Output = Result<Vec<T>, ElefantClientError>> {
        // Common collection logic (delegates to get() method)
    }
    
    // Other shared methods: collect_single_column_to_vec, etc.
}

impl<'postgres_client, C> SimpleQueryResult<'postgres_client, C> {
    pub(crate) fn new(client: &'postgres_client mut PostgresClient<C>) -> Self {
        Self { 
            base: QueryResultBase::new(client),
        }
    }
    
    // Only accepts types that implement FromSqlText
    pub fn get<T: FromSqlText<'postgres_client>>(&self, index: usize) -> Result<T, ElefantClientError> {
        let field = &self.current_row_description().fields[index];
        let column_data = self.current_data_row().get_column_data(index)?;
        
        match column_data {
            Some(data) => {
                // Always text format - no runtime check needed
                let text_data = std::str::from_utf8(data)
                    .map_err(|e| ElefantClientError::new(format!("Invalid UTF-8 in text data: {}", e)))?;
                T::from_sql_text(text_data, field)
            }
            None => T::from_sql_null(),
        }
        .map_err(|e| ElefantClientError::new(format!("Failed to convert column {}: {}", index, e)))
    }
    
    // Delegate shared methods to base
    pub fn next_result_set(&mut self) -> impl Future<Output = Result<SimpleQueryResultSet<'postgres_client, '_, C>, ElefantClientError>> {
        self.base.next_result_set()
    }
    
    pub fn collect_to_vec<T: FromSqlText<'postgres_client>>(&mut self) -> impl Future<Output = Result<Vec<T>, ElefantClientError>> {
        self.base.collect_to_vec()
    }
}

impl<'postgres_client, C> QueryResult<'postgres_client, C> {
    pub(crate) fn new(client: &'postgres_client mut PostgresClient<C>, prepared_query_result: Rc<PreparedQueryResult>) -> Self {
        Self { 
            base: QueryResultBase::new(client),
            prepared_query_result,
        }
    }
    
    // Only accepts types that implement FromSqlBinary
    pub fn get<T: FromSqlBinary<'postgres_client>>(&self, index: usize) -> Result<T, ElefantClientError> {
        let field = &self.current_row_description().fields[index];
        let column_data = self.current_data_row().get_column_data(index)?;
        
        match column_data {
            Some(data) => {
                // Always binary format - no runtime check needed
                T::from_sql_binary(data, field)
            }
            None => T::from_sql_null(),
        }
        .map_err(|e| ElefantClientError::new(format!("Failed to convert column {}: {}", index, e)))
    }
    
    // Delegate shared methods to base
    pub fn next_result_set(&mut self) -> impl Future<Output = Result<QueryResultSet<'postgres_client, '_, C>, ElefantClientError>> {
        self.base.next_result_set()
    }
    
    pub fn collect_to_vec<T: FromSqlBinary<'postgres_client>>(&mut self) -> impl Future<Output = Result<Vec<T>, ElefantClientError>> {
        self.base.collect_to_vec()
    }
}
```

### Phase 3: Simplified Statement Trait & Client API (`statements.rs`, `easy_client.rs`)

#### 3.1 Simplify Statement Trait (`statements.rs`)

**Current State**: Statement trait handles both preparation and execution

**Target State**: Statement trait only handles preparation

```rust
pub trait Statement: Sealed {
    fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> impl Future<Output = Result<PreparedQuery, ElefantClientError>>;
}

impl Statement for str {
    async fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        client.prepare_statement(self).await
    }
}

impl Statement for String {
    async fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        self.as_str().prepare(client).await
    }
}

// PreparedQuery doesn't need to implement Statement anymore,
// or it can just return itself
impl Statement for PreparedQuery {
    async fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        _client: &mut PostgresClient<C>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        Ok(self.clone()) // Already prepared
    }
}
```

#### 3.2 New Client API (`easy_client.rs`)

**Target State**: Type-safe client methods with compile-time format guarantees

```rust
impl<C: ElefantAsyncReadWrite> PostgresClient<C> {
    // Simple mode - only accepts &str, returns SimpleQueryResult
    pub async fn query_simple(&mut self, query: &str) -> Result<SimpleQueryResult<'_, C>, ElefantClientError> {
        // Send Query message directly (simple protocol)
        self.send_frontend_message(&FrontendMessage::Query {
            query: query.to_string(),
        }).await?;
        self.flush().await?; // Ensure message is sent
        
        Ok(SimpleQueryResult::new(self))
    }
    
    // Binary mode - accepts any Statement, returns QueryResult
    pub async fn query<S: Statement>(&mut self, statement: S, parameters: &[&(dyn ToSql)]) -> Result<QueryResult<'_, C>, ElefantClientError> {
        let prepared = statement.prepare(self).await?;
        prepared.execute(self, parameters).await
    }
    
    // Convenience methods for simple mode (text format)
    pub async fn read_single_value_simple<T: FromSqlText<'_>>(&mut self, query: &str) -> Result<T, ElefantClientError> {
        let mut result = self.query_simple(query).await?;
        let mut result_set = result.next_result_set().await?;
        
        match result_set {
            SimpleQueryResultSet::RowDescriptionReceived(mut row_reader) => {
                if let Some(row) = row_reader.next().await? {
                    row.get(0)
                } else {
                    Err(ElefantClientError::new("No rows returned"))
                }
            }
            SimpleQueryResultSet::QueryProcessingComplete => {
                Err(ElefantClientError::new("Query returned no result set"))
            }
        }
    }
    
    pub async fn execute_non_query_simple(&mut self, query: &str) -> Result<(), ElefantClientError> {
        let mut result = self.query_simple(query).await?;
        // Process all result sets to completion
        while let SimpleQueryResultSet::RowDescriptionReceived(mut row_reader) = result.next_result_set().await? {
            while row_reader.next().await?.is_some() {
                // Consume all rows
            }
        }
        Ok(())
    }
    
    // Existing convenience methods for binary mode (now always prepare)
    pub async fn read_single_value<T: FromSqlBinary<'_>>(&mut self, query: &str, parameters: &[&(dyn ToSql)]) -> Result<T, ElefantClientError> {
        let mut result = self.query(query, parameters).await?;
        // Existing single value extraction logic
    }
    
    pub async fn execute_non_query(&mut self, query: &str, parameters: &[&(dyn ToSql)]) -> Result<(), ElefantClientError> {
        let mut result = self.query(query, parameters).await?;
        // Existing non-query processing logic
    }
}
```

**Key Design Benefits**:
- **Compile-time safety**: Simple mode only accepts `&str`, binary mode accepts any `Statement`
- **Direct protocol access**: Simple mode bypasses Statement trait completely  
- **Clean separation**: Statement trait focused on preparation, client handles execution
- **Type system enforcement**: Impossible to pass PreparedQuery to simple mode

### Phase 4: Update All Type Implementations

#### 4.1 Implement Both Traits for All Types

**Current State**: All types implement single `FromSql` trait

**Target State**: All types implement both `FromSqlBinary` and `FromSqlText` where possible

**Implementation Pattern**:
```rust
// Example: i32 implementation with shared base
impl<'a> FromSqlBase<'a> for i32 {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::INTEGER.oid
    }
    // from_sql_null() uses default implementation from trait
}

impl<'a> FromSqlBinary<'a> for i32 {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 4 {
            return Err(format!("Expected 4 bytes for i32, got {}", raw.len()).into());
        }
        Ok(i32::from_be_bytes(raw.try_into().unwrap()))
    }
}

impl<'a> FromSqlText<'a> for i32 {
    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        raw.parse::<i32>().map_err(|e| format!("Failed to parse i32: {}", e).into())
    }
}

// BYTEA: Only supports binary mode (compile-time safety)
impl<'a> FromSqlBase<'a> for &'a [u8] {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::BYTEA.oid
    }
}

impl<'a> FromSqlBinary<'a> for &'a [u8] {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw) // Zero-copy borrowing
    }
}

// No FromSqlText implementation for &[u8] - compile-time error for simple mode

impl<'a> FromSqlBase<'a> for Vec<u8> {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::BYTEA.oid
    }
}

impl<'a> FromSqlBinary<'a> for Vec<u8> {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.to_vec())
    }
}

impl<'a> FromSqlText<'a> for Vec<u8> {
    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Parse PostgreSQL hex format: \x followed by hex digits
        if !raw.starts_with("\\x") {
            return Err("BYTEA text format must start with \\x".into());
        }
        
        let hex_str = &raw[2..];
        let mut result = Vec::with_capacity(hex_str.len() / 2);
        
        for chunk in hex_str.as_bytes().chunks(2) {
            if chunk.len() != 2 {
                return Err("Invalid hex encoding in BYTEA".into());
            }
            let hex_byte = std::str::from_utf8(chunk)
                .map_err(|_| "Invalid UTF-8 in hex encoding")?;
            let byte = u8::from_str_radix(hex_byte, 16)
                .map_err(|_| "Invalid hex digit in BYTEA")?;
            result.push(byte);
        }
        
        Ok(result)
    }
}
```

### Phase 5: Comprehensive Test Coverage

#### 5.1 Test Strategy Overview

**Goal**: Ensure all PostgreSQL types work correctly in both binary and simple/text modes with compile-time safety

**Test Pattern Template**:
```rust
#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::test_helpers::{get_postgres_client, TEST_DB_HOST, TEST_DB_PORT};

    // Binary mode tests (existing, but may need updates)
    #[tokio::test]
    async fn test_typename_binary_mode() {
        let mut client = get_postgres_client().await;
        
        // Test with parameter binding (forces binary mode)
        let result: TypeName = client
            .query("SELECT $1::typename", &[&test_value])
            .await?
            .next_result_set().await?
            .unwrap_single_row()?
            .get(0)?;
        
        assert_eq!(result, expected_value);
    }
    
    // NEW: Simple mode tests (text format) - only for types that implement FromSqlText
    #[tokio::test]
    async fn test_typename_simple_mode() {
        let mut client = get_postgres_client().await;
        
        // Test with simple query (forces text mode)
        let result: TypeName = client
            .query_simple("SELECT 'literal_value'::typename")
            .await?
            .next_result_set().await?
            .unwrap_single_row()?
            .get(0)?;
        
        assert_eq!(result, expected_value);
    }
    
    // NEW: Compile-time safety tests
    #[tokio::test] 
    async fn test_compile_time_safety() {
        let mut client = get_postgres_client().await;
        
        // This should NOT compile for types that don't implement FromSqlText:
        // let result: &[u8] = client.query_simple("SELECT 'test'::bytea").await?.get(0)?;
        
        // This SHOULD compile:
        let result: Vec<u8> = client.query_simple("SELECT '\\x48656c6c6f'::bytea").await?.next_result_set().await?.unwrap_single_row()?.get(0)?;
        assert_eq!(result, b"Hello");
    }
}
```

#### 5.2 Special Cases & Type-Specific Considerations

**BYTEA Type**: 
- `&[u8]`: Only `FromSqlBinary` (compile-time error for simple mode)
- `Vec<u8>`: Both `FromSqlBinary` and `FromSqlText` (hex parsing)

**JSON/JSONB Types**:
- Test wrapper types work in both modes
- Verify format-specific serialization behavior

**Array Types**:
- Ensure array parsing works correctly in text mode
- Test comma conflict resolution for complex types (POINT arrays)

**NULL Handling**:
- Test `Option<T>` for both binary and simple modes
- Verify NULL conversion works consistently


## Success Criteria

### Functional Requirements
- [ ] **`query_simple(&str)`**: Always uses PostgreSQL simple query protocol (text mode)
- [ ] **`query(Statement, params)`**: Always uses PostgreSQL extended query protocol (binary mode)  
- [ ] **Split result types**: `SimpleQueryResult` and `QueryResult` with format-specific type constraints
- [ ] **Compile-time safety**: `&[u8]` cannot be used with simple mode, `Vec<u8>` works with both
- [ ] **All compatible types work**: Every type that can support both formats implements both traits

### Type Safety Requirements
- [ ] **Compile-time format checking**: No runtime `ValueFormat` checks needed  
- [ ] **Clear error messages**: Compilation fails with helpful messages for incompatible type/format combinations
- [ ] **Zero runtime overhead**: Format selection happens at compile time

### Test Coverage Requirements  
- [ ] **100% type coverage**: All types tested in both binary and simple modes where supported
- [ ] **Compile-time safety verification**: Tests demonstrate compile-time errors for invalid combinations
- [ ] **Consistency tests**: Verify same logical values produce same results in both modes
- [ ] **Edge case coverage**: NULL handling, arrays, special values work in both modes

This improved design leverages the type system to provide compile-time guarantees about format compatibility while maintaining a clean, intuitive API.