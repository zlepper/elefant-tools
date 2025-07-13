# Agent Knowledge - Lessons Learned

## Rust Async Runtime Integration Patterns

### Custom Trait Abstraction for Multiple Runtimes
**Lesson**: When supporting multiple async runtimes (tokio, monoio, etc.), create a unified trait rather than trying to make existing traits work across different systems.

**What Worked**:
- Created single `ElefantAsyncReadWrite` trait with methods: `read()`, `write_all()`, `flush()`
- Used transparent wrapper types (`TokioWrapper<T>`, `MonoioWrapper<T>`) in runtime-specific modules
- Wrapper types implement the unified trait and handle runtime-specific differences
- All core business logic uses the unified trait, keeping it runtime-agnostic

**Key Benefits**:
- Zero runtime overhead (wrappers compile away)
- Clean separation of concerns
- Easy to add new runtimes
- Single codebase supports multiple backends

### Monoio-Specific Considerations
**Buffer Ownership**: Monoio uses `AsyncReadRent`/`AsyncWriteRent` which require owned buffers, unlike futures' borrowed slice approach.

**Solution Pattern**:
```rust
// Convert slice to owned Vec for monoio
let owned_buf = buf.to_vec();
let (result, _) = self.0.write_all(owned_buf).await;
result.map(|_| ()) // Map usize to ()
```

### Systematic Trait Migration Strategy
**Process that worked well**:
1. Create custom trait first
2. Update core components (frame reader, connection types)
3. Update all client modules systematically
4. Use find/replace for consistent patterns like `impl<C: AsyncRead + AsyncWrite + Unpin>`
5. Clean up imports last

**Import Organization**:
- Add proper `use` statements rather than fully qualified names
- Remove unused imports to keep code clean
- Group related imports together

## Cargo Feature Management

### Feature Gate Best Practices
**Pattern**: Use feature gates to cleanly separate optional dependencies
```toml
[dependencies]
tokio = { version = "1", optional = true }
monoio = { version = "0.2", optional = true }

[features]
tokio = ["dep:tokio", "dep:tokio-util"]
monoio = ["dep:monoio"]
```

**Testing Strategy**: Always test with `--all-features` unless specifically testing individual feature combinations.

## Code Organization Lessons

### Wrapper Placement Strategy
**Best Practice**: Keep runtime-specific wrapper types in their respective connection modules rather than in a central location.
- Better organization and discoverability
- Easier to extend with new runtimes
- Clear ownership of runtime-specific concerns

### Error Handling Consistency
**Important**: When creating wrapper types, ensure error types remain consistent across different runtime implementations.
- Both wrappers should return the same error types (`std::io::Error`)
- Handle differences in return types internally (e.g., monoio returning `usize` vs expected `()`)

## Common Pitfalls Avoided

### Trait Bound Complexity
**Avoided**: Don't try to make existing traits work with conditional compilation. Create a unified interface instead.

### Import Cleanup Timing
**Learned**: Clean up imports and comments last, after functionality is working. Doing it too early can cause confusion during debugging.

### Feature Testing
**Critical**: Test both individual features AND `--all-features` to ensure no conflicts between different runtime dependencies.

## Future Implementation Guidance

### Adding New Async Runtimes
Based on this implementation, adding a new runtime would involve:
1. Add dependency with feature gate in `Cargo.toml`
2. Create `{runtime}_connection.rs` module
3. Implement `{Runtime}Wrapper<T>` that implements `ElefantAsyncReadWrite`
4. Define type aliases for the new runtime
5. No changes needed to core business logic

### Performance Considerations
- Wrapper pattern has zero runtime cost
- Buffer copying in monoio wrapper is necessary due to ownership model
- Consider benchmarking different runtimes for specific use cases

This pattern should work well for other similar integration challenges where multiple backend implementations need to be supported through a unified interface.

## PostgreSQL Type System Implementation Patterns

### Binary vs Text Format Considerations
**Key Insight**: PostgreSQL protocol supports both binary and text formats, and type implementations must handle the constraints of each format appropriately.

**Borrowing vs Owned Data with BYTEA**:
- **Vec<u8>**: Can handle both binary and text formats since it owns the data
  - Binary format: Direct copy from raw bytes
  - Text format: Parse hex encoding (\\x prefix) and create owned vector
- **&[u8]**: Only supports binary format due to borrowing constraints
  - Binary format: Direct borrowing from raw bytes (zero-copy)
  - Text format: Cannot create borrowed slice from parsed hex data - return error with clear message

**Parameter Binding Design**:
- Parameter binding always uses binary format via `ToSql` trait
- Both `&[u8]` and `Vec<u8>` work perfectly for parameter binding
- `Option<&[u8]>` parameter binding works through generic `Option<T>` implementation
- This allows efficient zero-copy parameter binding with `&[u8]` while using `Vec<u8>` for text query results

**Testing Strategy for Mixed Format Support**:
- Test both owned and borrowed types for parameter binding (binary format)
- Test owned types for text format queries (reading from database)
- Verify error messages are clear when borrowed types encounter text format
- Include NULL handling tests for both `Option<&[u8]>` and `Option<Vec<u8>>`

**Pattern for Similar Types**:
```rust
// Owned type - supports both formats
impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.to_vec()) // Own the data
    }
    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Parse text format and create owned data
    }
}

// Borrowed type - binary format only
impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw) // Zero-copy borrowing
    }
    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Err(format!("Cannot create &[u8] from text format. Use Vec<u8> instead").into())
    }
}
```

This design provides both efficiency (zero-copy with borrowed types) and flexibility (full format support with owned types).

## Code Organization and Refactoring Patterns

### Large Module Refactoring Strategy
**Problem**: Single large module (`core.rs` with 670+ lines) becoming unmaintainable and hard to navigate.

**Successful Refactoring Approach**:
1. **Analyze by Functionality**: Group related implementations by type category and cohesion
2. **One Module Per Type Category**: Each fundamental concept gets its own module
3. **Co-locate Tests**: Move tests to the same module as their implementations
4. **Preserve All Functionality**: Zero regressions during refactoring
5. **Clean Up Coordination Module**: Original file becomes lightweight documentation/re-export hub

**Module Organization Pattern**:
```
types/
├── core.rs          // Coordination + documentation (15 lines)
├── numbers.rs       // Numeric types + macro + tests (170 lines)
├── bool.rs          // Boolean type + tests (45 lines)  
├── char.rs          // Character type + tests (40 lines)
├── text.rs          // String types + tests (45 lines)
├── binary.rs        // Binary types + comprehensive tests (170 lines)
├── collections.rs   // Array types + tests (110 lines)
└── nullable.rs      // Option wrapper + tests (70 lines)
```

**Key Success Factors**:
- **Granular Consistency**: Every type category gets its own module (even single types like `bool`)
- **Test Co-location**: Tests stay with implementations for easier maintenance
- **Focused Scope**: Each module has 40-170 lines vs original 670+ line monolith
- **Clear Naming**: Module names directly reflect their responsibility
- **Documentation**: Coordination module explains the new structure

**Testing Strategy During Refactoring**:
- Build after each module creation to catch syntax errors early
- Run full test suite frequently to ensure no regressions
- Move tests incrementally to verify each module works in isolation
- Use `#![allow(dead_code)]` to suppress warnings during development

**Benefits Realized**:
- **Maintainability**: Easy to find and modify specific type implementations
- **Readability**: Focused modules vs overwhelming single file
- **Parallel Development**: Multiple developers can work on different type categories
- **Test Organization**: Tests directly related to implementations they verify

This pattern works well for any large implementation module with multiple distinct responsibilities that can be logically separated.

## PostgreSQL Date/Time Type Implementation Patterns

### PostgreSQL Epoch vs Unix Epoch
**Key Insight**: PostgreSQL uses a different epoch than Unix systems:
- **PostgreSQL Epoch**: 2000-01-01 00:00:00 UTC
- **Unix Epoch**: 1970-01-01 00:00:00 UTC

**Implementation Pattern**:
```rust
// Always create the PostgreSQL epoch explicitly
let pg_epoch = Date::from_calendar_date(2000, Month::January, 1)?;
let days_since_pg_epoch = i32::from_be_bytes(raw.try_into().unwrap());
let result_date = pg_epoch + time::Duration::days(days_since_pg_epoch as i64);
```

### Date/Time Format Parsing Challenges
**Problem**: PostgreSQL's text format differs from ISO 8601 and requires custom parsing.

**Solutions**:
- **DATE**: `YYYY-MM-DD` format, use `[year]-[month]-[day]`
- **TIME**: `HH:MM:SS` or `HH:MM:SS.ffffff`, conditional parsing based on decimal presence
- **TIMESTAMP**: `YYYY-MM-DD HH:MM:SS` or with microseconds, similar conditional approach
- **TIMESTAMPTZ**: `YYYY-MM-DD HH:MM:SS+TZ` where timezone is `+HH` format, not `+HHMM`

**Pattern for Conditional Parsing**:
```rust
let format = if raw.contains('.') {
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]")
} else {
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]")
};
```

### Array Element Quoting in PostgreSQL
**Discovery**: PostgreSQL quotes complex array elements (especially timestamps) in text format arrays.

**Solution**: Enhanced array parsing to strip quotes from elements:
```rust
let clean_item = if item.starts_with('"') && item.ends_with('"') && item.len() >= 2 {
    &item[1..item.len()-1]  // Strip quotes
} else {
    item
};
```

**Why This Matters**: Arrays of strings, timestamps, and other complex types are quoted, but simple types (numbers) are not.

### Timezone Handling Best Practices
**TIMESTAMPTZ Design**: PostgreSQL stores TIMESTAMPTZ values as UTC internally, regardless of input timezone.

**Implementation Strategy**:
- Accept `OffsetDateTime` with any timezone for input
- Convert to UTC before storing: `self.to_offset(time::UtcOffset::UTC)`
- Always return values as UTC from database
- Let application handle timezone display conversions

### Testing Strategy for Date/Time Types
**Comprehensive Coverage**:
- Test PostgreSQL epoch values (2000-01-01)
- Test current dates/times for real-world validation
- Test microsecond precision with exact values
- Test timezone conversions (especially EST to UTC examples)
- Test array handling with quoted elements
- Test NULL handling through `Option<T>` wrapper
- Test round-trip parameter binding to verify `ToSql` implementations

### Dependency Management for Date/Time
**time Crate Configuration**:
```toml
time = { version = "0.3", features = ["macros", "formatting", "parsing"] }
```
- `macros` feature: Required for `format_description!` macro and `time!`, `date!`, `datetime!` macros in tests
- `formatting` and `parsing` features: Required for custom format string parsing

This implementation successfully handles all PostgreSQL date/time types with proper epoch handling, timezone conversions, and format parsing.

### Performance Optimization with Static Format Descriptions
**Problem**: Runtime parsing of format descriptions on every value conversion was inefficient.

**Solution**: Use `std::sync::LazyLock` for static format descriptions:
```rust
static DATE_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[year]-[month]-[day]")
        .expect("DATE format description should be valid")
});
```

**Benefits**:
- Format descriptions parsed only once per static lifetime
- Zero runtime parsing overhead after first use
- Thread-safe lazy initialization
- Compile-time validation via `expect()` for malformed formats

### Feature Flag Best Practices for Optional Dependencies
**Pattern**: Make specialized type support completely optional:
```toml
# Main dependency - minimal features
time = { version = "0.3", features = ["formatting", "parsing"], optional = true }

# Dev dependency - includes macros for tests
[dev-dependencies]
time = { version = "0.3", features = ["macros"] }

# Feature flag
[features]
time = ["dep:time"]
```

**Module Organization**:
```rust
// In mod.rs - entire module behind feature flag
#[cfg(feature = "time")]
mod datetime;
```

**Key Benefits**:
- Downstream packages not forced to include time dependency
- Tests can still use convenient macros via dev-dependencies
- Clean separation of runtime vs development requirements
- Eliminates macro dependency from production builds

This approach balances performance, optional dependencies, and developer experience effectively.

## PostgreSQL Type Implementation Best Practices

### OID Management and Constants
**Problem**: Hardcoded OIDs in `accepts_postgres_type` functions create maintenance issues and magic numbers.

**Solution**: Always use centralized constants from `PostgresType` struct:
```rust
// Bad - hardcoded magic numbers
fn accepts_postgres_type(oid: i32) -> bool {
    oid == 114 // JSON OID - what is 114?
}

// Good - use centralized constants
use crate::types::PostgresType;

fn accepts_postgres_type(oid: i32) -> bool {
    oid == PostgresType::JSON.oid
}
```

**Benefits**:
- **Maintainability**: OIDs centralized in `standard_types.rs`
- **Self-documenting**: Constant names explain what the OID represents
- **Type safety**: Prevents typos in magic numbers
- **Consistency**: All type implementations follow same pattern

**Implementation Pattern**:
1. Import `PostgresType`: `use crate::types::PostgresType;`
2. Use constants: `PostgresType::JSON.oid`, `PostgresType::UUID.oid`, etc.
3. All OID definitions are in `standard_types.rs` for easy reference

### JSON Type Performance Optimizations
**Key Insight**: `serde_json` provides direct byte slice operations that avoid intermediate allocations.

**Optimized Implementation**:
```rust
// Reading: Direct from byte slice (no UTF-8 validation overhead)
fn from_sql_binary(raw: &[u8], field: &FieldDescription) -> Result<Value, Box<dyn Error + Sync + Send>> {
    serde_json::from_slice(raw).map_err(|e| format!("Failed to parse JSON: {}", e).into())
}

// Writing: Direct to byte buffer (no intermediate string allocation)
fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
    serde_json::to_writer(target_buffer, self).map_err(|e| format!("Failed to serialize JSON: {}", e).into())
}
```

**Performance Benefits**:
- **`from_slice`**: Eliminates UTF-8 validation pass and string allocation
- **`to_writer`**: Writes UTF-8 bytes directly to target buffer, no intermediate string
- **Memory efficiency**: Especially important for large JSON payloads
- **Network I/O bound**: JSON parsing performance is typically secondary to network, but optimization is still valuable

**Testing Strategy**:
- Use round-trip parameter binding to avoid SQL escaping complexity
- Test JSON-level escaping: quotes, backslashes, newlines, unicode, control characters
- Verify complex nested structures and arrays work correctly
- Include error handling tests for malformed JSON

This pattern applies to other text-based PostgreSQL types that could benefit from direct byte operations.

## PostgreSQL JSONB vs JSON Parameter Binding Challenge

### The Core Problem
**Issue**: PostgreSQL has two similar JSON types (JSON and JSONB) with different binary wire formats, but the `ToSql` trait doesn't provide context about the target PostgreSQL column type during parameter binding.

### Binary Format Differences
- **JSON**: Plain UTF-8 encoded JSON text 
- **JSONB**: Version byte (0x01) + UTF-8 encoded JSON text

### Failed Approach: Single Implementation
**What Didn't Work**: Trying to make `serde_json::Value` work for both types with a single `ToSql` implementation.

```rust
// This approach failed - couldn't determine target type at binding time
impl ToSql for Value {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<...> {
        // No way to know if target is JSON or JSONB column!
    }
}
```

**Results**:
- Sending JSONB format to JSON columns: "invalid input syntax for type json" error
- Sending JSON format to JSONB columns: "unsupported jsonb version number" error

### Successful Solution: Wrapper Types
**Pattern**: Create separate wrapper types for each PostgreSQL type that has distinct binary formats.

```rust
/// Wrapper for JSON columns - sends plain UTF-8
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub Value);

/// Wrapper for JSONB columns - sends version byte + UTF-8  
#[derive(Debug, Clone, PartialEq)]
pub struct Jsonb(pub Value);

impl ToSql for Json {
    fn to_sql_binary(&self, buf: &mut Vec<u8>) -> Result<...> {
        serde_json::to_writer(buf, &self.0) // Plain JSON
    }
}

impl ToSql for Jsonb {
    fn to_sql_binary(&self, buf: &mut Vec<u8>) -> Result<...> {
        buf.push(1); // Version byte
        serde_json::to_writer(buf, &self.0) // JSON after version
    }
}
```

### Implementation Strategy
1. **Shared Reading Logic**: Both types use the same `FromSql` implementation that handles format detection based on OID
2. **Type-Specific Writing**: Each wrapper type has its own `ToSql` implementation for the correct binary format
3. **Backward Compatibility**: Keep `serde_json::Value` implementation (defaults to JSON format)
4. **Clear Documentation**: Usage examples show when to use each wrapper type

### Key Benefits
- **Type Safety**: Compile-time guarantee of correct format for target column
- **Performance**: No runtime format detection or conversion overhead
- **Clear Intent**: Code explicitly shows whether JSON or JSONB is intended
- **Standard Pattern**: Follows PostgreSQL driver conventions (e.g., Rust `postgres` crate)

### Usage Pattern
```rust
// For JSON columns (explicit wrapper needed)
let json_value = Json(serde_json::json!({"key": "value"}));
client.execute("INSERT INTO json_table VALUES ($1)", &[&json_value]);

// For JSONB columns (can use Value directly - now defaults to JSONB)
let value = serde_json::json!({"key": "value"});
client.execute("INSERT INTO jsonb_table VALUES ($1)", &[&value]);

// Or be explicit with wrapper
let jsonb_value = Jsonb(serde_json::json!({"key": "value"}));
client.execute("INSERT INTO jsonb_table VALUES ($1)", &[&jsonb_value]);
```

### Best Practice Changes
**Default Behavior Updated**: `serde_json::Value` now defaults to JSONB format instead of JSON format. This provides:
- **Better Performance**: JSONB is more efficient for storage and querying
- **PostgreSQL Recommendation**: JSONB is the recommended JSON type in PostgreSQL
- **Future-Proof**: Most new applications should use JSONB over JSON

**Migration Strategy**: Existing code using `serde_json::Value` with JSON columns needs to be updated to use the explicit `Json()` wrapper.

### Future Application
This wrapper type pattern should be used for any PostgreSQL types that:
1. Share similar logical representation (e.g., both are JSON)
2. Have different binary wire formats
3. Cannot be distinguished at parameter binding time

Examples might include: different text encoding types, various numeric precision types, or geometry types with different internal representations.

## Performance Optimization with Direct API Access

### rust_decimal Direct Mantissa/Scale Operations
**Problem**: Initial NUMERIC implementation used expensive string conversions in both directions:
- **Reading**: Binary → string representation → parse to Decimal
- **Writing**: Decimal → string → parse digits → binary format

**Solution**: Use `rust_decimal`'s direct internal API access:
```rust
// Reading: Direct construction from mantissa and scale
let mantissa: i128 = /* build from PostgreSQL base-10000 digits */;
let scale: u32 = /* calculate from PostgreSQL format */;
Decimal::try_from_i128_with_scale(mantissa, scale)

// Writing: Direct access to internal representation  
let mantissa = self.mantissa(); // Returns i128
let scale = self.scale();       // Returns u32
let is_negative = self.is_sign_negative(); // Returns bool
```

**Performance Benefits**:
- **Zero string parsing overhead** during binary format conversion
- **Direct integer arithmetic** for base-10000 digit operations
- **Minimal string usage** only for digit extraction (unavoidable for base conversion)
- **Significant performance improvement** especially for high-precision decimals

**Key Methods Available**:
- `mantissa()` → `i128`: The raw integer value
- `scale()` → `u32`: Number of decimal places  
- `is_sign_negative()` → `bool`: Sign information
- `is_zero()` → `bool`: Efficient zero check
- `try_from_i128_with_scale(mantissa, scale)`: Direct construction

**Testing Strategy**: All existing tests pass with the optimized implementation, ensuring correctness while gaining performance.

This pattern applies to any decimal/numeric type where direct access to internal representation is available over string conversions.

## PostgreSQL Array Parsing with Delimiter Conflicts

### The Problem: Comma Conflicts in Complex Types
**Issue**: PostgreSQL uses comma as the default array delimiter, but some types (like POINT) also contain commas internally in their text representation.

**Example Failure**:
- PostgreSQL sends: `{"(1,2)","(3,4)"}`
- Generic array parser splits on ALL commas: `"(1`, `2)"`, `"(3`, `4)"`
- Results in malformed individual elements that can't be parsed

### Root Cause Analysis
**Array Parsing Logic**: The generic `Vec<T>` implementation splits array content by `typ.array_delimiter` without respecting quoted boundaries or nested delimiters.

```rust
// Problematic approach in collections.rs
let items = narrowed.split(typ.array_delimiter); // Splits ALL commas
```

**POINT Type Challenge**: 
- Text format: `(x,y)` contains internal commas
- Array format: `{"(1,2)","(3,4)"}` has delimiters AND internal commas
- Generic parser can't distinguish between structural commas and data commas

### Solution: Custom Type-Specific Array Implementation

**Approach**: Create a newtype wrapper with custom `FromSql` implementation that handles the specific parsing challenges.

```rust
/// Wrapper for Vec<Point> to handle array parsing with comma conflicts
#[derive(Debug, Clone, PartialEq)]
pub struct PointArray(pub Vec<Point>);

impl<'a> FromSql<'a> for PointArray {
    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Custom parsing that respects quoted boundaries
        let mut current_element = String::new();
        let mut in_quotes = false;
        
        for ch in content.chars() {
            match ch {
                '"' => in_quotes = !in_quotes,
                ',' if !in_quotes => {
                    // Process completed element
                    result.push(Point::from_sql_text(&current_element, field)?);
                    current_element.clear();
                }
                _ => current_element.push(ch),
            }
        }
        Ok(PointArray(result))
    }
}
```

### Key Implementation Details

**Quote-Aware Parsing**:
- Track `in_quotes` state while iterating through characters
- Only treat commas as delimiters when `!in_quotes`
- Preserve quoted content including internal commas

**Type System Integration**:
- Custom newtype prevents conflicts with generic `Vec<T>` implementation
- Implements `FromSql` for the specific PostgreSQL array OID
- Provides conversion methods (`From`/`Into`) for seamless usage

**Binary Format Handling**:
- Implement both text and binary array parsing
- Binary format doesn't have delimiter conflicts (uses length prefixes)
- Manually implement array binary format to avoid generic conflicts

### Testing Strategy
```rust
// Test that was previously failing
let point_array: PointArray = client
    .read_single_value("select ARRAY[point(0,0), point(1,1), point(-1,-1)];", &[])
    .await
    .unwrap();
```

**Result**: Successfully parses complex arrays while maintaining 100% test coverage.

### Generalization Pattern
This solution pattern applies to any PostgreSQL type where:
1. **Internal delimiters conflict** with array delimiters
2. **Text representation is complex** (contains structural characters)
3. **Generic parsing fails** due to format ambiguity

**Examples**: 
- Geometric types (POINT, LINE, POLYGON) with coordinate lists
- JSON types with internal commas in text format
- Range types with internal comma-separated bounds
- Custom composite types with structured text representation

### Performance Considerations
- **Character-by-character parsing** has minor overhead vs simple `split()`
- **Quote tracking** adds state management but ensures correctness
- **Worth the cost** for types where generic parsing fails completely
- **Selective application** - only use for types that actually need it

This approach demonstrates how to handle complex parsing challenges while maintaining the overall type system architecture.

## PostgreSQL Binary Format Reverse Engineering Technique

### The Challenge: Unknown Binary Formats
**Problem**: PostgreSQL uses proprietary binary formats for many types that are not well documented. Implementing `FromSql` binary format support requires understanding these formats.

### The Solution: Text/Binary Mode Combination
**Technique**: Use a combination of text mode (for data insertion) and binary mode (for data retrieval) to reverse engineer the binary format.

**Step-by-Step Process**:
1. **Create test table with ID column**: `CREATE TABLE test_format(id int, target_column target_type);`
2. **Insert test data using text mode**: Use plain SQL without parameters to insert known values
3. **Force binary mode for retrieval**: Use parameter binding on the ID column to force binary protocol
4. **Add debug printing**: Temporarily add hex dump printing in `from_sql_binary` method
5. **Analyze the patterns**: Study the binary data to understand the format structure

**Example Implementation**:
```rust
fn from_sql_binary(raw: &[u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
    // Debug: Print the binary format to understand it
    println!("Binary length: {} bytes", raw.len());
    println!("Binary data (hex): {}", raw.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" "));
    
    // Add analysis of suspected format structure
    if raw.len() >= 4 {
        println!("  Header bytes: {:02x} {:02x} {:02x} {:02x}", raw[0], raw[1], raw[2], raw[3]);
        if raw.len() > 4 {
            println!("  Data bytes: {}", raw[4..].iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" "));
        }
    }
    
    // Return error to fall back to text mode initially
    Err("Format investigation in progress".into())
}
```

**Test Pattern for Binary Mode Investigation**:
```rust
// Setup: Create table and insert test data (text mode)
client.execute_non_query("CREATE TABLE test_format(id int, addr inet);", &[]).await.unwrap();
client.execute_non_query("INSERT INTO test_format VALUES (1, '127.0.0.1'), (2, '::1');", &[]).await.unwrap();

// Investigation: Force binary mode with parameter binding
for id in 1..=2 {
    println!("--- Testing ID {} ---", id);
    let _: Result<TargetType, _> = client.read_single_value("SELECT addr FROM test_format WHERE id = $1;", &[&id]).await;
}
```

### Why This Works
- **Text insertion**: PostgreSQL handles text-to-binary conversion internally, storing proper binary format
- **Parameter binding**: Forces binary protocol for the entire query result set
- **Unrelated parameter**: Using ID parameter bypasses type-specific parameter binding issues
- **Fallback behavior**: When binary format fails, PostgreSQL automatically falls back to text mode

### Success Example: INET Type
**Discovered Format**: `[family][bits][is_cidr][addr_size][address_bytes...]`
- Family: 2=IPv4, 3=IPv6  
- Bits: CIDR prefix length
- Is CIDR: Flag (observed as always 0)
- Address size: 4 for IPv4, 16 for IPv6
- Address bytes: Raw IP address bytes

**Implementation Result**: Complete binary format support for INET/CIDR types with both reading and writing capability.

### Generalization
This technique works for any PostgreSQL type with unknown binary format:
1. Create investigation test with ID-based parameter binding
2. Insert known test values via text mode
3. Retrieve with binary mode to see format
4. Analyze patterns and implement parsing
5. Test round-trip behavior with parameter binding

**Key Insight**: The combination of PostgreSQL's text-to-binary conversion (insertion) and forced binary mode (retrieval) provides a reliable way to discover proprietary binary formats without external documentation.

This reverse engineering approach has proven essential for implementing comprehensive PostgreSQL type support in custom protocol implementations.