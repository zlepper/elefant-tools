# Context - Elefant-Client Query Method Refactoring

## Current Task

**Project**: Refactoring elefant-client query API for explicit binary vs simple mode control  
**Phase**: Planning Complete, Ready for Implementation  
**Goal**: Create `query_simple()` (text mode) and modify `query()` (always binary mode)

## Investigation Complete

**Investigation Status**: ✅ COMPLETE  
**Plan Document**: `plan.md` - Comprehensive implementation roadmap  
**Current Architecture**: Fully mapped and understood

## Key Findings

### Current Binary/Text Mode Logic
- **No parameters** → Simple query protocol → Text mode results
- **Has parameters** → Extended protocol (Parse/Bind/Execute) → Binary mode results  
- **No user control** over format selection (automatic based on parameter presence)

### Key Files Requiring Changes
1. **`postgres_client/statements.rs`** - Statement trait and protocol logic
2. **`postgres_client/query.rs`** - Core query execution and result handling  
3. **`postgres_client/easy_client.rs`** - Convenience method implementations
4. **All type tests** in `src/types/` - Add simple mode coverage

### Architecture Insights
- **QueryResult type**: Can already handle both formats via `field.format` detection
- **Statement trait**: Needs new `send_simple()` method for text mode
- **Protocol separation**: Simple vs Extended protocols are cleanly separated  
- **Format handling**: Existing `PostgresDataRow.get()` supports both binary/text

## Plan Summary

**Target API**:
```rust
// Simple mode - only accepts &str, returns SimpleQueryResult<T: FromSqlText>
let result: SimpleQueryResult = client.query_simple("SELECT 42").await?;

// Binary mode - accepts Statement, returns QueryResult<T: FromSqlBinary>  
let result: QueryResult = client.query("SELECT $1", &[&42]).await?;
```

**Key Changes**:
- **Statement trait**: Only handles `prepare()`, client handles execution
- **Split result types**: `SimpleQueryResult` (text) vs `QueryResult` (binary) 
- **Split FromSql traits**: `FromSqlBinary` vs `FromSqlText` for compile-time safety
- **Type system enforcement**: `&[u8]` only works with binary mode (compile-time error for simple mode)
- **Direct protocol access**: No wrapper methods, direct `send_frontend_message` + flush
- Comprehensive test coverage for all types in both modes

## Next Steps

**Ready to begin implementation**: All planning complete, start with Phase 1
**Implementation order**: Follow 5-phase plan in `plan.md`  
**Starting point**: Phase 1 - Split FromSql traits in `src/types/from_sql_row.rs`

## Implementation Plan Summary

**Phase 1**: Split FromSql traits - Create `FromSqlBase`, `FromSqlBinary`, `FromSqlText`
**Phase 2**: Split result types - Create `QueryResultBase`, `SimpleQueryResult`, `QueryResult` with shared code
**Phase 3**: Simplify Statement trait - Only handle `prepare()`, client handles execution  
**Phase 4**: Update all type implementations - Implement new trait hierarchy for all types
**Phase 5**: Comprehensive test coverage - Test both binary and simple modes, compile-time safety

## Key Design Decisions Made

### Code Sharing Strategy
- **FromSqlBase trait**: Eliminates duplication of `from_sql_null()` and `accepts_postgres_type()`
- **QueryResultBase struct**: Shares `next_result_set()`, `collect_to_vec()`, etc. between result types
- **Delegation pattern**: Both result types delegate common methods to shared base

### Type Safety Approach
- **Split result types**: `SimpleQueryResult<T: FromSqlText>` vs `QueryResult<T: FromSqlBinary>`
- **Compile-time enforcement**: `&[u8]` only works with binary mode (no `FromSqlText` impl)
- **Direct protocol access**: `send_frontend_message` + `flush`, no wrapper methods

### API Design
```rust
// Simple mode - compile-time restriction to &str
client.query_simple("SELECT 42").await?  // Returns SimpleQueryResult

// Binary mode - accepts any Statement  
client.query("SELECT $1", &[&42]).await?  // Returns QueryResult
```

## Files Created
- **`plan.md`** - Complete implementation roadmap with detailed 5-phase approach
- **`context.md`** - Updated with task status, design decisions, and implementation readiness

## Implementation Ready
**Status**: ✅ ALL PLANNING COMPLETE
**Next action**: Begin Phase 1 implementation in `src/types/from_sql_row.rs`
**Key insight**: Maximum code reuse through shared base traits and structs while maintaining compile-time safety