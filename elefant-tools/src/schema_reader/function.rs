use tokio_postgres::Row;
use tracing::instrument;
use crate::{FunctionKind, Parallel, Volatility};
use crate::postgres_client_wrapper::{FromRow, RowEnumExt};
use crate::schema_reader::{SchemaReader};

pub struct FunctionResult {
    pub schema_name: String,
    pub function_name: String,
    pub language_name: String,
    pub estimated_cost: f32,
    pub estimated_rows: f32,
    pub support_function_name: Option<String>,
    pub function_kind: FunctionKind,
    pub security_definer: bool,
    pub leak_proof: bool,
    pub strict: bool,
    pub returns_set: bool,
    pub volatility: Volatility,
    pub parallel: Parallel,
    pub sql_body: String,
    pub configuration: Option<Vec<String>>,
    pub arguments: String,
    pub result: Option<String>,
    pub comment: Option<String>,
}

impl FromRow for FunctionResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            schema_name: row.try_get(0)?,
            function_name: row.try_get(1)?,
            language_name: row.try_get(2)?,
            estimated_cost: row.try_get(3)?,
            estimated_rows: row.try_get(4)?,
            support_function_name: row.try_get(5)?,
            function_kind: row.try_get_enum_value(6)?,
            security_definer: row.try_get(7)?,
            leak_proof: row.try_get(8)?,
            strict: row.try_get(9)?,
            returns_set: row.try_get(10)?,
            volatility: row.try_get_enum_value(11)?,
            parallel: row.try_get_enum_value(12)?,
            sql_body: row.try_get(13)?,
            configuration: row.try_get(14)?,
            arguments: row.try_get(15)?,
            result: row.try_get(16)?,
            comment: row.try_get(17)?,
        })
    }
}

impl SchemaReader<'_> {
    #[instrument(skip_all)]
    pub(in crate::schema_reader) async fn get_functions(&self) -> crate::Result<Vec<FunctionResult>> {
        //language=postgresql
        let query = if self.connection.version() >= 140 {
            r#"
select ns.nspname as schema_name,
    proc.proname as function_name,
       pl.lanname as language_name,
       proc.procost as estimated_cost,
       proc.prorows as estimated_rows,
       support_function.proname as support_function_name,
       proc.prokind as function_kind,
       proc.prosecdef as security_definer,
       proc.proleakproof as leak_proof,
       proc.proisstrict as strict,
       proc.proretset as returns_set,
       proc.provolatile as volatility,
       proc.proparallel as parallel,
       coalesce(pg_get_function_sqlbody(proc.oid), proc.prosrc) as sql_body,
       proc.proconfig as configuration,
       pg_get_function_arguments(proc.oid) as arguments,
       pg_get_function_result(proc.oid) as result,
       des.description
from pg_proc proc
         join pg_namespace ns on proc.pronamespace = ns.oid
         join pg_language pl on proc.prolang = pl.oid
         left join pg_type variadic_type on proc.provariadic = variadic_type.oid
         left join pg_proc support_function on proc.prosupport = support_function.oid
         join pg_type return_type on proc.prorettype = return_type.oid
         left join pg_depend dep on proc.oid = dep.objid and dep.deptype = 'e'
         left join pg_extension ext on dep.refobjid = ext.oid
         left join pg_description des on proc.oid = des.objoid
where ns.nspname = 'public' and ext.extname is null
order by ns.nspname, proc.proname;
"#
        } else {
            r#"
select ns.nspname as schema_name,
    proc.proname as function_name,
       pl.lanname as language_name,
       proc.procost as estimated_cost,
       proc.prorows as estimated_rows,
       support_function.proname as support_function_name,
       proc.prokind as function_kind,
       proc.prosecdef as security_definer,
       proc.proleakproof as leak_proof,
       proc.proisstrict as strict,
       proc.proretset as returns_set,
       proc.provolatile as volatility,
       proc.proparallel as parallel,
       proc.prosrc as sql_body,
       proc.proconfig as configuration,
       pg_get_function_arguments(proc.oid) as arguments,
       pg_get_function_result(proc.oid) as result,
       des.description
from pg_proc proc
         join pg_namespace ns on proc.pronamespace = ns.oid
         join pg_language pl on proc.prolang = pl.oid
         left join pg_type variadic_type on proc.provariadic = variadic_type.oid
         left join pg_proc support_function on proc.prosupport = support_function.oid
         join pg_type return_type on proc.prorettype = return_type.oid
         left join pg_depend dep on proc.oid = dep.objid and dep.deptype = 'e'
         left join pg_extension ext on dep.refobjid = ext.oid
         left join pg_description des on proc.oid = des.objoid
where ns.nspname = 'public' and ext.extname is null
order by ns.nspname, proc.proname;
"#
        };
        self.connection.get_results(query).await
    }
}
