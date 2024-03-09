use std::fmt::Display;
use crate::{ElefantToolsError, PostgresSchema};
use crate::postgres_client_wrapper::FromPgChar;
use ordered_float::NotNan;
use crate::object_id::ObjectId;
use crate::whitespace_ignorant_string::WhitespaceIgnorantString;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::{TypeOrFunctionName};

#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
pub enum FunctionKind {
    #[default]
    Function,
    Procedure,
    Aggregate,
    Window,
}

impl FromPgChar for FunctionKind {
    fn from_pg_char(c: char) -> Result<Self, crate::ElefantToolsError> {
        match c {
            'f' => Ok(FunctionKind::Function),
            'p' => Ok(FunctionKind::Procedure),
            'a' => Ok(FunctionKind::Aggregate),
            'w' => Ok(FunctionKind::Window),
            _ => Err(ElefantToolsError::UnknownFunctionKind(c.to_string()))
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
pub enum Volatility {
    Immutable,
    Stable,
    #[default]
    Volatile,
}

impl FromPgChar for Volatility {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'i' => Ok(Volatility::Immutable),
            's' => Ok(Volatility::Stable),
            'v' => Ok(Volatility::Volatile),
            _ => Err(ElefantToolsError::UnknownVolatility(c.to_string()))
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
pub enum Parallel {
    Safe,
    Restricted,
    #[default]
    Unsafe,
}

impl FromPgChar for Parallel {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            's' => Ok(Parallel::Safe),
            'r' => Ok(Parallel::Restricted),
            'u' => Ok(Parallel::Unsafe),
            _ => Err(ElefantToolsError::UnknownParallel(c.to_string()))
        }
    }
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PostgresFunction {
    pub function_name: String,
    pub language: String,
    pub estimated_cost: NotNan<f32>,
    pub estimated_rows: NotNan<f32>,
    pub support_function: Option<String>,
    pub kind: FunctionKind,
    pub security_definer: bool,
    pub leak_proof: bool,
    pub strict: bool,
    pub returns_set: bool,
    pub volatility: Volatility,
    pub parallel: Parallel,
    pub sql_body: WhitespaceIgnorantString,
    pub configuration: Option<Vec<String>>,
    pub arguments: String,
    pub result: Option<String>,
    pub comment: Option<String>,
    pub object_id: ObjectId,
}

impl PostgresFunction {
    pub fn get_create_statement(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let fn_name = format!("{}.{}", schema.name.quote(identifier_quoter, TypeOrFunctionName), &self.function_name.quote(identifier_quoter, TypeOrFunctionName));

        let function_keyword = if self.kind == FunctionKind::Procedure {
            "procedure"
        } else {
            "function"
        };

        let mut sql = format!("create {} {} ({})", function_keyword, fn_name, self.arguments);

        if let Some(result) = &self.result {
            sql.push_str(" returns ");

            sql.push_str(result);
        }

        sql.push_str(" language ");
        sql.push_str(&self.language);

        if self.kind == FunctionKind::Window {
            sql.push_str("window ");
        }

        if self.kind != FunctionKind::Procedure {
            match self.volatility {
                Volatility::Immutable => sql.push_str(" immutable "),
                Volatility::Stable => sql.push_str(" stable "),
                Volatility::Volatile => sql.push_str(" volatile "),
            }

            match self.parallel {
                Parallel::Safe => sql.push_str(" parallel safe "),
                Parallel::Restricted => sql.push_str(" parallel restricted "),
                Parallel::Unsafe => sql.push_str(" parallel unsafe "),
            }

            if self.leak_proof {
                sql.push_str(" leakproof ");
            }

            if self.strict {
                sql.push_str(" strict ");
            }
        }

        if self.security_definer {
            sql.push_str(" security definer ");
        }

        if let Some(configuration) = &self.configuration {
            sql.push_str(" set ");
            for cfg in configuration {
                sql.push_str(cfg);
            }
        }

        if self.kind != FunctionKind::Procedure {
            sql.push_str("cost ");
            sql.push_str(&self.estimated_cost.to_string());

            if self.estimated_rows.into_inner() > 0. {
                sql.push_str(" rows ");
                sql.push_str(&self.estimated_rows.to_string());
            }

            if let Some(support_function_name) = &self.support_function {
                sql.push_str(" support ");
                sql.push_str(support_function_name);
            }
        }

        sql.push_str(" as $$");
        sql.push_str(&self.sql_body);
        sql.push_str("$$;");

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on ");
            sql.push_str(function_keyword);
            String::push_str(&mut sql, " ");
            sql.push_str(&fn_name);
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }


        sql
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
pub enum FinalModify {
    #[default]
    ReadOnly,
    Shareable,
    ReadWrite,
}

impl FromPgChar for FinalModify {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'r' => Ok(FinalModify::ReadOnly),
            's' => Ok(FinalModify::Shareable),
            'w' => Ok(FinalModify::ReadWrite),
            _ => Err(ElefantToolsError::UnknownAggregateFinalFunctionModify(c.to_string()))
        }
    }
}

impl Display for FinalModify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            FinalModify::ReadOnly => "read_only",
            FinalModify::Shareable => "shareable",
            FinalModify::ReadWrite => "read_write",
        };
        write!(f, "{}", str)
    }
}


#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PostgresAggregateFunction {
    pub function_name: String,
    pub arguments: String,
    pub state_transition_function: String,
    pub final_function: Option<String>,
    pub combine_function: Option<String>,
    pub serial_function: Option<String>,
    pub deserial_function: Option<String>,
    pub moving_state_transition_function: Option<String>,
    pub inverse_moving_state_transition_function: Option<String>,
    pub moving_final_function: Option<String>,
    pub final_extra_data: bool,
    pub moving_final_extra_data: bool,
    pub final_modify: FinalModify,
    pub moving_final_modify: FinalModify,
    pub sort_operator: Option<String>,
    pub transition_type: String,
    pub transition_space: Option<i32>,
    pub moving_transition_type: Option<String>,
    pub moving_transition_space: Option<i32>,
    pub initial_value: Option<String>,
    pub moving_initial_value: Option<String>,
    pub parallel: Parallel,
    pub object_id: ObjectId,
}

impl PostgresAggregateFunction {
    pub fn get_create_statement(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let fn_name = format!("{}.{}", schema.name.quote(identifier_quoter, TypeOrFunctionName), &self.function_name.quote(identifier_quoter, TypeOrFunctionName));

        let mut sql = format!("create aggregate {} ({}) (\n", fn_name, self.arguments);

        sql.push_str("\tsfunc = ");
        sql.push_str(&self.state_transition_function);
        sql.push_str(",\n\tstype=");
        sql.push_str(&self.transition_type);

        if let Some(transition_space) = &self.transition_space {
            sql.push_str(",\n\tsspace=");
            sql.push_str(&transition_space.to_string());
        }

        if let Some(serial_function) = &self.serial_function {
            sql.push_str(",\n\tsfunc=");
            sql.push_str(serial_function);
        }

        if let Some(deserial_function) = &self.deserial_function {
            sql.push_str(",\n\tdfunc=");
            sql.push_str(deserial_function);
        }

        if let Some(initial_value) = &self.initial_value {
            sql.push_str(",\n\tinitcond=");
            sql.push_str(initial_value);
        }

        if let Some(final_function) = &self.final_function {
            sql.push_str(",\n\tfinalfunc=");
            sql.push_str(final_function);
            
            sql.push_str(",\n\tfinalfunc_modify=");
            sql.push_str(&self.final_modify.to_string());

            if self.final_extra_data {
                sql.push_str(",\n\tfinalfunc_extra");
            }
        }


        if let Some(moving_state_transition_function) = &self.moving_state_transition_function {
            sql.push_str(",\n\tmsfunc=");
            sql.push_str(moving_state_transition_function);
        }

        if let Some(inverse_moving_state_transition_function) = &self.inverse_moving_state_transition_function {
            sql.push_str(",\n\tminv_sfunc=");
            sql.push_str(inverse_moving_state_transition_function);
        }

        if let Some(moving_final_function) = &self.moving_final_function {
            sql.push_str(",\n\tmfinalfunc=");
            sql.push_str(moving_final_function);
            
            sql.push_str(",\n\tmfinalfunc_modify=");
            sql.push_str(&self.moving_final_modify.to_string());
            
            if self.moving_final_extra_data {
                sql.push_str(",\n\tmfinalfunc_extra");
            }
        }


        if let Some(moving_transition_type) = &self.moving_transition_type {
            sql.push_str(",\n\tmstype=");
            sql.push_str(moving_transition_type);

            if let Some(moving_transition_space) = &self.moving_transition_space {
                sql.push_str(",\n\tmsspace=");
                sql.push_str(&moving_transition_space.to_string());
            }
        }


        if let Some(moving_initial_value) = &self.moving_initial_value {
            sql.push_str(",\n\tminitcond=");
            sql.push_str(moving_initial_value);
        }

        if let Some(sort_operator) = &self.sort_operator {
            sql.push_str(",\n\tsortop=");
            sql.push_str(sort_operator);
        }

        match self.parallel {
            Parallel::Safe => sql.push_str(",\n\tparallel=safe"),
            Parallel::Restricted => sql.push_str(",\n\tparallel=restricted"),
            Parallel::Unsafe => sql.push_str(",\n\tparallel=unsafe"),
        }

        sql.push_str("\n);");

        sql
    }
}