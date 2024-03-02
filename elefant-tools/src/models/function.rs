use crate::ElefantToolsError;
use crate::postgres_client_wrapper::FromPgChar;
use ordered_float::NotNan;
use crate::whitespace_ignorant_string::WhitespaceIgnorantString;
use crate::quoting::{IdentifierQuoter, quote_value_string};
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

#[derive(Debug, Eq, PartialEq, Default)]
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
}

impl PostgresFunction {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        let fn_name = identifier_quoter.quote(&self.function_name, TypeOrFunctionName);
        
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
            sql.push_str(" ");
            sql.push_str(&fn_name);
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }


        sql
    }
}
