use crate::common::binop::{
    BinopFunc, abs_diff, avg, cmp, compare_eq, compare_gt, compare_gte, compare_lt, compare_lte,
    compare_neq, max, min, op_coalesce, op_div, op_minus, op_mod, op_mul, op_plus, op_pow,
    percent_change, sgn_diff,
};
use std::fmt;
use std::str::FromStr;
use valkey_module::ValkeyError;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum JoinReducer {
    AbsDiff,
    Avg,
    Cmp,
    Coalesce,
    Div,
    #[default]
    Eql,
    Mod,
    Mul,
    Pow,
    Sub,
    Gt,
    Gte,
    Lt,
    Lte,
    Max,
    Min,
    NotEq,
    PctChange,
    SgnDiff,
    Sum,
}

fn join_reducer_get(key: &str) -> Option<JoinReducer> {
    hashify::map_ignore_case!(
        key.as_bytes(),
        JoinReducer,
        "abs_diff" => JoinReducer::AbsDiff,
        "cmp" => JoinReducer::Cmp,
        "coalesce" => JoinReducer::Coalesce,
        "eq" => JoinReducer::Eql,
        "gt" => JoinReducer::Gt,
        "gte" => JoinReducer::Gte,
        "sub" => JoinReducer::Sub,
        "mod" => JoinReducer::Mod,
        "mul" => JoinReducer::Mul,
        "ne"  => JoinReducer::NotEq,
        "lt" => JoinReducer::Lt,
        "lte" => JoinReducer::Lte,
        "div" => JoinReducer::Div,
        "pow" => JoinReducer::Pow,
        "sgn_diff" => JoinReducer::SgnDiff,
        "pct_change" => JoinReducer::PctChange,

        "sum" => JoinReducer::Sum,
        "avg" => JoinReducer::Avg,
        "max" => JoinReducer::Max,
        "min" => JoinReducer::Min,
    )
    .copied()
}

impl JoinReducer {
    pub const fn as_str(&self) -> &'static str {
        use JoinReducer::*;
        match self {
            AbsDiff => "abs_diff",
            Sum => "sum",
            Cmp => "cmp",
            Coalesce => "coalesce",
            Div => "/",
            Eql => "==",
            Gt => ">",
            Gte => ">=",
            Mod => "%",
            Mul => "*",
            Lt => "<",
            Lte => "<=",
            NotEq => "!=",
            Pow => "^",
            SgnDiff => "sgn_diff",
            PctChange => "pct_change",
            Sub => "-",
            Avg => "avg",
            Max => "max",
            Min => "min",
        }
    }

    pub const fn get_handler(&self) -> BinopFunc {
        use JoinReducer::*;
        match self {
            Max => max,
            Min => min,
            Avg => avg,
            AbsDiff => abs_diff,
            Sum => op_plus,
            Cmp => cmp,
            Coalesce => op_coalesce,
            Div => op_div,
            Eql => compare_eq,
            Mod => op_mod,
            Mul => op_mul,
            Pow => op_pow,
            Sub => op_minus,
            Gt => compare_gt,
            Gte => compare_gte,
            Lt => compare_lt,
            Lte => compare_lte,
            NotEq => compare_neq,
            SgnDiff => sgn_diff,
            PctChange => percent_change,
        }
    }
}

impl FromStr for JoinReducer {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JoinReducer::try_from(s)
    }
}

impl TryFrom<&str> for JoinReducer {
    type Error = ValkeyError;

    fn try_from(op: &str) -> Result<Self, Self::Error> {
        match join_reducer_get(op) {
            Some(operator) => Ok(operator),
            None => Err(ValkeyError::String(format!(
                "TSDB: unknown binary op '{op}'"
            ))),
        }
    }
}

impl fmt::Display for JoinReducer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {}
