// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Boolean search expressions that combine terms with `NOT`, `AND`, `OR`, and parentheses,
//! and compile them into [`StringFilter`](crate::common::strings::filter::StringFilter)s.
//!
//! Ported from Prometheus `util/searchexpr`.

mod compiler;
#[cfg(test)]
mod compiler_tests;
mod parser;
#[cfg(test)]
mod parser_tests;

pub(crate) use compiler::{TermFilterError, compile, validate};
pub(crate) use parser::{Expr, parse};

pub(crate) const MAX_EXPRESSION_BYTES: usize = 4096;
pub(crate) const MAX_EXPRESSION_TERMS: usize = 32;
pub(crate) const MAX_EXPRESSION_NESTING_DEPTH: usize = 16;

/// An error parsing or compiling a search expression.
#[derive(Debug, thiserror::Error)]
pub enum SearchExprError {
    #[error("search expression: {0}")]
    Invalid(String),
    /// The term filter factory refused a term.
    #[error("search expression: compile term {term:?}: {source}")]
    TermFilter {
        term: String,
        #[source]
        source: TermFilterError,
    },
}

pub(crate) fn invalid(message: impl Into<String>) -> SearchExprError {
    SearchExprError::Invalid(message.into())
}
