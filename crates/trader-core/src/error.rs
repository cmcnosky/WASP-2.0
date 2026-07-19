use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("fixed-point overflow during {0}")]
    ArithmeticOverflow(&'static str),
    #[error("division by zero")]
    DivisionByZero,
    #[error("invalid fixed-point value: {0}")]
    InvalidFixed(String),
    #[error("invalid hash: {0}")]
    InvalidHash(String),
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),
    #[error("invalid domain value: {0}")]
    InvalidDomain(String),
    #[error("insufficient history for {symbol}: need {required}, got {actual}")]
    InsufficientHistory {
        symbol: String,
        required: usize,
        actual: usize,
    },
    #[error("risk rejected: {0}")]
    RiskRejected(String),
    #[error("accounting invariant failed: {0}")]
    AccountingInvariant(String),
    #[error("serialization failed: {0}")]
    Serialization(String),
}

impl From<serde_json::Error> for CoreError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}
