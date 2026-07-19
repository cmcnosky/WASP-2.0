use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

/// Six-decimal checked fixed-point value used at all trading/accounting boundaries.
/// The JSON wire representation is the scaled signed integer, never a float.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct Fixed(i128);

impl Fixed {
    pub const SCALE: i128 = 1_000_000;
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(Self::SCALE);

    pub const fn from_scaled(scaled: i128) -> Self {
        Self(scaled)
    }

    pub fn from_units(units: i128) -> CoreResult<Self> {
        units
            .checked_mul(Self::SCALE)
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("from_units"))
    }

    pub const fn scaled(self) -> i128 {
        self.0
    }

    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    pub fn checked_add(self, rhs: Self) -> CoreResult<Self> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("addition"))
    }

    pub fn checked_sub(self, rhs: Self) -> CoreResult<Self> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("subtraction"))
    }

    pub fn checked_mul(self, rhs: Self) -> CoreResult<Self> {
        self.0
            .checked_mul(rhs.0)
            .and_then(|value| value.checked_div(Self::SCALE))
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("multiplication"))
    }

    pub fn checked_mul_i128(self, rhs: i128) -> CoreResult<Self> {
        self.0
            .checked_mul(rhs)
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("integer multiplication"))
    }

    pub fn checked_div(self, rhs: Self) -> CoreResult<Self> {
        if rhs.0 == 0 {
            return Err(CoreError::DivisionByZero);
        }
        self.0
            .checked_mul(Self::SCALE)
            .and_then(|value| value.checked_div(rhs.0))
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("division"))
    }

    pub fn checked_abs(self) -> CoreResult<Self> {
        self.0
            .checked_abs()
            .map(Self)
            .ok_or(CoreError::ArithmeticOverflow("absolute value"))
    }

    pub const fn min(self, rhs: Self) -> Self {
        if self.0 <= rhs.0 {
            self
        } else {
            rhs
        }
    }

    pub const fn max(self, rhs: Self) -> Self {
        if self.0 >= rhs.0 {
            self
        } else {
            rhs
        }
    }

    pub fn floor_units(self) -> i128 {
        self.0.div_euclid(Self::SCALE)
    }
}

impl FromStr for Fixed {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(CoreError::InvalidFixed(value.into()));
        }
        let negative = value.starts_with('-');
        let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
        let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if fractional.len() > 6
            || whole.is_empty()
            || !whole.chars().all(|c| c.is_ascii_digit())
            || !fractional.chars().all(|c| c.is_ascii_digit())
        {
            return Err(CoreError::InvalidFixed(value.into()));
        }
        let whole: i128 = whole
            .parse()
            .map_err(|_| CoreError::InvalidFixed(value.into()))?;
        let mut fraction = fractional.to_owned();
        fraction.extend(std::iter::repeat_n('0', 6 - fraction.len()));
        let fraction: i128 = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse()
                .map_err(|_| CoreError::InvalidFixed(value.into()))?
        };
        let scaled = whole
            .checked_mul(Self::SCALE)
            .and_then(|v| v.checked_add(fraction))
            .ok_or(CoreError::ArithmeticOverflow("parsing"))?;
        Ok(Self(if negative { -scaled } else { scaled }))
    }
}

impl fmt::Display for Fixed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let negative = self.0 < 0;
        let magnitude = self.0.unsigned_abs();
        let scale = Self::SCALE as u128;
        let whole = magnitude / scale;
        let fractional = magnitude % scale;
        if negative {
            write!(formatter, "-")?;
        }
        write!(formatter, "{whole}.{fractional:06}")
    }
}

macro_rules! fixed_wrapper {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Deserialize,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Fixed);

        impl $name {
            pub const ZERO: Self = Self(Fixed::ZERO);
            pub const fn from_scaled(scaled: i128) -> Self {
                Self(Fixed::from_scaled(scaled))
            }
            pub fn from_units(units: i128) -> CoreResult<Self> {
                Fixed::from_units(units).map(Self)
            }
            pub const fn fixed(self) -> Fixed {
                self.0
            }
            pub const fn scaled(self) -> i128 {
                self.0.scaled()
            }
            pub const fn is_negative(self) -> bool {
                self.0.is_negative()
            }
            pub fn checked_add(self, rhs: Self) -> CoreResult<Self> {
                self.0.checked_add(rhs.0).map(Self)
            }
            pub fn checked_sub(self, rhs: Self) -> CoreResult<Self> {
                self.0.checked_sub(rhs.0).map(Self)
            }
            pub fn checked_mul_quantity(self, quantity: u64) -> CoreResult<Money> {
                self.0.checked_mul_i128(i128::from(quantity)).map(Money)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = CoreError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Fixed::from_str(value).map(Self)
            }
        }
    };
}

fixed_wrapper!(Money);
fixed_wrapper!(Price);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_formats_without_float() {
        assert_eq!(Fixed::from_str("12.34").unwrap().scaled(), 12_340_000);
        assert_eq!(Fixed::from_str("-0.000001").unwrap().scaled(), -1);
        assert_eq!(Fixed::from_scaled(-1).to_string(), "-0.000001");
        assert!(Fixed::from_str("1.0000001").is_err());
    }

    #[test]
    fn add_then_subtract_property_is_identity() {
        // A dependency-free property sweep keeps the MSRV lockfile bounded.
        let samples = [
            -1_000_000_000i128,
            -1_000_001,
            -1,
            0,
            1,
            999_999,
            1_000_000_000,
        ];
        for a in samples {
            for b in samples {
                let a = Fixed::from_scaled(a);
                let b = Fixed::from_scaled(b);
                assert_eq!(a.checked_add(b).unwrap().checked_sub(b).unwrap(), a);
            }
        }
    }
}
