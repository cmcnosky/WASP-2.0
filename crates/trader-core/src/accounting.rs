use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{AccountSnapshot, OrderSide, Symbol, WholeQuantity},
    error::{CoreError, CoreResult},
    fixed::{Money, Price},
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AccountingEvent {
    CashDeposit {
        amount: Money,
        at: DateTime<Utc>,
    },
    CashWithdrawal {
        amount: Money,
        at: DateTime<Utc>,
    },
    Dividend {
        amount: Money,
        at: DateTime<Utc>,
    },
    Fee {
        amount: Money,
        at: DateTime<Utc>,
    },
    Fill {
        symbol: Symbol,
        side: OrderSide,
        quantity: WholeQuantity,
        price: Price,
        fee: Money,
        at: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountingPosition {
    pub quantity: WholeQuantity,
    pub average_cost: Price,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountingState {
    pub cash: Money,
    pub positions: BTreeMap<Symbol, AccountingPosition>,
    /// Closed-price P&L less every fill fee, recognized when the fee is charged.
    pub realized_pnl: Money,
    pub fees: Money,
    pub dividends: Money,
}

pub fn replay(events: &[AccountingEvent]) -> CoreResult<AccountingState> {
    let mut state = AccountingState::default();
    let mut last_timestamp = None;
    for event in events {
        let timestamp = match event {
            AccountingEvent::CashDeposit { at, .. }
            | AccountingEvent::CashWithdrawal { at, .. }
            | AccountingEvent::Dividend { at, .. }
            | AccountingEvent::Fee { at, .. }
            | AccountingEvent::Fill { at, .. } => *at,
        };
        if last_timestamp.is_some_and(|last| timestamp < last) {
            return Err(CoreError::AccountingInvariant(
                "accounting events are not chronological".into(),
            ));
        }
        last_timestamp = Some(timestamp);
        apply(&mut state, event)?;
    }
    Ok(state)
}

fn require_non_negative(name: &str, amount: Money) -> CoreResult<()> {
    if amount.is_negative() {
        Err(CoreError::AccountingInvariant(format!(
            "{name} cannot be negative"
        )))
    } else {
        Ok(())
    }
}

fn apply(state: &mut AccountingState, event: &AccountingEvent) -> CoreResult<()> {
    match event {
        AccountingEvent::CashDeposit { amount, .. } => {
            require_non_negative("deposit", *amount)?;
            state.cash = state.cash.checked_add(*amount)?;
        }
        AccountingEvent::CashWithdrawal { amount, .. } => {
            require_non_negative("withdrawal", *amount)?;
            state.cash = state.cash.checked_sub(*amount)?;
        }
        AccountingEvent::Dividend { amount, .. } => {
            require_non_negative("dividend", *amount)?;
            state.cash = state.cash.checked_add(*amount)?;
            state.dividends = state.dividends.checked_add(*amount)?;
        }
        AccountingEvent::Fee { amount, .. } => {
            require_non_negative("fee", *amount)?;
            state.cash = state.cash.checked_sub(*amount)?;
            state.fees = state.fees.checked_add(*amount)?;
        }
        AccountingEvent::Fill {
            symbol,
            side,
            quantity,
            price,
            fee,
            ..
        } => {
            require_non_negative("fill fee", *fee)?;
            if quantity.get() == 0 || price.is_negative() || *price == Price::ZERO {
                return Err(CoreError::AccountingInvariant(
                    "fill requires positive whole quantity and price".into(),
                ));
            }
            let notional = price.checked_mul_quantity(quantity.get())?;
            let position = state.positions.entry(symbol.clone()).or_default();
            match side {
                OrderSide::Buy => {
                    let old_cost = position
                        .average_cost
                        .checked_mul_quantity(position.quantity.get())?;
                    let new_quantity = position
                        .quantity
                        .get()
                        .checked_add(quantity.get())
                        .ok_or(CoreError::ArithmeticOverflow("position quantity"))?;
                    let total_cost = old_cost.checked_add(notional)?;
                    position.quantity = WholeQuantity::new(new_quantity);
                    position.average_cost = Price(
                        total_cost
                            .fixed()
                            .checked_div(crate::Fixed::from_units(i128::from(new_quantity))?)?,
                    );
                    state.cash = state.cash.checked_sub(notional)?.checked_sub(*fee)?;
                    state.realized_pnl = state.realized_pnl.checked_sub(*fee)?;
                }
                OrderSide::Sell => {
                    if quantity.get() > position.quantity.get() {
                        return Err(CoreError::AccountingInvariant(format!(
                            "sell exceeds position for {symbol}"
                        )));
                    }
                    let cost_basis = position.average_cost.checked_mul_quantity(quantity.get())?;
                    state.realized_pnl = state
                        .realized_pnl
                        .checked_add(notional.checked_sub(cost_basis)?)?
                        .checked_sub(*fee)?;
                    state.cash = state.cash.checked_add(notional)?.checked_sub(*fee)?;
                    position.quantity =
                        WholeQuantity::new(position.quantity.get() - quantity.get());
                    if position.quantity == WholeQuantity::ZERO {
                        position.average_cost = Price::ZERO;
                    }
                }
            }
            state.fees = state.fees.checked_add(*fee)?;
        }
    }
    Ok(())
}

pub fn compare_to_account(state: &AccountingState, account: &AccountSnapshot) -> Vec<String> {
    let mut differences = Vec::new();
    if state.cash != account.cash {
        differences.push(format!(
            "cash mismatch: ledger={} account={}",
            state.cash, account.cash
        ));
    }
    let account_positions: BTreeMap<_, _> = account
        .positions
        .iter()
        .map(|position| (&position.symbol, position.quantity))
        .collect();
    for (symbol, position) in &state.positions {
        let broker_quantity = account_positions
            .get(symbol)
            .copied()
            .unwrap_or(WholeQuantity::ZERO);
        if position.quantity != broker_quantity {
            differences.push(format!(
                "position mismatch for {symbol}: ledger={} account={}",
                position.quantity.get(),
                broker_quantity.get()
            ));
        }
    }
    for (symbol, quantity) in account_positions {
        if !state.positions.contains_key(symbol) && quantity != WholeQuantity::ZERO {
            differences.push(format!(
                "position exists only at account for {symbol}: {}",
                quantity.get()
            ));
        }
    }
    differences
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn fills_drive_position_and_realized_pnl() {
        let at = Utc.with_ymd_and_hms(2025, 1, 2, 15, 0, 0).unwrap();
        let symbol = Symbol::new("SPY").unwrap();
        let events = vec![
            AccountingEvent::CashDeposit {
                amount: Money::from_units(1_000).unwrap(),
                at,
            },
            AccountingEvent::Fill {
                symbol: symbol.clone(),
                side: OrderSide::Buy,
                quantity: WholeQuantity::new(2),
                price: "100".parse().unwrap(),
                fee: Money::ZERO,
                at,
            },
            AccountingEvent::Fill {
                symbol,
                side: OrderSide::Sell,
                quantity: WholeQuantity::new(1),
                price: "110".parse().unwrap(),
                fee: "1".parse().unwrap(),
                at,
            },
        ];
        let result = replay(&events).unwrap();
        assert_eq!(result.cash, "909".parse().unwrap());
        assert_eq!(result.realized_pnl, "9".parse().unwrap());
    }

    #[test]
    fn realized_pnl_recognizes_buy_and_sell_fill_fees_exactly_once() {
        let at = Utc.with_ymd_and_hms(2025, 1, 2, 15, 0, 0).unwrap();
        let symbol = Symbol::new("SPY").unwrap();
        let events = vec![
            AccountingEvent::CashDeposit {
                amount: Money::from_units(1_000).unwrap(),
                at,
            },
            AccountingEvent::Fill {
                symbol: symbol.clone(),
                side: OrderSide::Buy,
                quantity: WholeQuantity::new(2),
                price: "100".parse().unwrap(),
                fee: "2".parse().unwrap(),
                at,
            },
            AccountingEvent::Fill {
                symbol,
                side: OrderSide::Sell,
                quantity: WholeQuantity::new(2),
                price: "110".parse().unwrap(),
                fee: "1".parse().unwrap(),
                at,
            },
        ];

        let result = replay(&events).unwrap();
        assert_eq!(result.cash, Money::from_units(1_017).unwrap());
        assert_eq!(result.realized_pnl, Money::from_units(17).unwrap());
        assert_eq!(result.fees, Money::from_units(3).unwrap());
    }
}
