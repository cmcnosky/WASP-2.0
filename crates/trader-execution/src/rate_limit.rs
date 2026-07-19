use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use crate::ExecutionError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestClass {
    /// Quotes and other routine requests may not consume the safety reserve.
    Routine,
    /// Reconciliation may consume part of the reserve but must leave an
    /// independent cancel-only reserve.
    Reconciliation,
    /// Cancels alone may consume the final reserve, never the provider cap.
    Cancel,
}

/// Monotonic sliding-window request budget. It never sleeps or retries: a
/// caller that lacks budget must skip routine work or enter a safe recovery
/// path. The configured ceiling is already below Alpaca's provider maximum.
#[derive(Clone, Debug)]
pub struct RequestBudget {
    limit_per_minute: usize,
    safety_reserve: usize,
    cancel_only_reserve: usize,
    observed_requests: VecDeque<Instant>,
    last_observed_at: Option<Instant>,
}

impl RequestBudget {
    pub fn new(limit_per_minute: u16, safety_reserve: u16) -> Result<Self, ExecutionError> {
        let limit_per_minute = usize::from(limit_per_minute);
        let safety_reserve = usize::from(safety_reserve);
        if limit_per_minute == 0
            || limit_per_minute > 180
            || safety_reserve == 0
            || safety_reserve >= limit_per_minute
        {
            return Err(ExecutionError::UnsafeConfiguration(
                "request budget requires a 1..=180 ceiling and a non-empty smaller reserve".into(),
            ));
        }
        let cancel_only_reserve = (safety_reserve / 2).max(1);
        Ok(Self {
            limit_per_minute,
            safety_reserve,
            cancel_only_reserve,
            observed_requests: VecDeque::new(),
            last_observed_at: None,
        })
    }

    /// Constructs a budget that assumes the prior process may have consumed
    /// the complete provider allowance immediately before it stopped. This
    /// deliberately quarantines all outbound requests for one minute after a
    /// process restart; losing rate-limit history must never create capacity.
    pub fn new_after_restart(
        limit_per_minute: u16,
        safety_reserve: u16,
        now: Instant,
    ) -> Result<Self, ExecutionError> {
        let mut budget = Self::new(limit_per_minute, safety_reserve)?;
        budget.observed_requests = std::iter::repeat_n(now, budget.limit_per_minute).collect();
        budget.last_observed_at = Some(now);
        Ok(budget)
    }

    pub fn try_acquire(&mut self, class: RequestClass, now: Instant) -> Result<(), ExecutionError> {
        if self.last_observed_at.is_some_and(|last| now < last) {
            return Err(ExecutionError::AuthorityDenied(
                "request-budget clock moved backwards".into(),
            ));
        }
        self.last_observed_at = Some(now);
        let window_start = now.checked_sub(Duration::from_secs(60));
        while self
            .observed_requests
            .front()
            .is_some_and(|observed| window_start.is_some_and(|start| *observed <= start))
        {
            self.observed_requests.pop_front();
        }
        let class_limit = match class {
            RequestClass::Routine => self.limit_per_minute - self.safety_reserve,
            RequestClass::Reconciliation => self.limit_per_minute - self.cancel_only_reserve,
            RequestClass::Cancel => self.limit_per_minute,
        };
        if self.observed_requests.len() >= class_limit {
            return Err(ExecutionError::Broker(format!(
                "request budget exhausted for {class:?}; no request was sent"
            )));
        }
        self.observed_requests.push_back(now);
        Ok(())
    }

    pub fn in_window(&self) -> usize {
        self.observed_requests.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routine_work_cannot_consume_cancel_and_reconciliation_reserve() {
        let at = Instant::now();
        let mut budget = RequestBudget::new(6, 2).unwrap();
        for _ in 0..4 {
            budget.try_acquire(RequestClass::Routine, at).unwrap();
        }
        assert!(budget.try_acquire(RequestClass::Routine, at).is_err());
        budget
            .try_acquire(RequestClass::Reconciliation, at)
            .unwrap();
        assert!(budget
            .try_acquire(RequestClass::Reconciliation, at)
            .is_err());
        budget.try_acquire(RequestClass::Cancel, at).unwrap();
        assert!(budget.try_acquire(RequestClass::Cancel, at).is_err());
    }

    #[test]
    fn elapsed_window_recovers_capacity_but_injected_regression_fails_closed() {
        let at = Instant::now();
        let mut budget = RequestBudget::new(5, 2).unwrap();
        budget.try_acquire(RequestClass::Routine, at).unwrap();
        budget
            .try_acquire(RequestClass::Routine, at + Duration::from_secs(60))
            .unwrap();
        assert_eq!(budget.in_window(), 1);
        assert!(budget
            .try_acquire(RequestClass::Reconciliation, at + Duration::from_secs(59))
            .is_err());
    }

    #[test]
    fn restart_budget_denies_every_class_for_a_complete_window() {
        let at = Instant::now();
        let mut budget = RequestBudget::new_after_restart(5, 2, at).unwrap();
        for class in [
            RequestClass::Routine,
            RequestClass::Reconciliation,
            RequestClass::Cancel,
        ] {
            assert!(budget.try_acquire(class, at).is_err());
        }
        budget
            .try_acquire(RequestClass::Cancel, at + Duration::from_secs(60))
            .unwrap();
    }
}
