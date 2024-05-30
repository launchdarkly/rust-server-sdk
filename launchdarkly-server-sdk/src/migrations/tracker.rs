use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::Duration,
};

use launchdarkly_server_sdk_evaluation::{Context, Detail, Flag};

use crate::events::event::{BaseEvent, EventFactory, MigrationOpEvent};

use super::{Operation, Origin, Stage};

/// An MigrationOpTracker is responsible for managing the collection of measurements that which a user might wish to record
/// throughout a migration-assisted operation.
///
/// Example measurements include latency, errors, and consistency.
pub struct MigrationOpTracker {
    key: String,
    flag: Option<Flag>,
    context: Context,
    detail: Detail<Stage>,
    default_stage: Stage,

    mutex: Mutex<()>,

    operation: Option<Operation>,
    invoked: HashSet<Origin>,
    consistent: Option<bool>,
    consistent_ratio: Option<u64>,
    errors: HashSet<Origin>,
    latencies: HashMap<Origin, Duration>,
}

impl MigrationOpTracker {
    pub(crate) fn new(
        key: String,
        flag: Option<Flag>,
        context: Context,
        detail: Detail<Stage>,
        default_stage: Stage,
    ) -> Self {
        let consistent_ratio = match &flag {
            Some(f) => f
                .migration_settings
                .as_ref()
                .map(|s| s.check_ratio.unwrap_or(1)),
            None => None,
        };

        Self {
            key,
            flag,
            context,
            detail,
            default_stage,
            mutex: Mutex::new(()),
            operation: None,
            invoked: HashSet::new(),
            consistent: None,
            consistent_ratio,
            errors: HashSet::new(),
            latencies: HashMap::new(),
        }
    }

    /// Sets the migration related operation associated with these tracking measurements.
    pub fn operation(&mut self, operation: Operation) {
        match self.mutex.lock() {
            Ok(_guard) => self.operation = Some(operation),
            Err(e) => {
                error!("failed to acquire lock for operation tracking: {}", e);
            }
        }
    }

    /// Allows recording which origins were called during a migration.
    pub fn invoked(&mut self, origin: Origin) {
        match self.mutex.lock() {
            Ok(_guard) => _ = self.invoked.insert(origin),
            Err(e) => {
                error!("failed to acquire lock for invocation tracking: {}", e);
            }
        }
    }

    /// This method accepts a callable which should take no parameters and return a single boolean
    /// to represent the consistency check results for a read operation.
    ///
    /// A callable is provided in case sampling rules do not require consistency checking to run.
    /// In this case, we can avoid the overhead of a function by not using the callable.
    pub fn consistent(&mut self, is_consistent: impl Fn() -> bool) {
        // TODO: We need to add sampling here at some point.
        match self.mutex.lock() {
            Ok(_guard) => self.consistent = Some(is_consistent()),
            Err(e) => {
                error!("failed to acquire lock for consistency tracking: {}", e);
            }
        }
    }

    /// Allows recording which origins were called during a migration.
    pub fn error(&mut self, origin: Origin) {
        match self.mutex.lock() {
            Ok(_guard) => _ = self.errors.insert(origin),
            Err(e) => {
                error!("failed to acquire lock for invocation tracking: {}", e);
            }
        }
    }

    /// Allows tracking the recorded latency for an individual operation.
    pub fn latency(&mut self, origin: Origin, latency: Duration) {
        if latency.is_zero() {
            return;
        }

        match self.mutex.lock() {
            Ok(_guard) => _ = self.latencies.insert(origin, latency),
            Err(e) => {
                error!("failed to acquire lock for latency tracking: {}", e);
            }
        }
    }

    /// Creates an instance of [crate::MigrationOpEvent]. This event data can be
    /// provided to the [crate::Client::track_migration_op] method to rely this metric
    /// information upstream to LaunchDarkly services.
    pub fn build(&self) -> Result<MigrationOpEvent, String> {
        let _guard = self
            .mutex
            .lock()
            .map_err(|e| format!("failed to acquire lock for building event: {:?}", e))?;

        let operation = self
            .operation
            .ok_or_else(|| "operation not provided".to_string())?;

        self.check_invoked_consistency()?;

        if self.key.is_empty() {
            return Err("operation cannot contain an empty key".to_string());
        }

        let invoked = self.invoked.clone();
        if invoked.is_empty() {
            return Err("no origins were invoked".to_string());
        }

        Ok(MigrationOpEvent {
            base: BaseEvent::new(EventFactory::now(), self.context.clone()),
            key: self.key.clone(),
            version: self.flag.as_ref().map(|f| f.version),
            operation,
            default_stage: self.default_stage,
            evaluation: self.detail.clone(),
            invoked,
            consistency_check_ratio: self.consistent_ratio,
            consistency_check: self.consistent,
            errors: self.errors.clone(),
            latency: self.latencies.clone(),
        })
    }

    fn check_invoked_consistency(&self) -> Result<(), String> {
        for origin in [Origin::Old, Origin::New].iter() {
            if self.invoked.contains(origin) {
                continue;
            }

            if self.errors.contains(origin) {
                return Err(format!(
                    "provided error for origin {:?} without recording invocation",
                    origin
                ));
            }

            if self.latencies.contains_key(origin) {
                return Err(format!(
                    "provided latency for origin {:?} without recording invocation",
                    origin
                ));
            }
        }

        if self.consistent.is_some() && self.invoked.len() != 2 {
            return Err("provided consistency without recording both invocations".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use launchdarkly_server_sdk_evaluation::{ContextBuilder, Detail, Reason};
    use test_case::test_case;

    use super::{MigrationOpTracker, Operation, Origin, Stage};
    use crate::test_common::basic_flag;

    fn minimal_tracker() -> MigrationOpTracker {
        let mut tracker = MigrationOpTracker::new(
            "flag-key".into(),
            Some(basic_flag("flag-key")),
            ContextBuilder::new("user")
                .build()
                .expect("failed to build context"),
            Detail {
                value: Some(Stage::Live),
                variation_index: Some(1),
                reason: Reason::Fallthrough {
                    in_experiment: false,
                },
            },
            Stage::Live,
        );
        tracker.operation(Operation::Read);
        tracker.invoked(Origin::Old);
        tracker.invoked(Origin::New);

        tracker
    }

    #[test]
    fn build_minimal_tracker() {
        let tracker = minimal_tracker();
        let result = tracker.build();

        assert!(result.is_ok());
    }

    #[test]
    fn build_without_flag() {
        let mut tracker = minimal_tracker();
        tracker.flag = None;
        let result = tracker.build();

        assert!(result.is_ok());
    }

    #[test_case(Origin::Old)]
    #[test_case(Origin::New)]
    fn track_invocations_individually(origin: Origin) {
        let mut tracker = MigrationOpTracker::new(
            "flag-key".into(),
            Some(basic_flag("flag-key")),
            ContextBuilder::new("user")
                .build()
                .expect("failed to build context"),
            Detail {
                value: Some(Stage::Live),
                variation_index: Some(1),
                reason: Reason::Fallthrough {
                    in_experiment: false,
                },
            },
            Stage::Live,
        );
        tracker.operation(Operation::Read);
        tracker.invoked(origin);

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.invoked.len(), 1);
        assert!(event.invoked.contains(&origin));
    }

    #[test]
    fn tracks_both_invocations() {
        let mut tracker = MigrationOpTracker::new(
            "flag-key".into(),
            Some(basic_flag("flag-key")),
            ContextBuilder::new("user")
                .build()
                .expect("failed to build context"),
            Detail {
                value: Some(Stage::Live),
                variation_index: Some(1),
                reason: Reason::Fallthrough {
                    in_experiment: false,
                },
            },
            Stage::Live,
        );
        tracker.operation(Operation::Read);
        tracker.invoked(Origin::Old);
        tracker.invoked(Origin::New);

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.invoked.len(), 2);
        assert!(event.invoked.contains(&Origin::Old));
        assert!(event.invoked.contains(&Origin::New));
    }

    #[test_case(false)]
    #[test_case(true)]
    fn tracks_consistency(expectation: bool) {
        let mut tracker = minimal_tracker();
        tracker.operation(Operation::Read);
        tracker.consistent(|| expectation);

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.consistency_check, Some(expectation));
    }

    #[test_case(Origin::Old)]
    #[test_case(Origin::New)]
    fn track_errors_individually(origin: Origin) {
        let mut tracker = minimal_tracker();
        tracker.error(origin);

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.errors.len(), 1);
        assert!(event.errors.contains(&origin));
    }

    #[test]
    fn tracks_both_errors() {
        let mut tracker = minimal_tracker();
        tracker.error(Origin::Old);
        tracker.error(Origin::New);

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.errors.len(), 2);
        assert!(event.errors.contains(&Origin::Old));
        assert!(event.errors.contains(&Origin::New));
    }

    #[test_case(Origin::Old)]
    #[test_case(Origin::New)]
    fn track_latencies_individually(origin: Origin) {
        let mut tracker = minimal_tracker();
        tracker.latency(origin, std::time::Duration::from_millis(100));

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.latency.len(), 1);
        assert_eq!(
            event.latency.get(&origin),
            Some(&std::time::Duration::from_millis(100))
        );
    }

    #[test]
    fn track_both_latencies() {
        let mut tracker = minimal_tracker();
        tracker.latency(Origin::Old, std::time::Duration::from_millis(100));
        tracker.latency(Origin::New, std::time::Duration::from_millis(200));

        let event = tracker.build().expect("failed to build event");
        assert_eq!(event.latency.len(), 2);
        assert_eq!(
            event.latency.get(&Origin::Old),
            Some(&std::time::Duration::from_millis(100))
        );
        assert_eq!(
            event.latency.get(&Origin::New),
            Some(&std::time::Duration::from_millis(200))
        );
    }

    #[test]
    fn fails_without_calling_invocations() {
        let mut tracker = MigrationOpTracker::new(
            "flag-key".into(),
            Some(basic_flag("flag-key")),
            ContextBuilder::new("user")
                .build()
                .expect("failed to build context"),
            Detail {
                value: Some(Stage::Live),
                variation_index: Some(1),
                reason: Reason::Fallthrough {
                    in_experiment: false,
                },
            },
            Stage::Live,
        );
        tracker.operation(Operation::Read);

        let failure = tracker
            .build()
            .expect_err("tracker should have failed to build event");

        assert_eq!(failure, "no origins were invoked");
    }

    #[test]
    fn fails_without_operation() {
        let mut tracker = MigrationOpTracker::new(
            "flag-key".into(),
            Some(basic_flag("flag-key")),
            ContextBuilder::new("user")
                .build()
                .expect("failed to build context"),
            Detail {
                value: Some(Stage::Live),
                variation_index: Some(1),
                reason: Reason::Fallthrough {
                    in_experiment: false,
                },
            },
            Stage::Live,
        );
        tracker.invoked(Origin::Old);
        tracker.invoked(Origin::New);

        let failure = tracker
            .build()
            .expect_err("tracker should have failed to build event");

        assert_eq!(failure, "operation not provided");
    }
}
