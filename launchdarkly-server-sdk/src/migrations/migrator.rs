// TODO: Remove this when subsequent PRs have added the required implementations.
#![allow(dead_code)]

use std::sync::Mutex;
use std::time::Instant;
use std::{sync::Arc, thread};

use launchdarkly_server_sdk_evaluation::Context;

use crate::{Client, ExecutionOrder, MigrationOpTracker, Operation, Origin, Stage};

/// An internally used struct to represent the result of a migration operation along with the
/// origin it was executed against.
pub struct MigrationOriginResult {
    origin: Origin,
    result: MigrationResult,
}

/// MigrationResult represents the result of a migration operation. If the operation was
/// successful, the result will contain a pair of values representing the result of the operation
/// and the origin it was executed against. If the operation failed, the result will contain an
/// error.
type MigrationResult = Result<serde_json::Value, String>;

/// A write result contains the operation results against both the authoritative and
/// non-authoritative origins.
///
/// Authoritative writes are always executed first. In the event of a failure, the
/// non-authoritative write will not be executed, resulting in a `None` value in the final
/// MigrationWriteResult.
pub struct MigrationWriteResult {
    authoritative: MigrationOriginResult,
    nonauthoritative: Option<MigrationOriginResult>,
}

// MigrationComparisonFn is used to compare the results of two migration operations. If the
// provided results are equal, this method will return true and false otherwise.
type MigrationComparisonFn = fn(&serde_json::Value, &serde_json::Value) -> bool;

// MigrationImplFn represents the user defined migration operation function. This method is
// expected to return a meaningful value if the function succeeds, and an error otherwise.
type MigrationImplFn = Arc<dyn Fn(&serde_json::Value) -> MigrationResult + Send + Sync>;

struct MigrationConfig {
    old: MigrationImplFn,
    new: MigrationImplFn,
    compare: Option<MigrationComparisonFn>,
}

/// Migrator represents the interface through which migration support is executed.
pub trait Migrator {
    /// read uses the provided flag key and context to execute a migration-backed read operation.
    fn read(
        &self,
        key: String,
        context: Context,
        default_stage: Stage,
        payload: serde_json::Value,
    ) -> MigrationOriginResult;

    /// write uses the provided flag key and context to execute a migration-backed write operation.
    fn write(
        &self,
        key: String,
        context: Context,
        default_stage: Stage,
        payload: serde_json::Value,
    ) -> MigrationWriteResult;
}

/// The migration builder is used to configure and construct an instance of a [Migrator]. This
/// migrator can be used to perform LaunchDarkly assisted technology migrations through the use of
/// migration-based feature flags.
pub struct MigratorBuilder {
    client: Arc<Client>,
    read_execution_order: ExecutionOrder,
    measure_latency: bool,
    measure_errors: bool,

    read_config: Option<MigrationConfig>,
    write_config: Option<MigrationConfig>,
}

impl MigratorBuilder {
    /// Create a new migrator builder instance with the provided client.
    pub fn new(client: Arc<Client>) -> Self {
        MigratorBuilder {
            client,
            read_execution_order: ExecutionOrder::Parallel,
            measure_latency: true,
            measure_errors: true,
            read_config: None,
            write_config: None,
        }
    }

    /// The read execution order influences the parallelism and execution order for read operations
    /// involving multiple origins.
    pub fn read_execution_order(mut self, order: ExecutionOrder) -> Self {
        self.read_execution_order = order;
        self
    }

    /// Enable or disable latency tracking for migration operations. This latency information can
    /// be sent upstream to LaunchDarkly to enhance migration visibility.
    pub fn track_latency(mut self, measure: bool) -> Self {
        self.measure_latency = measure;
        self
    }

    /// Enable or disable error tracking for migration operations. This error information can be
    /// sent upstream to LaunchDarkly to enhance migration visibility.
    pub fn track_errors(mut self, measure: bool) -> Self {
        self.measure_errors = measure;
        self
    }

    /// Read can be used to configure the migration-read behavior of the resulting
    /// [Migrator] instance.
    ///
    /// Users are required to provide two different read methods -- one to read from the old migration origin, and one
    /// to read from the new origin. Additionally, users can opt-in to consistency tracking by providing a
    /// comparison function.
    ///
    /// Depending on the migration stage, one or both of these read methods may be called.
    pub fn read(
        mut self,
        old: MigrationImplFn,
        new: MigrationImplFn,
        compare: Option<MigrationComparisonFn>,
    ) -> Self {
        self.read_config = Some(MigrationConfig { old, new, compare });
        self
    }

    /// Write can be used to configure the migration-write behavior of the resulting
    /// [Migrations::Migrator] instance.
    ///
    /// Users are required to provide two different write methods -- one to write to the old
    /// migration origin, and one to write to the new origin. Not every stage requires
    ///
    /// Depending on the migration stage, one or both of these write methods may be called.
    pub fn write(mut self, old: MigrationImplFn, new: MigrationImplFn) -> Self {
        self.write_config = Some(MigrationConfig {
            old,
            new,
            compare: None,
        });
        self
    }

    // TODO: Do we make this a real error type?

    /// Build constructs a [Migrations::Migrator] instance to support migration-based reads and
    /// writes. A string describing any failure conditions will be returned if the build fails.
    pub fn build(self) -> Result<Box<dyn Migrator>, String> {
        let read_config = self.read_config.ok_or("read configuration not provided")?;
        let write_config = self
            .write_config
            .ok_or("write configuration not provided")?;

        Ok(Box::new(MigratorImpl::new(
            self.client,
            self.read_execution_order,
            self.measure_latency,
            self.measure_errors,
            read_config,
            write_config,
        )))
    }
}

struct MigratorImpl {
    client: Arc<Client>,
    read_execution_order: ExecutionOrder,
    measure_latency: bool,
    measure_errors: bool,
    read_config: MigrationConfig,
    write_config: MigrationConfig,
}

impl MigratorImpl {
    fn new(
        client: Arc<Client>,
        read_execution_order: ExecutionOrder,
        measure_latency: bool,
        measure_errors: bool,
        read_config: MigrationConfig,
        write_config: MigrationConfig,
    ) -> Self {
        MigratorImpl {
            client,
            read_execution_order,
            measure_latency,
            measure_errors,
            read_config,
            write_config,
        }
    }
}

impl Migrator for MigratorImpl {
    fn read(
        &self,
        key: String,
        context: Context,
        default_stage: Stage,
        payload: serde_json::Value,
    ) -> MigrationOriginResult {
        let (stage, mut tracker) = self
            .client
            .migration_variation(&context, &key, default_stage);
        tracker.operation(Operation::Read);

        let tracker = Arc::new(Mutex::new(tracker));

        let mut old = Executor {
            origin: Origin::Old,
            function: &self.read_config.old,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload: payload.clone(),
        };
        let mut new = Executor {
            origin: Origin::New,
            function: &self.read_config.new,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload,
        };

        let result = match stage {
            Stage::Off => old.run(),
            Stage::DualWrite => old.run(),
            Stage::Shadow => Executor::read_both(
                old,
                new,
                self.read_config.compare,
                self.read_execution_order,
                tracker.clone(),
            ),
            Stage::Live => Executor::read_both(
                new,
                old,
                self.read_config.compare,
                self.read_execution_order,
                tracker.clone(),
            ),
            Stage::Rampdown => new.run(),
            Stage::Complete => new.run(),
        };

        self.client.track_migration_op(tracker);

        result
    }

    fn write(
        &self,
        key: String,
        context: Context,
        default_stage: Stage,
        payload: serde_json::Value,
    ) -> MigrationWriteResult {
        let (stage, mut tracker) = self
            .client
            .migration_variation(&context, &key, default_stage);
        tracker.operation(Operation::Write);

        let tracker = Arc::new(Mutex::new(tracker));

        let mut old = Executor {
            origin: Origin::Old,
            function: &self.write_config.old,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload: payload.clone(),
        };
        let mut new = Executor {
            origin: Origin::New,
            function: &self.write_config.new,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload,
        };

        let result = match stage {
            Stage::Off => MigrationWriteResult {
                authoritative: old.run(),
                nonauthoritative: None,
            },
            Stage::DualWrite => Executor::write_both(old, new),
            Stage::Shadow => Executor::write_both(old, new),
            Stage::Live => Executor::write_both(new, old),
            Stage::Rampdown => Executor::write_both(new, old),
            Stage::Complete => MigrationWriteResult {
                authoritative: new.run(),
                nonauthoritative: None,
            },
        };

        self.client.track_migration_op(tracker);

        result
    }
}

struct Executor<'a> {
    origin: Origin,
    function: &'a MigrationImplFn,
    tracker: Arc<Mutex<MigrationOpTracker>>,
    measure_latency: bool,
    measure_errors: bool,
    payload: serde_json::Value,
}

impl Executor<'_> {
    fn read_both(
        mut authoritative: Executor,
        mut nonauthoritative: Executor,
        compare: Option<MigrationComparisonFn>,
        execution_order: ExecutionOrder,
        tracker: Arc<Mutex<MigrationOpTracker>>,
    ) -> MigrationOriginResult {
        let authoritative_result: MigrationOriginResult;
        let nonauthoritative_result: MigrationOriginResult;

        match execution_order {
            ExecutionOrder::Parallel => {
                let result = thread::scope(|s| {
                    let authoritative_handler = s.spawn(|| authoritative.run());
                    let nonauthoritative_handler = s.spawn(|| nonauthoritative.run());

                    (
                        authoritative_handler.join().unwrap(),
                        nonauthoritative_handler.join().unwrap(),
                    )
                });

                authoritative_result = result.0;
                nonauthoritative_result = result.1;
            }
            // TODO: Add a sampler.sample(2) style call
            ExecutionOrder::Random => {
                nonauthoritative_result = nonauthoritative.run();
                authoritative_result = authoritative.run();
            }
            ExecutionOrder::Serial => {
                authoritative_result = authoritative.run();
                nonauthoritative_result = nonauthoritative.run();
            }
        };

        if let Some(compare) = compare {
            if let (Ok(authoritative), Ok(nonauthoritative)) = (
                &authoritative_result.result,
                &nonauthoritative_result.result,
            ) {
                if let Ok(mut tracker) = tracker.lock() {
                    tracker.consistent(|| compare(authoritative, nonauthoritative));
                } else {
                    error!("Failed to acquire tracker lock. Cannot track consistency.");
                }
            }
        }

        authoritative_result
    }

    fn write_both(
        mut authoritative: Executor,
        mut nonauthoritative: Executor,
    ) -> MigrationWriteResult {
        let authoritative_result = authoritative.run();

        if authoritative_result.result.is_err() {
            return MigrationWriteResult {
                authoritative: authoritative_result,
                nonauthoritative: None,
            };
        }

        let nonauthoritative_result = nonauthoritative.run();

        MigrationWriteResult {
            authoritative: authoritative_result,
            nonauthoritative: Some(nonauthoritative_result),
        }
    }

    fn run(&mut self) -> MigrationOriginResult {
        let start = Instant::now();
        let result = (self.function)(&self.payload);
        let elapsed = start.elapsed();

        let result = match self.tracker.lock() {
            Ok(mut tracker) => {
                if self.measure_latency {
                    tracker.latency(self.origin, elapsed);
                }

                if self.measure_errors && result.is_err() {
                    tracker.error(self.origin);
                }

                tracker.invoked(self.origin);

                result
            }
            Err(_) => Err("Failed to acquire lock".into()),
        };

        MigrationOriginResult {
            origin: self.origin,
            result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{mpsc, Arc},
        time::{Duration, Instant},
    };

    use crate::{
        migrations::migrator::MigratorBuilder, Client, ConfigBuilder, ExecutionOrder, Stage,
    };
    use launchdarkly_server_sdk_evaluation::ContextBuilder;
    use test_case::test_case;

    fn default_builder(client: Arc<Client>) -> MigratorBuilder {
        MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .read(
                Arc::new(|_| Ok(serde_json::Value::Null)),
                Arc::new(|_| Ok(serde_json::Value::Null)),
                Some(|_, _| true),
            )
            .write(
                Arc::new(|_| Ok(serde_json::Value::Null)),
                Arc::new(|_| Ok(serde_json::Value::Null)),
            )
    }

    #[test]
    fn can_build_successfully() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        let migrator = default_builder(client).build();

        assert!(migrator.is_ok());
    }

    #[test]
    fn read_passes_payload_through() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let migrator = default_builder(client)
            .read_execution_order(ExecutionOrder::Serial)
            .read(
                Arc::new(move |payload| {
                    old_sender.send(payload.clone()).unwrap();
                    Ok(serde_json::Value::Null)
                }),
                Arc::new(move |payload| {
                    new_sender.send(payload.clone()).unwrap();
                    Ok(serde_json::Value::Null)
                }),
                None,
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator.read(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            crate::Stage::Shadow,
            "payload".into(),
        );

        let old_payload = receiver.recv().unwrap();
        let new_payload = receiver.recv().unwrap();

        assert_eq!(old_payload, "payload");
        assert_eq!(new_payload, "payload");
    }

    #[test]
    fn write_passes_payload_through() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let migrator = default_builder(client)
            .write(
                Arc::new(move |payload| {
                    old_sender.send(payload.clone()).unwrap();
                    Ok(serde_json::Value::Null)
                }),
                Arc::new(move |payload| {
                    new_sender.send(payload.clone()).unwrap();
                    Ok(serde_json::Value::Null)
                }),
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator.write(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            crate::Stage::Shadow,
            "payload".into(),
        );

        let old_payload = receiver.recv().unwrap();
        let new_payload = receiver.recv().unwrap();

        assert_eq!(old_payload, "payload");
        assert_eq!(new_payload, "payload");
    }

    #[test_case(Stage::Off, true, false)]
    #[test_case(Stage::DualWrite, true, false)]
    #[test_case(Stage::Shadow, true, true)]
    #[test_case(Stage::Live, true, true)]
    #[test_case(Stage::Rampdown, false, true)]
    #[test_case(Stage::Complete, false, true)]
    fn read_handles_correct_origin(stage: Stage, expected_old: bool, expected_new: bool) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let migrator = default_builder(client)
            .read_execution_order(ExecutionOrder::Serial)
            .read(
                Arc::new(move |_| {
                    old_sender.send("old").unwrap();
                    Ok(serde_json::Value::Null)
                }),
                Arc::new(move |_| {
                    new_sender.send("new").unwrap();
                    Ok(serde_json::Value::Null)
                }),
                None,
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator.read(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            stage,
            "payload".into(),
        );

        let payloads = receiver.try_iter().collect::<Vec<_>>();

        if expected_old {
            assert!(payloads.contains(&"old"));
        } else {
            assert!(!payloads.contains(&"old"));
        }

        if expected_new {
            assert!(payloads.contains(&"new"));
        } else {
            assert!(!payloads.contains(&"new"));
        }
    }

    #[test]
    fn read_handles_parallel_execution() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let migrator = default_builder(client)
            .read_execution_order(ExecutionOrder::Parallel)
            .read(
                Arc::new(|_| {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    Ok(serde_json::Value::Null)
                }),
                Arc::new(|_| {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    Ok(serde_json::Value::Null)
                }),
                None,
            )
            .build()
            .expect("migrator failed to build");

        let start = Instant::now();
        let _result = migrator.read(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            crate::Stage::Shadow,
            "payload".into(),
        );
        let elapsed = start.elapsed();
        println!("Elapsed: {:?}", elapsed);
        assert!(elapsed < Duration::from_millis(500));
    }

    #[test_case(ExecutionOrder::Serial)]
    #[test_case(ExecutionOrder::Random)]
    fn read_handles_nonparallel_execution(execution_order: ExecutionOrder) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let migrator = default_builder(client)
            .read_execution_order(execution_order)
            .read(
                Arc::new(|_| {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    Ok(serde_json::Value::Null)
                }),
                Arc::new(|_| {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    Ok(serde_json::Value::Null)
                }),
                None,
            )
            .build()
            .expect("migrator failed to build");

        let start = Instant::now();
        let _result = migrator.read(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            crate::Stage::Shadow,
            "payload".into(),
        );
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(500));
    }

    #[test_case(Stage::Off, true, false)]
    #[test_case(Stage::DualWrite, true, true)]
    #[test_case(Stage::Shadow, true, true)]
    #[test_case(Stage::Live, true, true)]
    #[test_case(Stage::Rampdown, true, true)]
    #[test_case(Stage::Complete, false, true)]
    fn write_handles_correct_origin(stage: Stage, expected_old: bool, expected_new: bool) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let migrator = default_builder(client)
            .write(
                Arc::new(move |_| {
                    old_sender.send("old").unwrap();
                    Ok(serde_json::Value::Null)
                }),
                Arc::new(move |_| {
                    new_sender.send("new").unwrap();
                    Ok(serde_json::Value::Null)
                }),
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator.write(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            stage,
            "payload".into(),
        );

        let payloads = receiver.try_iter().collect::<Vec<_>>();

        if expected_old {
            assert!(payloads.contains(&"old"));
        } else {
            assert!(!payloads.contains(&"old"));
        }

        if expected_new {
            assert!(payloads.contains(&"new"));
        } else {
            assert!(!payloads.contains(&"new"));
        }
    }

    // #[test_case(Stage::Off, true, false)]
    #[test_case(Stage::DualWrite, true, false)]
    #[test_case(Stage::Shadow, true, false)]
    #[test_case(Stage::Live, false, true)]
    #[test_case(Stage::Rampdown, false, true)]
    // #[test_case(Stage::Complete, false, true)]
    fn write_stops_if_authoritative_fails(stage: Stage, expected_old: bool, expected_new: bool) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let migrator = default_builder(client)
            .write(
                Arc::new(move |_| {
                    old_sender.send("old").unwrap();
                    Err("error".into())
                }),
                Arc::new(move |_| {
                    new_sender.send("new").unwrap();
                    Err("error".into())
                }),
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator.write(
            "migration-key".into(),
            ContextBuilder::new("user-key")
                .build()
                .expect("context failed to build"),
            stage,
            "payload".into(),
        );

        let payloads = receiver.try_iter().collect::<Vec<_>>();

        if expected_old {
            assert!(payloads.contains(&"old"));
        } else {
            assert!(!payloads.contains(&"old"));
        }

        if expected_new {
            assert!(payloads.contains(&"new"));
        } else {
            assert!(!payloads.contains(&"new"));
        }
    }

    #[test_case(ExecutionOrder::Serial)]
    #[test_case(ExecutionOrder::Random)]
    #[test_case(ExecutionOrder::Parallel)]
    fn can_modify_execution_order(execution_order: ExecutionOrder) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        let migrator = default_builder(client)
            .read_execution_order(execution_order)
            .build();

        assert!(migrator.is_ok());
    }

    #[test]
    fn build_will_fail_without_read_config() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        let migrator = MigratorBuilder::new(client)
            .write(
                Arc::new(|_| Ok(serde_json::Value::Null)),
                Arc::new(|_| Ok(serde_json::Value::Null)),
            )
            .build();

        assert!(migrator.is_err());
        assert_eq!(migrator.err().unwrap(), "read configuration not provided");
    }

    #[test]
    fn build_will_fail_without_write_config() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        let migrator = MigratorBuilder::new(client)
            .read(
                Arc::new(|_| Ok(serde_json::Value::Null)),
                Arc::new(|_| Ok(serde_json::Value::Null)),
                Some(|_, _| true),
            )
            .build();

        assert!(migrator.is_err());
        assert_eq!(migrator.err().unwrap(), "write configuration not provided");
    }
}
