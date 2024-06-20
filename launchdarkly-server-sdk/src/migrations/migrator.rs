use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use futures::future::join_all;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use launchdarkly_server_sdk_evaluation::Context;
use rand::thread_rng;
use serde::Serialize;

use crate::sampler::Sampler;
use crate::sampler::ThreadRngSampler;
use crate::{Client, ExecutionOrder, MigrationOpTracker, Operation, Origin, Stage};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// An internally used struct to represent the result of a migration operation along with the
/// origin it was executed against.
pub struct MigrationOriginResult<T> {
    pub origin: Origin,
    pub result: MigrationResult<T>,
}

/// MigrationResult represents the result of a migration operation. If the operation was
/// successful, the result will contain a pair of values representing the result of the operation
/// and the origin it was executed against. If the operation failed, the result will contain an
/// error.
type MigrationResult<T> = Result<T, String>;

/// A write result contains the operation results against both the authoritative and
/// non-authoritative origins.
///
/// Authoritative writes are always executed first. In the event of a failure, the
/// non-authoritative write will not be executed, resulting in a `None` value in the final
/// MigrationWriteResult.
pub struct MigrationWriteResult<T> {
    pub authoritative: MigrationOriginResult<T>,
    pub nonauthoritative: Option<MigrationOriginResult<T>>,
}

// MigrationComparisonFn is used to compare the results of two migration operations. If the
// provided results are equal, this method will return true and false otherwise.
type MigrationComparisonFn<T> = fn(&T, &T) -> bool;

struct MigrationConfig<P, T, FO, FN>
where
    P: Send + Sync,
    T: Send + Sync,
    FO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    old: FO,
    new: FN,
    compare: Option<MigrationComparisonFn<T>>,

    _p: std::marker::PhantomData<P>,
}

/// The migration builder is used to configure and construct an instance of a [Migrator]. This
/// migrator can be used to perform LaunchDarkly assisted technology migrations through the use of
/// migration-based feature flags.
pub struct MigratorBuilder<P, T, FRO, FRN, FWO, FWN>
where
    P: Send + Sync,
    T: Send + Sync,
    FRO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FRN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    client: Arc<Client>,
    read_execution_order: ExecutionOrder,
    measure_latency: bool,
    measure_errors: bool,

    read_config: Option<MigrationConfig<P, T, FRO, FRN>>,
    write_config: Option<MigrationConfig<P, T, FWO, FWN>>,
}

impl<P, T, FRO, FRN, FWO, FWN> MigratorBuilder<P, T, FRO, FRN, FWO, FWN>
where
    P: Send + Sync,
    T: Send + Sync,
    FRO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FRN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    /// Create a new migrator builder instance with the provided client.
    pub fn new(client: Arc<Client>) -> Self {
        MigratorBuilder {
            client,
            read_execution_order: ExecutionOrder::Concurrent,
            measure_latency: true,
            measure_errors: true,
            read_config: None,
            write_config: None,
        }
    }

    /// The read execution order influences the concurrency and execution order for read operations
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
    pub fn read(mut self, old: FRO, new: FRN, compare: Option<MigrationComparisonFn<T>>) -> Self {
        self.read_config = Some(MigrationConfig {
            old,
            new,
            compare,
            _p: std::marker::PhantomData,
        });
        self
    }

    /// Write can be used to configure the migration-write behavior of the resulting
    /// [crate::Migrator] instance.
    ///
    /// Users are required to provide two different write methods -- one to write to the old
    /// migration origin, and one to write to the new origin. Not every stage requires
    ///
    /// Depending on the migration stage, one or both of these write methods may be called.
    pub fn write(mut self, old: FWO, new: FWN) -> Self {
        self.write_config = Some(MigrationConfig {
            old,
            new,
            compare: None,
            _p: std::marker::PhantomData,
        });
        self
    }

    /// Build constructs a [crate::Migrator] instance to support migration-based reads and
    /// writes. A string describing any failure conditions will be returned if the build fails.
    pub fn build(self) -> Result<Migrator<P, T, FRO, FRN, FWO, FWN>, String> {
        let read_config = self.read_config.ok_or("read configuration not provided")?;
        let write_config = self
            .write_config
            .ok_or("write configuration not provided")?;

        Ok(Migrator::new(
            self.client,
            self.read_execution_order,
            self.measure_latency,
            self.measure_errors,
            read_config,
            write_config,
        ))
    }
}

/// The migrator is the primary interface for executing migration operations. It is configured
/// through the [MigratorBuilder] and can be used to perform LaunchDarkly assisted technology
/// migrations through the use of migration-based feature flags.
pub struct Migrator<P, T, FRO, FRN, FWO, FWN>
where
    P: Send + Sync,
    T: Send + Sync,
    FRO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FRN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    client: Arc<Client>,
    read_execution_order: ExecutionOrder,
    measure_latency: bool,
    measure_errors: bool,
    read_config: MigrationConfig<P, T, FRO, FRN>,
    write_config: MigrationConfig<P, T, FWO, FWN>,
    sampler: Box<dyn Sampler>,
}

impl<P, T, FRO, FRN, FWO, FWN> Migrator<P, T, FRO, FRN, FWO, FWN>
where
    P: Send + Sync,
    T: Send + Sync,
    FRO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FRN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWO: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FWN: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    fn new(
        client: Arc<Client>,
        read_execution_order: ExecutionOrder,
        measure_latency: bool,
        measure_errors: bool,
        read_config: MigrationConfig<P, T, FRO, FRN>,
        write_config: MigrationConfig<P, T, FWO, FWN>,
    ) -> Self {
        Migrator {
            client,
            read_execution_order,
            measure_latency,
            measure_errors,
            read_config,
            write_config,
            sampler: Box::new(ThreadRngSampler::new(thread_rng())),
        }
    }

    /// Uses the provided flag key and context to execute a migration-backed read operation.
    pub async fn read(
        &mut self,
        context: &Context,
        flag_key: String,
        default_stage: Stage,
        payload: P,
    ) -> MigrationOriginResult<T> {
        let (stage, tracker) = self
            .client
            .migration_variation(context, &flag_key, default_stage);

        if let Ok(mut tracker) = tracker.lock() {
            tracker.operation(Operation::Read);
        } else {
            error!("Failed to acquire tracker lock. Cannot track migration write.");
        }

        let mut old = Executor {
            origin: Origin::Old,
            function: &self.read_config.old,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload: &payload,
        };
        let mut new = Executor {
            origin: Origin::New,
            function: &self.read_config.new,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload: &payload,
        };

        let result = match stage {
            Stage::Off => old.run().await,
            Stage::DualWrite => old.run().await,
            Stage::Shadow => {
                read_both(
                    old,
                    new,
                    self.read_config.compare,
                    self.read_execution_order,
                    tracker.clone(),
                    self.sampler.as_mut(),
                )
                .await
            }
            Stage::Live => {
                read_both(
                    new,
                    old,
                    self.read_config.compare,
                    self.read_execution_order,
                    tracker.clone(),
                    self.sampler.as_mut(),
                )
                .await
            }
            Stage::Rampdown => new.run().await,
            Stage::Complete => new.run().await,
        };

        self.client.track_migration_op(tracker);

        result
    }

    /// Uses the provided flag key and context to execute a migration-backed write operation.
    pub async fn write(
        &mut self,
        context: &Context,
        flag_key: String,
        default_stage: Stage,
        payload: P,
    ) -> MigrationWriteResult<T> {
        let (stage, tracker) = self
            .client
            .migration_variation(context, &flag_key, default_stage);

        if let Ok(mut tracker) = tracker.lock() {
            tracker.operation(Operation::Write);
        } else {
            error!("Failed to acquire tracker lock. Cannot track migration write.");
        }

        let mut old = Executor {
            origin: Origin::Old,
            function: &self.write_config.old,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload: &payload,
        };
        let mut new = Executor {
            origin: Origin::New,
            function: &self.write_config.new,
            tracker: tracker.clone(),
            measure_latency: self.measure_latency,
            measure_errors: self.measure_errors,
            payload: &payload,
        };

        let result = match stage {
            Stage::Off => MigrationWriteResult {
                authoritative: old.run().await,
                nonauthoritative: None,
            },
            Stage::DualWrite => write_both(old, new).await,
            Stage::Shadow => write_both(old, new).await,
            Stage::Live => write_both(new, old).await,
            Stage::Rampdown => write_both(new, old).await,
            Stage::Complete => MigrationWriteResult {
                authoritative: new.run().await,
                nonauthoritative: None,
            },
        };

        self.client.track_migration_op(tracker);

        result
    }
}

async fn read_both<P, T, FA, FB>(
    mut authoritative: Executor<'_, P, T, FA>,
    mut nonauthoritative: Executor<'_, P, T, FB>,
    compare: Option<MigrationComparisonFn<T>>,
    execution_order: ExecutionOrder,
    tracker: Arc<Mutex<MigrationOpTracker>>,
    sampler: &mut dyn Sampler,
) -> MigrationOriginResult<T>
where
    P: Send + Sync,
    T: Send + Sync,
    FA: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FB: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    let authoritative_result: MigrationOriginResult<T>;
    let nonauthoritative_result: MigrationOriginResult<T>;

    match execution_order {
        ExecutionOrder::Concurrent => {
            let auth_handle = authoritative.run().boxed();
            let nonauth_handle = nonauthoritative.run().boxed();
            let handles = vec![auth_handle, nonauth_handle];

            let mut results = join_all(handles).await;

            // Note that we are doing this is the reverse order of the handles since we are
            // popping the results off the end of the vector.
            nonauthoritative_result = results.pop().unwrap_or_else(|| MigrationOriginResult {
                origin: nonauthoritative.origin,
                result: Err("Failed to execute non-authoritative read".into()),
            });

            authoritative_result = results.pop().unwrap_or_else(|| MigrationOriginResult {
                origin: authoritative.origin,
                result: Err("Failed to execute authoritative read".into()),
            });
        }
        ExecutionOrder::Random if sampler.sample(2) => {
            nonauthoritative_result = nonauthoritative.run().await;
            authoritative_result = authoritative.run().await;
        }
        _ => {
            authoritative_result = authoritative.run().await;
            nonauthoritative_result = nonauthoritative.run().await;
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

async fn write_both<P, T, FA, FB>(
    mut authoritative: Executor<'_, P, T, FA>,
    mut nonauthoritative: Executor<'_, P, T, FB>,
) -> MigrationWriteResult<T>
where
    P: Send + Sync,
    T: Send + Sync,
    FA: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
    FB: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    let authoritative_result = authoritative.run().await;

    if authoritative_result.result.is_err() {
        return MigrationWriteResult {
            authoritative: authoritative_result,
            nonauthoritative: None,
        };
    }

    let nonauthoritative_result = nonauthoritative.run().await;

    MigrationWriteResult {
        authoritative: authoritative_result,
        nonauthoritative: Some(nonauthoritative_result),
    }
}

struct Executor<'a, P, T, F>
where
    P: Send + Sync,
    T: Send + Sync,
    F: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    origin: Origin,
    function: &'a F,
    tracker: Arc<Mutex<MigrationOpTracker>>,
    measure_latency: bool,
    measure_errors: bool,
    payload: &'a P,
}

impl<'a, P, T, F> Executor<'a, P, T, F>
where
    P: Send + Sync,
    T: Send + Sync,
    F: Fn(&P) -> BoxFuture<MigrationResult<T>> + Sync + Send,
{
    async fn run(&mut self) -> MigrationOriginResult<T> {
        let start = Instant::now();
        let result = (self.function)(self.payload).await;
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
    use futures::future::FutureExt;
    use launchdarkly_server_sdk_evaluation::ContextBuilder;
    use test_case::test_case;

    #[test]
    fn can_build_successfully() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        let migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .read(
                |_: &u32| async move { Ok(()) }.boxed(),
                |_: &u32| async move { Ok(()) }.boxed(),
                Some(|_, _| true),
            )
            .write(
                |_: &u32| async move { Ok(()) }.boxed(),
                |_: &u32| async move { Ok(()) }.boxed(),
            )
            .build();

        assert!(migrator.is_ok());
    }

    #[tokio::test]
    async fn read_passes_payload_through() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .write(
                |_| async move { Ok(0) }.boxed(),
                |_| async move { Ok(0) }.boxed(),
            )
            .read_execution_order(ExecutionOrder::Serial)
            .read(
                move |&payload| {
                    let old_sender = old_sender.clone();
                    async move {
                        old_sender.send(payload).unwrap();
                        Ok(0)
                    }
                    .boxed()
                },
                move |&payload| {
                    let new_sender = new_sender.clone();
                    async move {
                        new_sender.send(payload).unwrap();
                        Ok(0)
                    }
                    .boxed()
                },
                None,
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator
            .read(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                crate::Stage::Shadow,
                1,
            )
            .await;

        let old_payload = receiver.recv().unwrap();
        let new_payload = receiver.recv().unwrap();

        assert_eq!(old_payload, 1);
        assert_eq!(new_payload, 1);
    }

    #[tokio::test]
    async fn write_passes_payload_through() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .read(
                |_| async move { Ok(0) }.boxed(),
                |_| async move { Ok(0) }.boxed(),
                Some(|_, _| true),
            )
            .write(
                move |&payload| {
                    let old_sender = old_sender.clone();
                    async move {
                        old_sender.send(payload).unwrap();
                        Ok(0)
                    }
                    .boxed()
                },
                move |&payload| {
                    let new_sender = new_sender.clone();
                    async move {
                        new_sender.send(payload).unwrap();
                        Ok(0)
                    }
                    .boxed()
                },
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator
            .write(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                crate::Stage::Shadow,
                1,
            )
            .await;

        let old_payload = receiver.recv().unwrap();
        let new_payload = receiver.recv().unwrap();

        assert_eq!(old_payload, 1);
        assert_eq!(new_payload, 1);
    }

    #[tokio::test]
    async fn read_handles_correct_origin() {
        read_handles_correct_origin_driver(Stage::Off, true, false).await;
        read_handles_correct_origin_driver(Stage::DualWrite, true, false).await;
        read_handles_correct_origin_driver(Stage::Shadow, true, true).await;
        read_handles_correct_origin_driver(Stage::Live, true, true).await;
        read_handles_correct_origin_driver(Stage::Rampdown, false, true).await;
        read_handles_correct_origin_driver(Stage::Complete, false, true).await;
    }

    async fn read_handles_correct_origin_driver(
        stage: Stage,
        expected_old: bool,
        expected_new: bool,
    ) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .write(
                |_| async move { Ok("write") }.boxed(),
                |_| async move { Ok("write") }.boxed(),
            )
            .read_execution_order(ExecutionOrder::Serial)
            .read(
                move |_| {
                    let old_sender = old_sender.clone();
                    async move {
                        old_sender.send("old").unwrap();
                        Ok("read")
                    }
                    .boxed()
                },
                move |_| {
                    let new_sender = new_sender.clone();
                    async move {
                        new_sender.send("new").unwrap();
                        Ok("read")
                    }
                    .boxed()
                },
                None,
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator
            .read(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                stage,
                "payload",
            )
            .await;

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

    #[tokio::test]
    async fn read_handles_concurrent_execution() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .write(
                |_| async move { Ok(()) }.boxed(),
                |_| async move { Ok(()) }.boxed(),
            )
            .read_execution_order(ExecutionOrder::Concurrent)
            .read(
                |_| {
                    async move {
                        async_std::task::sleep(std::time::Duration::from_millis(250)).await;
                        Ok(())
                    }
                    .boxed()
                },
                |_| {
                    async move {
                        async_std::task::sleep(std::time::Duration::from_millis(250)).await;
                        Ok(())
                    }
                    .boxed()
                },
                None,
            )
            .build()
            .expect("migrator failed to build");

        let start = Instant::now();
        let _result = migrator
            .read(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                crate::Stage::Shadow,
                (),
            )
            .await;
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn read_handles_nonconcurrent_execution() {
        read_handles_nonconcurrent_execution_driver(ExecutionOrder::Serial).await;
        read_handles_nonconcurrent_execution_driver(ExecutionOrder::Random).await;
    }

    async fn read_handles_nonconcurrent_execution_driver(execution_order: ExecutionOrder) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .write(
                |_| async move { Ok(()) }.boxed(),
                |_| async move { Ok(()) }.boxed(),
            )
            .read_execution_order(execution_order)
            .read(
                |_| {
                    async move {
                        std::thread::sleep(std::time::Duration::from_millis(250));
                        Ok(())
                    }
                    .boxed()
                },
                |_| {
                    async move {
                        std::thread::sleep(std::time::Duration::from_millis(250));
                        Ok(())
                    }
                    .boxed()
                },
                None,
            )
            .build()
            .expect("migrator failed to build");

        let start = Instant::now();
        let _result = migrator
            .read(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                crate::Stage::Shadow,
                (),
            )
            .await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(500));
    }

    #[tokio::test]
    async fn write_handles_correct_origin() {
        write_handles_correct_origin_driver(Stage::Off, true, false).await;
        write_handles_correct_origin_driver(Stage::DualWrite, true, true).await;
        write_handles_correct_origin_driver(Stage::Shadow, true, true).await;
        write_handles_correct_origin_driver(Stage::Live, true, true).await;
        write_handles_correct_origin_driver(Stage::Rampdown, true, true).await;
        write_handles_correct_origin_driver(Stage::Complete, false, true).await;
    }

    async fn write_handles_correct_origin_driver(
        stage: Stage,
        expected_old: bool,
        expected_new: bool,
    ) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .read(
                |_| async move { Ok(()) }.boxed(),
                |_| async move { Ok(()) }.boxed(),
                Some(|_, _| true),
            )
            .write(
                move |_| {
                    let old_sender = old_sender.clone();
                    async move {
                        old_sender.send("old").unwrap();
                        Ok(())
                    }
                    .boxed()
                },
                move |_| {
                    let new_sender = new_sender.clone();
                    async move {
                        new_sender.send("new").unwrap();
                        Ok(())
                    }
                    .boxed()
                },
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator
            .write(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                stage,
                (),
            )
            .await;

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

    #[tokio::test]
    async fn write_stops_if_authoritative_fails() {
        // doesn't write to new if old fails
        // write_stops_if_authoritative_fails_driver(Stage::Off, true, false).await;

        write_stops_if_authoritative_fails_driver(Stage::DualWrite, true, false).await;
        write_stops_if_authoritative_fails_driver(Stage::Shadow, true, false).await;
        write_stops_if_authoritative_fails_driver(Stage::Live, false, true).await;
        write_stops_if_authoritative_fails_driver(Stage::Rampdown, false, true).await;

        // doesn't write to old if new fails
        // write_stops_if_authoritative_fails_driver(Stage::Complete, false, true).await;
    }

    async fn write_stops_if_authoritative_fails_driver(
        stage: Stage,
        expected_old: bool,
        expected_new: bool,
    ) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        client.start_with_default_executor();

        let (sender, receiver) = mpsc::channel();
        let old_sender = sender.clone();
        let new_sender = sender.clone();
        let mut migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .read(
                |_| async move { Ok(()) }.boxed(),
                |_| async move { Ok(()) }.boxed(),
                Some(|_, _| true),
            )
            .write(
                move |_| {
                    let old_sender = old_sender.clone();
                    async move {
                        old_sender.send("old").unwrap();
                        Err("error".into())
                    }
                    .boxed()
                },
                move |_| {
                    let new_sender = new_sender.clone();
                    async move {
                        new_sender.send("new").unwrap();
                        Err("error".into())
                    }
                    .boxed()
                },
            )
            .build()
            .expect("migrator failed to build");

        let _result = migrator
            .write(
                &ContextBuilder::new("user-key")
                    .build()
                    .expect("context failed to build"),
                "migration-key".into(),
                stage,
                (),
            )
            .await;

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
    #[test_case(ExecutionOrder::Concurrent)]
    fn can_modify_execution_order(execution_order: ExecutionOrder) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Arc::new(Client::build(config).expect("client failed to build"));
        let migrator = MigratorBuilder::new(client)
            .track_latency(false)
            .track_errors(false)
            .read(
                |_: &u32| async move { Ok(()) }.boxed(),
                |_: &u32| async move { Ok(()) }.boxed(),
                Some(|_, _| true),
            )
            .write(
                |_: &u32| async move { Ok(()) }.boxed(),
                |_: &u32| async move { Ok(()) }.boxed(),
            )
            .read_execution_order(execution_order)
            .build();

        assert!(migrator.is_ok());
    }
}
