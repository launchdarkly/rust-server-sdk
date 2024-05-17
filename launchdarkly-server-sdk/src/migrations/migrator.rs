// TODO: Remove this when subsequent PRs have added the required implementations.
#![allow(dead_code)]

use launchdarkly_server_sdk_evaluation::Context;

use crate::{Client, ExecutionOrder, Origin, Stage};

/// An internally used struct to represent the result of a migration operation along with the
/// origin it was executed against.
pub struct MigrationResultPair {
    value: serde_json::Value,
    origin: Origin,
}

/// MigrationResult represents the result of a migration operation. If the operation was
/// successful, the result will contain a pair of values representing the result of the operation
/// and the origin it was executed against. If the operation failed, the result will contain an
/// error.
type MigrationResult = Result<MigrationResultPair, Box<dyn std::error::Error>>;

/// A write result contains the operation results against both the authoritative and
/// non-authoritative origins.
///
/// Authoritative writes are always executed first. In the event of a failure, the
/// non-authoritative write will not be executed, resulting in a `None` value in the final
/// MigrationWriteResult.
pub struct MigrationWriteResult {
    authoritative: MigrationResult,
    nonauthoritative: Option<MigrationResult>,
}

// MigrationComparisonFn is used to compare the results of two migration operations. If the
// provided results are equal, this method will return true and false otherwise.
type MigrationComparisonFn = fn(serde_json::Value, serde_json::Value) -> bool;

// MigrationImplFn represents the user defined migration operation function. This method is
// expected to return a meaningful value if the function succeeds, and an error otherwise.
type MigrationImplFn =
    fn(serde_json::Value) -> Result<serde_json::Value, Box<dyn std::error::Error>>;

struct MigrationConfig {
    old: MigrationImplFn,
    new: MigrationImplFn,
    compare: MigrationComparisonFn,
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
    ) -> MigrationResult;

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
    client: Client,
    read_execution_order: ExecutionOrder,
    measure_latency: bool,
    measure_errors: bool,

    read_config: Option<MigrationConfig>,
    write_config: Option<MigrationConfig>,
}

impl MigratorBuilder {
    /// Create a new migrator builder instance with the provided client.
    pub fn new(client: Client) -> Self {
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
        compare: MigrationComparisonFn,
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
    pub fn write(
        mut self,
        old: MigrationImplFn,
        new: MigrationImplFn,
        compare: MigrationComparisonFn,
    ) -> Self {
        self.write_config = Some(MigrationConfig { old, new, compare });
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
    client: Client,
    read_execution_order: ExecutionOrder,
    measure_latency: bool,
    measure_errors: bool,
    read_config: MigrationConfig,
    write_config: MigrationConfig,
}

impl MigratorImpl {
    fn new(
        client: Client,
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
        _key: String,
        _context: Context,
        _default_stage: Stage,
        _payload: serde_json::Value,
    ) -> MigrationResult {
        unimplemented!()
    }

    fn write(
        &self,
        _key: String,
        _context: Context,
        _default_stage: Stage,
        _payload: serde_json::Value,
    ) -> MigrationWriteResult {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{migrations::migrator::MigratorBuilder, Client, ConfigBuilder, ExecutionOrder};
    use test_case::test_case;

    #[test]
    fn test_can_build() {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Client::build(config).expect("client failed to build");
        let migrator = MigratorBuilder::new(client)
            .read(
                |_| Ok(serde_json::Value::Null),
                |_| Ok(serde_json::Value::Null),
                |_, _| true,
            )
            .write(
                |_| Ok(serde_json::Value::Null),
                |_| Ok(serde_json::Value::Null),
                |_, _| true,
            )
            .build();

        assert!(migrator.is_ok());
    }

    #[test_case(ExecutionOrder::Serial)]
    #[test_case(ExecutionOrder::Random)]
    #[test_case(ExecutionOrder::Parallel)]
    fn can_modify_execution_order(execution_order: ExecutionOrder) {
        let config = ConfigBuilder::new("sdk-key")
            .offline(true)
            .build()
            .expect("config failed to build");

        let client = Client::build(config).expect("client failed to build");
        let migrator = MigratorBuilder::new(client)
            .read(
                |_| Ok(serde_json::Value::Null),
                |_| Ok(serde_json::Value::Null),
                |_, _| true,
            )
            .write(
                |_| Ok(serde_json::Value::Null),
                |_| Ok(serde_json::Value::Null),
                |_, _| true,
            )
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

        let client = Client::build(config).expect("client failed to build");
        let migrator = MigratorBuilder::new(client)
            .write(
                |_| Ok(serde_json::Value::Null),
                |_| Ok(serde_json::Value::Null),
                |_, _| true,
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

        let client = Client::build(config).expect("client failed to build");
        let migrator = MigratorBuilder::new(client)
            .read(
                |_| Ok(serde_json::Value::Null),
                |_| Ok(serde_json::Value::Null),
                |_, _| true,
            )
            .build();

        assert!(migrator.is_err());
        assert_eq!(migrator.err().unwrap(), "write configuration not provided");
    }
}
