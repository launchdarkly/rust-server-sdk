//! Types for implementing custom FDv2 data sources.
//!
//! This module is experimental and not subject to semantic versioning. Its API
//! may change in any release.
//!
//! # Examples
//!
//! Implement a custom synchronizer and add it to a data system.
//! ```
//! use launchdarkly_server_sdk::data_sources::{
//!     DataSourceBuildContext, FDv2SourceEvent, FDv2SourceEventFuture, FDv2SourceResult,
//!     FDv2SynchronizerConfig, Selector, Synchronizer, SynchronizerFactory,
//! };
//! use launchdarkly_server_sdk::{ConfigBuilder, DataSystemBuildError, DataSystemBuilder};
//!
//! struct MySynchronizer;
//!
//! impl Synchronizer for MySynchronizer {
//!     fn next(&mut self, _selector: Selector) -> FDv2SourceEventFuture<'_> {
//!         Box::pin(async {
//!             FDv2SourceEvent {
//!                 result: FDv2SourceResult::Goodbye,
//!                 fdv1_fallback: None,
//!             }
//!         })
//!     }
//!
//!     fn name(&self) -> &str {
//!         "my-synchronizer"
//!     }
//! }
//!
//! struct MyFactory;
//!
//! impl SynchronizerFactory for MyFactory {
//!     fn create(&self) -> Box<dyn Synchronizer> {
//!         Box::new(MySynchronizer)
//!     }
//! }
//!
//! #[derive(Clone)]
//! struct MyConfig;
//!
//! impl FDv2SynchronizerConfig for MyConfig {
//!     fn build_synchronizer(
//!         &self,
//!         _context: &DataSourceBuildContext,
//!     ) -> Result<Box<dyn SynchronizerFactory>, DataSystemBuildError> {
//!         Ok(Box::new(MyFactory))
//!     }
//!
//!     fn to_owned(&self) -> Box<dyn FDv2SynchronizerConfig> {
//!         Box::new(self.clone())
//!     }
//! }
//!
//! let mut data_system = DataSystemBuilder::custom();
//! data_system.synchronizer(MyConfig);
//! ConfigBuilder::new("sdk-key").data_system(&data_system);
//! ```

pub use crate::data_system_builders::{
    DataSourceBuildContext, FDv2InitializerConfig, FDv2SynchronizerConfig,
};
pub use crate::fdv2::data_system::{InitializerFactory, SynchronizerFactory};
pub use crate::fdv2::model::{ChangeSetKind, Selector};
pub use crate::fdv2::request_headers::RequestHeaders;
pub use crate::fdv2::source::{
    ErrorInfo, ErrorKind, FDv1FallbackDirective, FDv2SourceEvent, FDv2SourceEventFuture,
    FDv2SourceResult, Initializer, Synchronizer,
};
pub use crate::stores::change_set::{ChangeSet, ItemChange};
