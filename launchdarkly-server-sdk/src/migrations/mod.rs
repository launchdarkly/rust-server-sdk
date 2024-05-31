use core::fmt;
use std::fmt::{Display, Formatter};

use launchdarkly_server_sdk_evaluation::FlagValue;
use serde::Serialize;

#[non_exhaustive]
#[derive(Debug, Copy, Clone, Serialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "lowercase")]
/// Origin represents the source of origin for a migration-related operation.
pub enum Origin {
    /// Old represents the technology source we are migrating away from.
    Old,
    /// New represents the technology source we are migrating towards.
    New,
}

#[non_exhaustive]
#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
/// Operation represents a type of migration operation; namely, read or write.
pub enum Operation {
    /// Read denotes a read-related migration operation.
    Read,
    /// Write denotes a write-related migration operation.
    Write,
}

#[non_exhaustive]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
/// Stage denotes one of six possible stages a technology migration could be a
/// part of, progressing through the following order.
///
/// Off -> DualWrite -> Shadow -> Live -> RampDown -> Complete
pub enum Stage {
    /// Off - migration hasn't started, "old" is authoritative for reads and writes
    Off,
    /// DualWrite - write to both "old" and "new", "old" is authoritative for reads
    DualWrite,
    /// Shadow - both "new" and "old" versions run with a preference for "old"
    Shadow,
    /// Live - both "new" and "old" versions run with a preference for "new"
    Live,
    /// RampDown - only read from "new", write to "old" and "new"
    Rampdown,
    /// Complete - migration is done
    Complete,
}

impl Display for Stage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Stage::Off => write!(f, "off"),
            Stage::DualWrite => write!(f, "dualwrite"),
            Stage::Shadow => write!(f, "shadow"),
            Stage::Live => write!(f, "live"),
            Stage::Rampdown => write!(f, "rampdown"),
            Stage::Complete => write!(f, "complete"),
        }
    }
}

impl From<Stage> for FlagValue {
    fn from(stage: Stage) -> FlagValue {
        FlagValue::Str(stage.to_string())
    }
}

impl TryFrom<FlagValue> for Stage {
    type Error = String;

    fn try_from(value: FlagValue) -> Result<Self, Self::Error> {
        if let FlagValue::Str(value) = value {
            match value.as_str() {
                "off" => Ok(Stage::Off),
                "dualwrite" => Ok(Stage::DualWrite),
                "shadow" => Ok(Stage::Shadow),
                "live" => Ok(Stage::Live),
                "rampdown" => Ok(Stage::Rampdown),
                "complete" => Ok(Stage::Complete),
                _ => Err(format!("Invalid stage: {}", value)),
            }
        } else {
            Err("Cannot convert non-string value to Stage".to_string())
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
/// ExecutionOrder represents the various execution modes this SDK can operate under while
/// performing migration-assisted reads.
pub enum ExecutionOrder {
    /// Serial execution ensures the authoritative read will always complete execution before
    /// executing the non-authoritative read.
    Serial,
    /// Random execution randomly decides if the authoritative read should execute first or second.
    Random,
    /// Parallel executes both reads in separate threads, and waits until both calls have
    /// finished before proceeding.
    Parallel,
}

pub use migrator::MigratorBuilder;
pub use tracker::MigrationOpTracker;

mod migrator;
mod tracker;
