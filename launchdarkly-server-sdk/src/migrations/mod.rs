use serde::Serialize;

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "lowercase")]
/// Origin represents the source of origin for a migration-related operation.
pub enum Origin {
    /// Old represents the technology source we are migrating away from.
    Old,
    /// New represents the technology source we are migrating towards.
    New,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
/// Operation represents a type of migration operation; namely, read or write.
pub enum Operation {
    /// Read denotes a read-related migration operation.
    Read,
    /// Write denotes a write-related migration operation.
    Write,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize)]
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

pub use tracker::MigrationOpTracker;

mod tracker;
