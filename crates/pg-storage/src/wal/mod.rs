//! Write-ahead log (WAL) subsystem.

pub mod reader;
pub mod record;
pub mod segment;
pub mod writer;

pub use reader::{WalReader, WalRecordIter};
pub use record::{
    bincode_config, CheckpointEndRecord, FullPageImageRecord, PageAllocRecord, TxnAbortRecord,
    TxnCommitRecord, WalRecord, WalRecordType, CHECKPOINT_END_V2_FLAGS,
};
pub use segment::{wal_filename, WalSegmentManager};
pub use writer::WalWriter;
