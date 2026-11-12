//! pg_rust heap access method.
//!
//! Provides the slotted-page and 64-byte tuple formats, WAL-backed CRUD and
//! redo handlers, MVCC/HOT updates, vacuum, and Phase 4a TOAST chunk I/O.
//! External attributes retain the frozen 20-byte pointer and live in hidden
//! heap relations whose chunks share the main row's transaction and snapshot
//! visibility rules.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod access_method;
pub mod error;
pub mod heap_am;
pub mod line_pointer;
pub mod redo;
pub mod slotted_page;
pub mod toast;
pub mod tuple;
pub mod undo;

pub use access_method::{
    AccessMethod, BuildContext, DeleteContext, InsertContext, RelationDesc, ScanContext,
    UpdatableAM, UpdateContext, Vacuumable,
};
pub use error::{HeapError, Result};
pub use heap_am::{follow_hot_chain, hot_chain_root, HeapAM};
pub use line_pointer::{LinePointer, LpFlags};
pub use redo::{
    heap_redo_handlers, HeapCleanupRedoHandler, HeapDeleteHandler, HeapHotUpdateHandler,
    HeapInsertHandler, HeapUpdateHandler,
};
pub use slotted_page::{SlottedPage, HEAP_SPECIAL_SIZE};
pub use toast::ToastPointer;
pub use tuple::{ColumnType, Datum, TupleHeader, MAX_VECTOR_DIM};
pub use undo::HeapUndoHandler;
