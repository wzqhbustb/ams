//! pg_rust heap access method — Phase 1 M2.
//!
//! Current stage (M2a Stage G) implements the **in-memory page format** only:
//! - Slotted page layout (line pointer array + tuple data)
//! - Tuple encoding/decoding (64-byte header + null bitmap + attributes)
//! - TOAST pointer encoding (oversized attribute storage)
//!
//! Trait implementations (`pg-catalog::AccessMethod`, `UpdatableAM`,
//! `Vacuumable`), heap redo handlers (`HeapInsert`, `HeapUpdate`,
//! `HeapDelete`) and TOAST chunk table I/O land in Stage I.

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
pub use tuple::{ColumnType, Datum, TupleHeader};
pub use undo::HeapUndoHandler;
