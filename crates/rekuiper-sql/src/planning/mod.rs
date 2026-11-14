//! Typed planning, expression type-checking, and capability descriptors.
//!
//! Legacy schemaless execution in [`crate::eval`] remains untouched; planning
//! types are opt-in and enforce conservative defaults for unknown functions.

pub mod adapter;
pub mod capabilities;
pub mod types;

pub use adapter::*;
pub use capabilities::*;
pub use types::*;
