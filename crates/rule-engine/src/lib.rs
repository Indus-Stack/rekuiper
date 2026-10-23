//! # rule-engine
//!
//! High-performance edge stream processing engine and streaming SQL rule engine in Rust.
//!
//! ## Embedding in Your Own Application
//!
//! You can embed `rule-engine` or its streaming SQL engine directly in your Rust project:
//!
//! ```toml
//! [dependencies]
//! rule-engine = "1.0.0-rc4"
//! ```
//!
//! ### Using the Streaming SQL Engine
//!
//! ```rust
//! use rule_engine::sql::Parser;
//!
//! # fn main() -> anyhow::Result<()> {
//! let sql = "SELECT temperature * 1.8 + 32 AS temp_f FROM sensor WHERE temperature > 20";
//! let mut parser = Parser::new(sql);
//! let stmt = parser.parse_select()?;
//! # Ok(())
//! # }
//! ```

pub use kuiperd::*;
pub use rekuiper_core as core;
pub use rekuiper_sql as sql;
