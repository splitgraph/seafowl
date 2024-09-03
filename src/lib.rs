#![feature(let_chains)]

pub mod auth;
pub mod catalog;
pub mod cli;
pub mod config;
pub mod context;
pub mod datafusion;
pub mod frontend;
pub mod memory_pool;
pub mod nodes;
pub mod object_store;
pub mod provider;
pub mod repository;
pub mod sync;
pub mod system_tables;
pub mod utils;
pub mod version;
pub mod wasm_udf;

extern crate lazy_static;

#[cfg(test)]
pub(crate) mod testutils;
