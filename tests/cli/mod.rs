use assert_cmd::prelude::*; // Add methods on commands
use rstest::rstest;
use seafowl::config::schema::DEFAULT_SQLITE_DB;
use seafowl::repository::postgres::testutils::get_random_schema;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio}; // Run programs
use tempfile::{Builder, TempDir};

use crate::{get_sts_creds, AssumeRoleTarget};

mod basic;
mod one_off;

const TEST_CONFIG_FILE: &str = "seafowl-test.toml";

fn setup_temp_config_and_data_dir() -> std::io::Result<TempDir> {
    let temp_dir = Builder::new()
        .prefix("seafowl-test-dir")
        .rand_bytes(5)
        .tempdir()?;

    let file_path = temp_dir.path().join(TEST_CONFIG_FILE);
    let mut conf_file = File::create(file_path)?;

    let dsn = Path::new(&temp_dir.path().display().to_string())
        .join(DEFAULT_SQLITE_DB)
        .to_str()
        .unwrap()
        .to_string();

    let config_str = format!(
        r#"
[object_store]
type = "local"
data_dir = "{}"

[catalog]
type = "sqlite"
dsn = "{}"
"#,
        temp_dir.path().display(),
        dsn.escape_default(),
    );

    write!(conf_file, "{config_str}")?;
    Ok(temp_dir)
}
