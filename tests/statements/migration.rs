use crate::statements::*;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;

/// Make a SeafowlContext that's connected to a legacy SQLite catalog copy
async fn make_context_with_local_sqlite(
    source_dir: &str,
    data_dir: String,
) -> DefaultSeafowlContext {
    assert_ne!(data_dir.as_str(), source_dir);

    // Copy the legacy catalog into the provided data directory
    copy_dir(Path::new(source_dir), Path::new(&data_dir)).unwrap();

    let config_text = format!(
        r#"
[object_store]
type = "local"
data_dir = "{data_dir}"
[catalog]
type = "sqlite"
dsn = "{data_dir}/seafowl.sqlite""#
    );

    let config = load_config_from_string(&config_text, true, None).unwrap();
    build_context(&config).await.unwrap()
}

fn copy_dir(src: &Path, dst: &Path) -> io::Result<()> {
    if !dst.exists() {
        fs::create_dir_all(dst)?;
    }

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let new_path = dst.join(path.file_name().ok_or_else(|| {
            io::Error::new(ErrorKind::Other, "Failed to get file name")
        })?);

        if entry.file_type()?.is_dir() {
            copy_dir(&path, &new_path)?;
        } else {
            fs::copy(&path, &new_path)?;
        }
    }

    Ok(())
}

#[rstest]
#[should_panic(
    expected = r#"There are still some legacy partitions that need to be removed before running migrations for Seafowl v0.4:
7fbfeeeade71978b4ae82cd3d97b8c1bd9ae7ab9a7a78ee541b66209cfd7722d.parquet
9ae6f4222893474551037d0e44ff223ca5ea8e703d141b14835025923a66ab50.parquet
ea192fa7ae3b4abca9ded70e480c188e2c260ece02a810e5f1e2be41b0d6c0f6.parquet
534e5cc396e5b24725993145821b864cbfb07c2d8d7116f3d60d28bc02900861.parquet
Please go through the migration instructions laid out in https://github.com/splitgraph/seafowl/issues/392."#
)]
#[case::with_legacy_v0_2("tests/data/seafowl-0.2-legacy-data/")]
#[should_panic(
    expected = r#"There are still some legacy tables that need to be removed before running migrations for Seafowl v0.4:
default.public.legacy_table
Please go through the migration instructions laid out in https://github.com/splitgraph/seafowl/issues/392."#
)]
#[case::with_legacy_v0_3("tests/data/seafowl-0.3-legacy-data/")]
#[tokio::test]
async fn test_legacy_tables(#[case] source_dir: &str) {
    let data_dir = TempDir::new().unwrap();

    make_context_with_local_sqlite(source_dir, data_dir.path().display().to_string())
        .await;
}
