use crate::cli::*;

#[tokio::test]
async fn test_one_off_with_token() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = setup_temp_config_and_data_dir()?;
    let (access_key_id, secret_access_key, session_token) = get_sts_creds().await;

    let output = Command::cargo_bin("seafowl")?
        .arg("--one-off")
        .arg("CREATE TABLE one_off AS VALUES (1, 'one'), (2, 'two'); SELECT * FROM one_off")
        .arg("-c")
        .arg(temp_dir.path().join(TEST_CONFIG_FILE))
        // Override the object_store section with the custom credentials
        .env("SEAFOWL__OBJECT_STORE__ACCESS_KEY_ID", access_key_id)
        .env("SEAFOWL__OBJECT_STORE__SECRET_ACCESS_KEY", secret_access_key)
        .env("SEAFOWL__OBJECT_STORE__SESSION_TOKEN", session_token)
        .env("SEAFOWL__OBJECT_STORE__ENDPOINT", "http://localhost:9000")
        .env("SEAFOWL__OBJECT_STORE__BUCKET", "seafowl-test-bucket")
        .output()?;

    assert!(output.status.success());

    // Convert the stdout bytes to a text representation
    let output: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .trim()
        .lines()
        .map(String::from)
        .collect();
    let lines = output.len();

    // Assert only the results (throw away the preceding log output).
    let results = output[lines - 2..lines].join("\n");

    assert_eq!(
        r#"{"column1":1,"column2":"one"}
{"column1":2,"column2":"two"}"#,
        results,
    );

    Ok(())
}
