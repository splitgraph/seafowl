use crate::cli::*;

#[rstest]
#[tokio::test]
async fn test_one_off_with_token(
    #[values(AssumeRoleTarget::MinioUser, AssumeRoleTarget::DexOIDC)]
    role: AssumeRoleTarget,
) -> Result<(), Box<dyn std::error::Error>> {
    let (access_key_id, secret_access_key, session_token) = get_sts_creds(role).await;

    let output = Command::cargo_bin("seafowl")?
        .arg("--one-off")
        .arg("CREATE TABLE one_off AS VALUES (1, 'one'), (2, 'two'); SELECT * FROM one_off")
        .env("SEAFOWL__OBJECT_STORE__TYPE", "s3")
        .env("SEAFOWL__OBJECT_STORE__ACCESS_KEY_ID", access_key_id)
        .env("SEAFOWL__OBJECT_STORE__SECRET_ACCESS_KEY", secret_access_key)
        .env("SEAFOWL__OBJECT_STORE__SESSION_TOKEN", session_token)
        .env("SEAFOWL__OBJECT_STORE__ENDPOINT", "http://localhost:9000")
        .env("SEAFOWL__OBJECT_STORE__BUCKET", "seafowl-test-bucket")
        .env("SEAFOWL__CATALOG__TYPE", "postgres")
        .env("SEAFOWL__CATALOG__DSN", env::var("DATABASE_URL").unwrap())
        .env("SEAFOWL__CATALOG__SCHEMA", get_random_schema())
        .output()?;

    assert!(
        output.status.success(),
        "Command unsuccessful\nCaptured stdout:\n{}\n\nCaptured stderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

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
