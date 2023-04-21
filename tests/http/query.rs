use crate::http::*;

#[tokio::test]
async fn test_http_server_reader_writer() {
    // It's questionable how much value this testing adds on top of the tests in http.rs, but we do:
    //   - test the code consumes the config correctly, which we don't do in HTTP tests
    //   - hit the server that's actually listening on a port instead of calling warp routines directly.
    // Still, this test is mostly to make sure the whole thing roughly does what is intended by the config.

    let (addr, server, terminate, _) = make_read_only_http_server().await;

    tokio::task::spawn(server);
    let client = Client::new();
    let uri = format!("http://{addr}/q");

    // POST SELECT 1 as a read-only user
    let resp = post_query(&client, &uri, "SELECT 1", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        r#"application/json; arrow-schema="{\"fields\":[{\"children\":[],\"name\":\"Int64(1)\",\"nullable\":false,\"type\":{\"bitWidth\":64,\"isSigned\":true,\"name\":\"int\"}}],\"metadata\":{}}""#,
    );
    assert_eq!(response_text(resp).await, "{\"Int64(1)\":1}\n");

    // POST CREATE TABLE as a read-only user
    let resp = post_query(&client, &uri, "CREATE TABLE test_table (col INT)", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(response_text(resp).await, "WRITE_FORBIDDEN");

    // Same, wrong token
    let resp = post_query(
        &client,
        &uri,
        "CREATE TABLE test_table (col INT)",
        Some("wrongpw"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response_text(resp).await, "INVALID_ACCESS_TOKEN");

    // Perform a write correctly
    let resp = post_query(
        &client,
        &uri,
        "CREATE TABLE test_table (col INT); INSERT INTO test_table VALUES(1); SELECT * FROM test_table",
        Some("write_password"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        r#"application/json; arrow-schema="{\"fields\":[{\"children\":[],\"name\":\"col\",\"nullable\":true,\"type\":{\"bitWidth\":32,\"isSigned\":true,\"name\":\"int\"}}],\"metadata\":{}}""#,
    );
    assert_eq!(response_text(resp).await, "{\"col\":1}\n");

    // Test the DB-scoped endpoint variant
    // First create the new database
    let resp = post_query(
        &client,
        &uri,
        "CREATE DATABASE new_db",
        Some("write_password"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);

    let scoped_uri = format!("http://{addr}/new_db/q");

    let resp = post_query(
        &client,
        &scoped_uri,
        "CREATE TABLE new_table (new_col INT); INSERT INTO new_table VALUES(2); SELECT * FROM new_table",
        Some("write_password")
    ).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(response_text(resp).await, "{\"new_col\":2}\n");

    // Stop the server
    // NB this won't run if the test fails, but at that point we're terminating the process
    // anyway. Maybe it'll become a problem if we have a bunch of tests running that all
    // start servers and don't stop them.
    terminate.send(()).unwrap();
}
