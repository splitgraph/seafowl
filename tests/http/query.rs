#[tokio::test]
async fn test_http_server_reader_writer() {
    // It's questionable how much value this testing adds on top of the tests in http.rs, but we do:
    //   - test the code consumes the config correctly, which we don't do in HTTP tests
    //   - hit the server that's actually listening on a port instead of calling warp routines directly.
    // Still, this test is mostly to make sure the whole thing roughly does what is intended by the config.

    let (addr, server, terminate, _) = make_read_only_http_server().await;

    tokio::task::spawn(server);
    let client = Client::new();
    let uri = format!("http://{}/q", addr);

    // POST SELECT 1 as a read-only user
    let resp = post_query(&client, &uri, "SELECT 1", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
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
        "CREATE TABLE test_table (col INT)",
        Some("write_password"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);

    // TODO use single statement when https://github.com/splitgraph/seafowl/issues/48 lands
    let resp = post_query(
        &client,
        &uri,
        "INSERT INTO test_table VALUES(1)",
        Some("write_password"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);

    // SELECT from the table as a read-only user
    let resp = post_query(&client, &uri, "SELECT * FROM test_table", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(response_text(resp).await, "{\"col\":1}\n");

    // Stop the server
    // NB this won't run if the test fails, but at that point we're terminating the process
    // anyway. Maybe it'll become a problem if we have a bunch of tests running that all
    // start servers and don't stop them.
    terminate.send(()).unwrap();
}
