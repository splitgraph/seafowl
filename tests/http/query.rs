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

    // Configure metrics
    setup_metrics(&Metrics::default());

    // GET & POST SELECT 1 as a read-only user
    for method in [Method::GET, Method::POST] {
        let resp = q(&client, method, &uri, "SELECT 1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            testutils::schema_from_header(resp.headers()),
            Schema::new(vec![Field::new("Int64(1)", DataType::Int64, false),])
        );
        assert_eq!(response_text(resp).await, "{\"Int64(1)\":1}\n");
    }

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
        testutils::schema_from_header(resp.headers()),
        Schema::new(vec![Field::new("col", DataType::Int32, true),])
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

    // Also test serialization of schemas with special characters
    let resp = post_query(
        &client,
        &scoped_uri,
        r#"CREATE TABLE new_table ("col_with_ :;.,\/""'?!(){}[]@<>=-+*#$&`|~^%" INT); INSERT INTO new_table VALUES(2); SELECT * FROM new_table"#,
        Some("write_password")
    ).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        testutils::schema_from_header(resp.headers()),
        Schema::new(vec![Field::new(
            r#"col_with_ :;.,\/"'?!(){}[]@<>=-+*#$&`|~^%"#,
            DataType::Int32,
            true
        ),])
    );
    assert_eq!(
        response_text(resp).await,
        "{\"col_with_ :;.,\\\\/\\\"'?!(){}[]@<>=-+*#$&`|~^%\":2}\n"
    );

    // Test multi-statement query starting with external table creation; it gets changed with almost
    // every DataFusion upgrade, so it can sometimes introduce regressions.
    let resp = post_query(
        &client,
        &uri,
        "CREATE EXTERNAL TABLE test_external \
            STORED AS PARQUET \
            LOCATION './tests/data/table_with_ns_column.parquet'; \
            CREATE TABLE test_external AS SELECT * FROM staging.test_external; \
            SELECT * FROM test_external LIMIT 1",
        Some("write_password"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        testutils::schema_from_header(resp.headers()),
        Schema::new(vec![
            Field::new("some_int_value", DataType::Int64, true),
            Field::new(
                "some_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true
            ),
            Field::new("some_value", DataType::Float32, true),
        ])
    );
    assert_eq!(response_text(resp).await, "{\"some_int_value\":1111,\"some_time\":\"2022-01-01T20:01:01\",\"some_value\":42.0}\n");

    // Finally test HTTP-related metrics
    assert_eq!(
        get_metrics(HTTP_REQUESTS).await,
        vec![
            "# HELP http_requests Counter tracking HTTP request statistics",
            "# TYPE http_requests counter",
            "http_requests{method=\"GET\",route=\"/q\",status=\"200\"} 1",
            "http_requests{method=\"POST\",route=\"/q\",status=\"200\"} 5",
            "http_requests{method=\"POST\",route=\"/q\",status=\"403\"} 1",
            "http_requests{method=\"POST\",route=\"/q\",status=\"500\"} 1",
        ]
    );

    // Stop the server
    // NB this won't run if the test fails, but at that point we're terminating the process
    // anyway. Maybe it'll become a problem if we have a bunch of tests running that all
    // start servers and don't stop them.
    terminate.send(()).unwrap();
}
