use crate::statements::*;

#[tokio::test]
async fn test_create_and_run_function() {
    let (context, _) = make_context_with_pg().await;

    let function_query = r#"CREATE FUNCTION sintau AS '
    {
        "entrypoint": "sintau",
        "language": "wasm",
        "input_types": ["float"],
        "return_type": "float",
        "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
    }';"#;

    let plan = context.plan_query(function_query).await.unwrap();
    context.collect(plan).await.unwrap();

    let results = context
        .collect(
            context
                .plan_query(
                    "
        SELECT v, ROUND(sintau(CAST(v AS REAL)) * 100) AS sintau
        FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5)) d (v)",
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let expected = vec![
        "+-----+--------+",
        "| v   | sintau |",
        "+-----+--------+",
        "| 0.1 | 59     |",
        "| 0.2 | 95     |",
        "| 0.3 | 95     |",
        "| 0.4 | 59     |",
        "| 0.5 | 0      |",
        "+-----+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // Run the same query again to make sure we raise an error if the function already exists
    let err = context.plan_query(function_query).await.unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: Function \"sintau\" already exists"
    );
}

#[tokio::test]
async fn test_create_and_run_function_legacy_type_names() {
    let (context, _) = make_context_with_pg().await;

    let function_query = r#"CREATE FUNCTION sintau AS '
    {
        "entrypoint": "sintau",
        "language": "wasm",
        "input_types": ["f32"],
        "return_type": "f32",
        "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
    }';"#;

    let plan = context.plan_query(function_query).await.unwrap();
    context.collect(plan).await.unwrap();

    let results = context
        .collect(
            context
                .plan_query(
                    "
        SELECT v, ROUND(sintau(CAST(v AS REAL)) * 100) AS sintau
        FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5)) d (v)",
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let expected = vec![
        "+-----+--------+",
        "| v   | sintau |",
        "+-----+--------+",
        "| 0.1 | 59     |",
        "| 0.2 | 95     |",
        "| 0.3 | 95     |",
        "| 0.4 | 59     |",
        "| 0.5 | 0      |",
        "+-----+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // Run the same query again to make sure we raise an error if the function already exists
    let err = context.plan_query(function_query).await.unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: Function \"sintau\" already exists"
    );
}

#[tokio::test]
async fn test_create_and_run_function_uppercase_type_names() {
    let (context, _) = make_context_with_pg().await;

    let function_query = r#"CREATE FUNCTION sintau AS '
    {
        "entrypoint": "sintau",
        "language": "wasm",
        "input_types": ["FLOAT"],
        "return_type": "REAL",
        "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
    }';"#;

    let plan = context.plan_query(function_query).await.unwrap();
    context.collect(plan).await.unwrap();

    let results = context
        .collect(
            context
                .plan_query(
                    "
        SELECT v, ROUND(sintau(CAST(v AS REAL)) * 100) AS sintau
        FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5)) d (v)",
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let expected = vec![
        "+-----+--------+",
        "| v   | sintau |",
        "+-----+--------+",
        "| 0.1 | 59     |",
        "| 0.2 | 95     |",
        "| 0.3 | 95     |",
        "| 0.4 | 59     |",
        "| 0.5 | 0      |",
        "+-----+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // Run the same query again to make sure we raise an error if the function already exists
    let err = context.plan_query(function_query).await.unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: Function \"sintau\" already exists"
    );
}
