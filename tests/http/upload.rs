use crate::http::*;

#[rstest]
#[case::csv_schema_supplied_with_headers("csv", true, Some(true))]
#[case::csv_schema_supplied_no_headers("csv", true, Some(false))]
#[case::csv_schema_inferred_with_headers("csv", false, Some(true))]
#[case::csv_schema_inferred_no_headers("csv", false, Some(false))]
#[case::parquet("parquet", false, None)]
#[tokio::test]
async fn test_upload_base(
    #[case] file_format: &str,
    #[case] include_schema: bool,
    #[case] add_headers: Option<bool>,
    #[values(None, Some("test_db"))] db_prefix: Option<&str>,
) {
    let (addr, server, terminate, mut context) = make_read_only_http_server().await;

    tokio::task::spawn(server);

    let table_name = format!("{file_format}_table");

    // Prepare the schema + data (a single record batch) which we'll save to a temp file via
    // a corresponding writer
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::Int32, false),
        Field::new("parity", DataType::Utf8, false),
    ]));

    // For CSV uploads we can supply the schema as another part of the multipart request, to
    // remove the ambiguity resulting from automatic schema inference
    let schema_json = serde_json::to_string(&schema).unwrap();

    let range = 0..2000;
    let mut input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(range.clone().collect_vec())),
            Arc::new(StringArray::from(
                range
                    .map(|number| if number % 2 == 0 { "even" } else { "odd" })
                    .collect_vec(),
            )),
        ],
    )
    .unwrap();

    // Open a temp file to write the data into
    let mut named_tempfile = Builder::new()
        .suffix(format!(".{file_format}").as_str())
        .tempfile()
        .unwrap();

    // Write out the CSV/Parquet format data to a temp file
    // drop the writer early to release the borrow.
    if file_format == "csv" {
        let mut writer = WriterBuilder::new()
            .with_header(add_headers.unwrap_or(true))
            .build(&mut named_tempfile);
        writer.write(&input_batch).unwrap();
    } else if file_format == "parquet" {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    // Generate curl arguments
    let mut curl_args: Vec<String> = vec![
        "-H".to_string(),
        "Authorization: Bearer write_password".to_string(),
    ];
    if include_schema {
        curl_args.append(&mut vec![
            "-F".to_string(),
            format!("schema={schema_json};type=application/json"),
        ]);
    }
    if let Some(has_headers) = add_headers {
        curl_args.append(&mut vec![
            "-F".to_string(),
            format!("has_header={has_headers}"),
        ])
    }

    let db_path = if let Some(db_name) = db_prefix {
        // Create the target database first
        context
            .collect(
                context
                    .plan_query(format!("CREATE DATABASE {db_name}").as_str())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        format!("/{db_name}")
    } else {
        String::from("")
    };

    curl_args.append(&mut vec![
        "-F".to_string(),
        format!("data=@{}", named_tempfile.path().to_str().unwrap()),
        format!("http://{addr}{db_path}/upload/test_upload/{table_name}"),
    ]);

    let mut child = Command::new("curl").args(curl_args).spawn().unwrap();
    let status = child.wait().await.unwrap();
    dbg!("Upload status is {}", status);

    // Verify the newly created table contents
    if let Some(db_name) = db_prefix {
        context = context.scope_to_catalog(db_name.to_string());
    }
    let plan = context
        .plan_query(format!("SELECT * FROM test_upload.{table_name}").as_str())
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    // Generate expected output from the input batch that was used in the multipart request
    if !include_schema && add_headers == Some(false) {
        // Rename the columns, as they will be inferred without a schema
        input_batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("column_1", input_batch.column(0).clone(), false),
            ("column_2", input_batch.column(1).clone(), false),
        ])
        .unwrap();
    }
    let formatted = arrow::util::pretty::pretty_format_batches(&[input_batch])
        .unwrap()
        .to_string();

    let expected: Vec<&str> = formatted.trim().lines().collect();

    assert_batches_eq!(expected, &results);

    terminate.send(()).unwrap();
}

#[tokio::test]
async fn test_upload_to_existing_table() {
    let (addr, server, terminate, context) = make_read_only_http_server().await;

    tokio::task::spawn(server);

    // Create a pre-existing table to upload into
    context
        .collect(
            context
                .plan_query("CREATE TABLE test_table(col_1 INT, col_2 TEXT, col_3 DOUBLE, col_4 TIMESTAMP) \
                AS VALUES (1, 'one', 1.0, '2001-01-01 01:01:01'), (2, 'two', 2.0, '2002-02-02 02:02:02')")
                .await
                .unwrap()
        )
        .await
        .unwrap();

    // Prepare the schema that matches the existing table + some data
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Int32, true),
        Field::new("col_2", DataType::Utf8, true),
        Field::new("col_3", DataType::Float64, true),
        Field::new(
            "col_4",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec![Some("three"), Some("four")])),
            Arc::new(Float64Array::from(vec![3.0, 4.0])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1046660583000000,
                1081051444000000,
            ])),
        ],
    )
    .unwrap();

    let mut named_tempfile = Builder::new().suffix(".parquet").tempfile().unwrap();
    // drop the writer early to release the borrow.
    {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    let mut child = Command::new("curl")
        .args([
            "-H",
            "Authorization: Bearer write_password",
            "-F",
            format!("data=@{}", named_tempfile.path().to_str().unwrap()).as_str(),
            format!("http://{addr}/upload/public/test_table").as_str(),
        ])
        .spawn()
        .unwrap();
    let status = child.wait().await.unwrap();
    dbg!("Upload status is {}", status);

    // Verify the newly created table contents
    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY col_1")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+-------+-------+-------+---------------------+",
        "| col_1 | col_2 | col_3 | col_4               |",
        "+-------+-------+-------+---------------------+",
        "| 1     | one   | 1.0   | 2001-01-01T01:01:01 |",
        "| 2     | two   | 2.0   | 2002-02-02T02:02:02 |",
        "| 3     | three | 3.0   | 2003-03-03T03:03:03 |",
        "| 4     | four  | 4.0   | 2004-04-04T04:04:04 |",
        "+-------+-------+-------+---------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Now try with schema that doesn't match the existing table, but can be coerced to it; also
    // has one missing column supposed to be replaced with NULL's, and one extra column that should
    // be ignored.
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Float64, true),
        Field::new("col_3", DataType::Int32, true),
        Field::new("col_5", DataType::Boolean, true),
        Field::new(
            "col_4",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));

    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![5.0, 6.0])),
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1115269505000000000,
                1149573966000000000,
            ])),
        ],
    )
    .unwrap();

    // Open a temp file to write the data into
    let mut named_tempfile = Builder::new().suffix(".parquet").tempfile().unwrap();
    // drop the writer early to release the borrow.
    {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    Command::new("curl")
        .args([
            "-H",
            "Authorization: Bearer write_password",
            "-F",
            format!("data=@{}", named_tempfile.path().to_str().unwrap()).as_str(),
            format!("http://{addr}/upload/public/test_table").as_str(),
        ])
        .output()
        .await
        .unwrap();

    dbg!("Upload status is {}", status);

    // Verify that the rows have been appended
    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY col_1")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+-------+-------+-------+---------------------+",
        "| col_1 | col_2 | col_3 | col_4               |",
        "+-------+-------+-------+---------------------+",
        "| 1     | one   | 1.0   | 2001-01-01T01:01:01 |",
        "| 2     | two   | 2.0   | 2002-02-02T02:02:02 |",
        "| 3     | three | 3.0   | 2003-03-03T03:03:03 |",
        "| 4     | four  | 4.0   | 2004-04-04T04:04:04 |",
        "| 5     |       | 5.0   | 2005-05-05T05:05:05 |",
        "| 6     |       | 6.0   | 2006-06-06T06:06:06 |",
        "+-------+-------+-------+---------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Finally try with schema that can't be coerced to the existing table.
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Boolean, true),
        Field::new("col_2", DataType::Boolean, true),
        Field::new("col_3", DataType::Boolean, true),
        Field::new("col_4", DataType::Boolean, true),
    ]));

    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
        ],
    )
    .unwrap();

    // Open a temp file to write the data into
    let mut named_tempfile = Builder::new().suffix(".parquet").tempfile().unwrap();
    // drop the writer early to release the borrow.
    {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    let output = Command::new("curl")
        .args([
            "-H",
            "Authorization: Bearer write_password",
            "-F",
            format!("data=@{}", named_tempfile.path().to_str().unwrap()).as_str(),
            format!("http://{addr}/upload/public/test_table").as_str(),
        ])
        .output()
        .await
        .unwrap();

    dbg!("Upload status is {}", status);

    assert_eq!(
        "Error during planning: Cannot cast file schema field col_4 of type Boolean to table schema field of type Timestamp(Microsecond, None)".to_string(),
        String::from_utf8(output.stdout).unwrap()
    );

    terminate.send(()).unwrap();
}

#[tokio::test]
async fn test_upload_not_writer_authz() {
    let (addr, server, terminate, _context) = make_read_only_http_server().await;

    tokio::task::spawn(server);

    let output = Command::new("curl")
        .args([
            "-H",
            "Authorization: Bearer wrong_password",
            "-F",
            "data='doesntmatter'",
            format!("http://{addr}/upload/public/test_table").as_str(),
        ])
        .output()
        .await
        .unwrap();

    assert_eq!(
        "INVALID_ACCESS_TOKEN".to_string(),
        String::from_utf8(output.stdout).unwrap()
    );
    terminate.send(()).unwrap();
}
