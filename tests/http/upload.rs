use crate::http::*;
use test_case::test_case;

#[test_case(
    "csv",
    true,
    Some(true);
    "CSV file schema supplied w/ headers")
]
#[test_case(
    "csv",
    true,
    Some(false);
    "CSV file schema supplied w/o headers")
]
#[test_case(
    "csv",
    false,
    Some(true);
    "CSV file schema inferred w/ headers")
]
#[test_case(
    "csv",
    false,
    Some(false);
    "CSV file schema inferred w/o headers")
]
#[test_case(
    "parquet",
    false,
    None;
    "Parquet file")
]
#[tokio::test]
async fn test_upload_base(
    file_format: &str,
    include_schema: bool,
    add_headers: Option<bool>,
) {
    let (addr, server, terminate, context) = make_read_only_http_server().await;

    tokio::task::spawn(server);

    let table_name = format!("{}_table", file_format);

    // Prepare the schema + data (a single record batch) which we'll save to a temp file via
    // a corresponding writer
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::Int32, false),
        Field::new("parity", DataType::Utf8, false),
    ]));

    // For CSV uploads we can supply the schema as another part of the multipart request, to
    // remove the ambiguity resulting from automatic schema inference
    let schema_json = r#"{
        "fields": [
            {
                "name": "number",
                "nullable": false,
                "type": {
                    "name": "int",
                    "bitWidth": 32,
                    "isSigned": true
                }
            },
            {
                "name": "parity",
                "nullable": false,
                "type": {
                    "name": "utf8"
                }
            }
        ]
    }"#;

    let range = 0..2000;
    let mut input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_slice(range.clone().collect_vec())),
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
        .suffix(format!(".{}", file_format).as_str())
        .tempfile()
        .unwrap();

    // Write out the CSV/Parquet format data to a temp file
    // drop the writer early to release the borrow.
    if file_format == "csv" {
        let mut writer = WriterBuilder::new()
            .has_headers(if let Some(has_headers) = add_headers {
                has_headers
            } else {
                true
            })
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
            format!("schema={};type=application/json", schema_json),
        ]);
    }
    if let Some(has_headers) = add_headers {
        curl_args.append(&mut vec![
            "-F".to_string(),
            format!("has_header={}", has_headers),
        ])
    }
    curl_args.append(&mut vec![
        "-F".to_string(),
        format!("data=@{}", named_tempfile.path().to_str().unwrap()),
        format!("http://{}/upload/test_upload/{}", addr, table_name),
    ]);

    let mut child = Command::new("curl").args(curl_args).spawn().unwrap();
    let status = child.wait().await.unwrap();
    dbg!("Upload status is {}", status);

    // Verify the newly created table contents
    let plan = context
        .plan_query(format!("SELECT * FROM test_upload.{}", table_name).as_str())
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
                .plan_query("CREATE TABLE test_table(col_1 INT)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    context
        .collect(
            context
                .plan_query("INSERT INTO test_table VALUES (1)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    // Prepare the schema that matches the existing table + some data
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col_1",
        DataType::Int32,
        true,
    )]));

    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![2, 3]))],
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
            format!("http://{}/upload/public/test_table", addr).as_str(),
        ])
        .spawn()
        .unwrap();
    let status = child.wait().await.unwrap();
    dbg!("Upload status is {}", status);

    // Verify the newly created table contents
    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+-------+",
        "| col_1 |",
        "+-------+",
        "| 1     |",
        "| 2     |",
        "| 3     |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &results);

    // Now try with schema that doesn't matches the existing table (re-use input batch from before)
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col_1",
        DataType::Int32,
        false,
    )]));

    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![4, 5]))],
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
            format!("http://{}/upload/public/test_table", addr).as_str(),
        ])
        .output()
        .await
        .unwrap();

    assert_eq!(
        "Execution error: The table public.test_table already exists but has a different schema than the one provided.".to_string(),
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
            format!("http://{}/upload/public/test_table", addr).as_str(),
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
