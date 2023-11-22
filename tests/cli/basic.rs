use crate::cli::*;

// Seems like we can't read from stdout until we close stdin, at which point we can no longer
// enter any commands.
// Consequently, the test is structured such that we first issue all te commands and only then
// do we assert on the output.
#[test]
fn test_cli_basic() -> std::io::Result<()> {
    let temp_dir = setup_temp_config_and_data_dir()?;
    let mut cmd = Command::cargo_bin("seafowl").expect("Seafowl bin exists");
    cmd.arg("--cli")
        .arg("-c")
        .arg(temp_dir.path().join(TEST_CONFIG_FILE))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn()?;

    //
    // First run all the input commands
    //

    // Send input to the command through stdin
    let mut stdin = child.stdin.take().expect("Failed to open stdin");
    let mut expected_stdout: Vec<&str> = vec![];
    let mut expected_stderr: Vec<&str> = vec![];

    // Test a basic SELECT
    writeln!(&stdin, "SELECT 'Hello World';")?;
    expected_stdout.extend(vec![
        "+---------------------+",
        "| Utf8(\"Hello World\") |",
        "+---------------------+",
        "| Hello World         |",
        "+---------------------+",
    ]);

    // Hit a missing table error
    writeln!(stdin, "SELECT * FROM t;")?;
    expected_stderr.push("Error during planning: table 'default.public.t' not found");

    // Test multi-line query and create the table
    writeln!(stdin, "CREATE TABLE t")?;
    writeln!(stdin, "AS VALUES")?;
    writeln!(stdin, "(1, 'one'),")?;
    writeln!(stdin, "(2, 'two');")?;

    // Hit syntax error
    writeln!(stdin, "SELECT * FRM t;")?;
    expected_stderr
        .push("SQL error: ParserError(\"Expected end of statement, found: FRM\")");

    // Now actually select from it and test multi-query execution
    writeln!(stdin, "SELECT column1 FROM t; SELECT column2 FROM t;")?;
    expected_stdout.extend(vec![
        "+---------+",
        "| column1 |",
        "+---------+",
        "| 1       |",
        "| 2       |",
        "+---------+",
        "+---------+",
        "| column2 |",
        "+---------+",
        "| one     |",
        "| two     |",
        "+---------+",
    ]);

    // Test some commands
    writeln!(stdin, "\\d")?;
    expected_stdout.extend(vec![
        "+---------------+--------------------+----------------+------------+",
        "| table_catalog | table_schema       | table_name     | table_type |",
        "+---------------+--------------------+----------------+------------+",
        "| default       | public             | t              | BASE TABLE |",
        "| default       | system             | table_versions | VIEW       |",
        "| default       | system             | dropped_tables | VIEW       |",
        "| default       | information_schema | tables         | VIEW       |",
        "| default       | information_schema | views          | VIEW       |",
        "| default       | information_schema | columns        | VIEW       |",
        "| default       | information_schema | df_settings    | VIEW       |",
        "+---------------+--------------------+----------------+------------+",
    ]);

    writeln!(stdin, "\\d t")?;
    expected_stdout.extend(
        vec![
            "+---------------+--------------+------------+-------------+-----------+-------------+",
            "| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |",
            "+---------------+--------------+------------+-------------+-----------+-------------+",
            "| default       | public       | t          | column1     | Int64     | YES         |",
            "| default       | public       | t          | column2     | Utf8      | YES         |",
            "+---------------+--------------+------------+-------------+-----------+-------------+",
        ]
    );

    // Close the CLI
    // NB: if we hadn't done it like this we'd need to call drop(stdin), since otherwise the
    // test would hang.
    writeln!(stdin, "\\q")?;

    //
    // Now examine the actual output and assert on expected values
    //

    let stdout = child.stdout.take().expect("Failed to open stdout");
    let reader = BufReader::new(stdout);

    // Read out all the lines from stdout
    let mut actual = vec![];
    for line in reader.lines() {
        let l = line?;
        // Don't assert on the timing info since that is not deterministic
        if !l.starts_with("Time: ") {
            actual.push(l);
        }
    }

    assert_eq!(
        expected_stdout, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_stdout, actual
    );

    //
    // Finally examine the errors
    //

    let stderr = child.stderr.take().expect("Failed to open stderr");
    let reader = BufReader::new(stderr);

    // Read out all the lines from stderr
    let mut actual = vec![];
    for line in reader.lines() {
        actual.push(line?);
    }

    assert_eq!(
        expected_stderr, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_stderr, actual
    );

    // Wait for the command to finish
    let status = child.wait().expect("Failed to wait for command");
    assert!(status.success());

    Ok(())
}
