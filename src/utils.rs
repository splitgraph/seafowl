use std::{io::Write, sync::Arc};

use arrow::json::LineDelimitedWriter;

use crate::context::SeafowlContext;

// Run a one-off command and output its results to a writer
pub async fn run_one_off_command<W>(
    context: Arc<dyn SeafowlContext>,
    command: &str,
    mut output: W,
) where
    W: Write,
{
    // TODO when https://github.com/splitgraph/seafowl/issues/48 is implemented: run this
    // without splitting on the semicolon (which can also be a legitimate part of a query)
    for s in command.split(';') {
        if s.trim() == "" {
            continue;
        }
        async {
            let physical = context.plan_query(s).await?;
            let batches = context.collect(physical).await?;

            let mut writer = LineDelimitedWriter::new(&mut output);
            writer.write_batches(&batches)?;
            writer.finish()
        }
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::run_one_off_command;
    use crate::context::test_utils::in_memory_context;

    #[tokio::test]
    async fn test_command_splitting() {
        let mut buf = Vec::new();
        let context = in_memory_context().await;

        run_one_off_command(Arc::from(context), "SELECT 1; SELECT 1", &mut buf).await;

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "{\"Int64(1)\":1}\n{\"Int64(1)\":1}\n"
        );
    }
}
