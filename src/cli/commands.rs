use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::str::FromStr;
use std::sync::Arc;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

/// Commands available inside the CLI
#[derive(Debug, EnumIter)]
pub enum Command {
    Quit,
    Help,
    ListTables,
    DescribeTable(String),
}

impl Command {
    fn get_name_and_description(&self) -> (&'static str, &'static str) {
        match self {
            Self::Quit => ("\\q", "quit seafowl cli"),
            Self::ListTables => ("\\d", "list tables"),
            Self::DescribeTable(_) => ("\\d name", "describe table"),
            Self::Help => ("\\?", "help"),
        }
    }
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (c, arg) = if let Some((a, b)) = s.split_once(' ') {
            (a, Some(b))
        } else {
            (s, None)
        };
        Ok(match (c, arg) {
            ("q", None) | ("quit", None) => Self::Quit,
            ("d", None) => Self::ListTables,
            ("d", Some(name)) => Self::DescribeTable(name.into()),
            ("?", None) => Self::Help,
            _ => return Err(()),
        })
    }
}

pub fn all_commands_info() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Command", DataType::Utf8, false),
        Field::new("Description", DataType::Utf8, false),
    ]));
    let (names, description): (Vec<&str>, Vec<&str>) = Command::iter()
        .map(|c| c.get_name_and_description())
        .unzip();
    RecordBatch::try_new(
        schema,
        [names, description]
            .into_iter()
            .map(|i| Arc::new(StringArray::from(i)) as ArrayRef)
            .collect::<Vec<_>>(),
    )
    .expect("This should not fail")
}
