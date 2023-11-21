use std::str::FromStr;

/// Commands available inside the CLI
#[derive(Debug)]
pub enum Command {
    Quit,
    Help,
    ListTables,
    DescribeTable(String),
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
