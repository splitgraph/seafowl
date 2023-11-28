use rustyline::completion::Completer;
use rustyline::completion::Pair;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::ValidationContext;
use rustyline::validate::ValidationResult;
use rustyline::validate::Validator;
use rustyline::Helper;
use rustyline::Result;

pub struct CliHelper {}

// The accompanying helper for SeafowlCli.
// For now only supports multi-line statements through the `Validator` override.
impl CliHelper {
    fn validate_input(&self, input: &str) -> Result<ValidationResult> {
        if input.ends_with(';') || input.starts_with('\\') {
            // command
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

impl Highlighter for CliHelper {}

impl Hinter for CliHelper {
    type Hint = String;
}

impl Completer for CliHelper {
    type Candidate = Pair;
}

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        let input = ctx.input().trim_end();
        self.validate_input(input)
    }
}

impl Helper for CliHelper {}
