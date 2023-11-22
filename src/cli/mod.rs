mod commands;
mod helper;

use crate::cli::commands::all_commands_info;
use crate::context::{DefaultSeafowlContext, SeafowlContext};
use arrow::util::pretty::pretty_format_batches_with_options;
use commands::Command;
use datafusion_common::Result;
use helper::CliHelper;
use rustyline::{error::ReadlineError, Editor};
use std::sync::Arc;
use std::time::Instant;

pub struct SeafowlCli {
    ctx: Arc<DefaultSeafowlContext>,
}

impl SeafowlCli {
    // Instantiate new CLI instance
    pub fn new(ctx: Arc<DefaultSeafowlContext>) -> Self {
        SeafowlCli { ctx }
    }

    // Interactive loop for running commands from a CLI
    pub async fn repl_loop(&self) -> rustyline::Result<()> {
        let mut rl = Editor::new()?;
        rl.set_helper(Some(CliHelper {}));
        rl.load_history(".history").ok();

        loop {
            match rl.readline(format!("{}> ", self.ctx.database).as_str()) {
                Ok(line) if line.starts_with('\\') => {
                    rl.add_history_entry(line.trim_end())?;
                    let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                    if let Ok(cmd) = &command[1..].parse::<Command>() {
                        match cmd {
                            Command::Quit => break,
                            _ => {
                                if let Err(e) = self.handle_command(cmd).await {
                                    eprintln!("{e}")
                                }
                            }
                        }
                    } else {
                        eprintln!("'\\{}' is not a valid command", &line[1..]);
                    }
                }
                Ok(line) => {
                    rl.add_history_entry(line.trim_end())?;

                    match self.exec_and_print(&line).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("{err}"),
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("\\q");
                    break;
                }
                Err(err) => {
                    eprintln!("Error while reading input: {err:?}",);
                    break;
                }
            }
        }

        rl.save_history(".history")
    }

    // Handle a client command
    async fn handle_command(&self, cmd: &Command) -> Result<()> {
        match cmd {
            Command::Help => {
                let help = all_commands_info();
                println!(
                    "{}",
                    pretty_format_batches_with_options(&[help], &Default::default())?
                );
                Ok(())
            }
            Command::ListTables => self.exec_and_print("SHOW TABLES").await,
            Command::DescribeTable(name) => {
                self.exec_and_print(&format!("SHOW COLUMNS FROM {name}"))
                    .await
            }
            Command::Quit => {
                panic!("Unexpected quit, this should be handled in the repl loop")
            }
        }
    }

    // Execute provided statement(s) and print the output
    async fn exec_and_print(&self, query: &str) -> Result<()> {
        let now = Instant::now();
        // Parse queries into statements
        let statements = self.ctx.parse_query(query).await?;

        // Generate physical plans from the statements
        let mut plans = vec![];
        for statement in statements {
            let logical = self
                .ctx
                .create_logical_plan_from_statement(statement)
                .await?;
            plans.push(self.ctx.create_physical_plan(&logical).await?);
        }

        // Collect batches and print them
        for plan in plans {
            let batches = self.ctx.collect(plan).await?;
            if !batches.is_empty() {
                println!(
                    "{}",
                    pretty_format_batches_with_options(&batches, &Default::default())?
                );
            }
        }
        println!("Time: {:.3}s", now.elapsed().as_secs_f64());

        Ok(())
    }
}
