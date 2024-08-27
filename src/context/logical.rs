use crate::catalog::DEFAULT_SCHEMA;
use crate::context::SeafowlContext;
use crate::datafusion::parser::{DFParser, Statement as DFStatement, CONVERT_TO_DELTA};
use crate::datafusion::utils::build_schema;
use crate::nodes::Truncate;
use crate::wasm_udf::data_types::CreateFunctionDetails;
use crate::{
    nodes::{
        ConvertTable, CreateFunction, CreateTable, DropFunction, RenameTable,
        SeafowlExtensionNode, Vacuum,
    },
    version::TableVersionProcessor,
};

use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError as Error, Result};
use datafusion::execution::context::SessionState;
use datafusion::optimizer::analyzer::Analyzer;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{CopyToSource, CopyToStatement};
use datafusion_common::TableReference;
use datafusion_expr::logical_plan::{Extension, LogicalPlan};
use deltalake::DeltaTable;
use itertools::Itertools;
use sqlparser::ast::{
    AlterTableOperation, CreateFunctionBody, CreateTable as CreateTableSql,
    Expr as SqlExpr, Expr, Insert, ObjectType, Query, Statement, TableFactor,
    TableWithJoins, Value, VisitMut,
};
use std::sync::Arc;
use tracing::debug;

pub fn is_read_only(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Copy(_)
    )
}

pub fn is_statement_read_only(statement: &DFStatement) -> bool {
    if let DFStatement::Statement(s) = statement {
        matches!(
            **s,
            Statement::Query(_)
                | Statement::Explain { .. }
                | Statement::ShowTables { .. }
        )
    } else {
        false
    }
}

impl SeafowlContext {
    pub async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = self.parse_query(sql).await?;

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        self.create_logical_plan_from_statement(statements.pop().unwrap())
            .await
    }

    pub async fn parse_query(&self, sql: &str) -> Result<Vec<DFStatement>> {
        Ok(DFParser::parse_sql(sql)?.into_iter().collect_vec())
    }

    pub async fn create_logical_plan_from_statement(
        &self,
        statement: DFStatement,
    ) -> Result<LogicalPlan> {
        // Reload the schema before planning a query
        // TODO: A couple of possible optimisations here:
        // 1. Do a visit of the statement AST, and then load the metadata for only the referenced identifiers.
        // 2. No need to load metadata for the TableProvider implementation maps when instantiating SqlToRel,
        //    since it's sufficient to have metadata for TableSource implementation in the logical query
        //    planning phase. We could use a lighter structure for that, and implement `ContextProvider` for
        //    it rather than for SeafowlContext.
        self.reload_schema().await?;

        // Create a mutable clone of the statement so that we can rewrite table names if we encounter
        // time travel syntax.
        // Alternatively, this could be done without the `mut`, except then we'd need to construct
        // and return a new `DFStatement` after the rewrite. In case of `Statement::Query` this is
        // straight-forward as that enum variant contains a struct, however `Statement::Insert` and
        // `Statement::CreateTable` wrap dozens of fields in curly braces, and we'd need to capture
        // and re-populate each of those, so it would be very verbose.
        // TODO: If `sqlparser-rs` encapsulates these fields inside a struct at some point remove
        // the mut and go with @ capture and de-structuring to pass along other fields unchanged.
        let mut stmt = statement.clone();

        match &mut stmt {
            DFStatement::Statement(ref mut s) => match &mut **s {
                Statement::Query(ref mut query) => {
                    let state = self.rewrite_time_travel_query(query).await?;
                    state.statement_to_plan(stmt).await
                }
                // Delegate generic queries to the basic DataFusion logical planner
                // (though note EXPLAIN [our custom query] will mean we have to implement EXPLAIN ourselves)
                Statement::Explain { .. }
                | Statement::ExplainTable { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateView { .. }
                | Statement::CreateDatabase { .. } => self.inner.state().statement_to_plan(stmt).await,
                Statement::Insert(Insert{ source: Some(ref mut source), .. }) => {
                    let state = self.rewrite_time_travel_query(source).await?;
                    let plan = state.statement_to_plan(stmt).await?;
                    state.optimize(&plan)
                }
                Statement::Update {
                    table: TableWithJoins {relation: TableFactor::Table { alias: None, args: None, with_hints, .. }, joins },
                    ..
                }
                // We only support the most basic form of UPDATE (no aliases or FROM or joins)
                if with_hints.is_empty() && joins.is_empty() => {
                    let state = self.inner.state();
                    let plan = state.statement_to_plan(stmt).await?;

                    // Create a custom optimizer to avoid mangling effects of some optimizers (like
                    // `CommonSubexprEliminate`) which can add nested Projection plans and rewrite
                    // expressions.
                    // We also need to do a analyze round beforehand for type coercion.
                    let analyzer = Analyzer::new();
                    let plan = analyzer.execute_and_check(
                        plan,
                        self.inner.copied_config().options(),
                        |_, _| {},
                    )?;
                    let optimizer = Optimizer::with_rules(
                        vec![
                            Arc::new(SimplifyExpressions::new()),
                        ]
                    );
                    let config = OptimizerContext::default();
                    optimizer.optimize(plan, &config, |plan: &LogicalPlan, rule: &dyn OptimizerRule| {
                        debug!(
                                "After applying rule '{}':\n{}\n",
                                rule.name(),
                                plan.display_indent()
                            )
                    }
                    )
                },
                Statement::Delete{ .. } => {
                    let state = self.inner.state();
                    let plan = state.statement_to_plan(stmt).await?;
                    state.optimize(&plan)
                }
                Statement::Drop { object_type: ObjectType::Table | ObjectType::Schema, .. } => self.inner.state().statement_to_plan(stmt).await,
                // CREATE TABLE (create empty table with columns)
                Statement::CreateTable(CreateTableSql {
                    query: None,
                    name,
                    columns,
                    constraints,
                    table_properties,
                    with_options,
                    if_not_exists,
                    or_replace: _,
                    ..
                }) if constraints.is_empty()
                    && table_properties.is_empty()
                    && with_options.is_empty() =>
                    {
                        let schema = build_schema(columns.to_vec())?;
                        Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SeafowlExtensionNode::CreateTable(CreateTable {
                                schema,
                                name: name.to_string(),
                                if_not_exists: *if_not_exists,
                                output_schema: Arc::new(DFSchema::empty())
                            })),
                        }))
                    },

                // ALTER TABLE ... RENAME TO
                Statement::AlterTable { name, operations, ..} => {
                    let old_table_name = name.to_string();
                    let new_table_name = match operations[..] {
                        [AlterTableOperation::RenameTable {ref table_name}] => table_name.to_string(),
                        _ => return Err(Error::Plan(
                            "Unsupported ALTER TABLE statement".to_string()
                        ))
                    };

                    if self.inner.table_provider(old_table_name.to_owned()).await.is_err() {
                        return Err(Error::Plan(
                            format!("Source table {old_table_name:?} doesn't exist")
                        ))
                    } else if self.inner.table_provider(new_table_name.to_owned()).await.is_ok() {
                        return Err(Error::Plan(
                            format!("Target table {new_table_name:?} already exists")
                        ))
                    }

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::RenameTable(RenameTable {
                            old_name: old_table_name,
                            new_name: new_table_name,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }

                // Other CREATE TABLE: SqlToRel only allows CreateTableAs statements and makes
                // a CreateMemoryTable node. We're fine with that, but we'll execute it differently.
                Statement::CreateTable(CreateTableSql { query: Some(ref mut input), .. })
                => {
                    let state = self.rewrite_time_travel_query(input).await?;
                    state.statement_to_plan(stmt).await
                },

                Statement::CreateFunction {
                    or_replace,
                    temporary: false,
                    name,
                    function_body: Some(CreateFunctionBody::AsBeforeOptions(Expr::Value(Value::SingleQuotedString(details)))),
                    ..
                } => {
                    // We abuse the fact that in CREATE FUNCTION AS [class_name], class_name can be an arbitrary string
                    // and so we can get the user to put some JSON in there
                    let function_details: CreateFunctionDetails = serde_json::from_str(details)
                        .map_err(|e| {
                            Error::Execution(format!("Error parsing UDF details: {e:?}"))
                        })?;

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::CreateFunction(CreateFunction {
                            or_replace: *or_replace,
                            name: name.to_string(),
                            details: function_details,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                },
                Statement::Truncate { table: false, table_name, partitions, .. } => {
                    let table_name = if partitions.is_none() {
                        Some(table_name.to_string())
                    } else {
                        None
                    };

                    let mut database = None;
                    if let Some(sql_exprs) = partitions {
                        if let [SqlExpr::Identifier(name)] = sql_exprs.as_slice() {
                            database = Some(name.value.clone());
                        }
                    }

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Vacuum(Vacuum {
                            database,
                            table_name,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }
                Statement::Truncate { table: true, table_name, .. } => {
                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Truncate(Truncate {
                            table_name: table_name.to_string(),
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }
                Statement::DropFunction{
                    if_exists,
                    func_desc,
                    option: _
                } => {
                    let func_names: Vec<String> =
                        func_desc.iter().map(|desc| desc.name.to_string()).collect();
                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::DropFunction(DropFunction {
                            if_exists: *if_exists,
                            func_names,
                            output_schema: Arc::new(DFSchema::empty()),
                        }))
                    }))
                }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {s:?}"
                ))),
            },
            DFStatement::CopyTo(CopyToStatement {
                ref mut source,
                options,
                ..
            }) if !options.contains(&CONVERT_TO_DELTA) => {
                let state = if let CopyToSource::Query(ref mut query) = source {
                    self.rewrite_time_travel_query(query).await?
                } else {
                    self.inner.state()
                };
                state.statement_to_plan(stmt).await
            }
            DFStatement::CopyTo(CopyToStatement {
                source: CopyToSource::Relation(table_name),
                target,
                options,
                ..
            }) if options.contains(&CONVERT_TO_DELTA) => {
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(SeafowlExtensionNode::ConvertTable(ConvertTable {
                        location: target.clone(),
                        name: table_name.to_string(),
                        output_schema: Arc::new(DFSchema::empty()),
                    })),
                }))
            }
            DFStatement::CreateExternalTable(_) => {
                self.inner.state().statement_to_plan(stmt).await
            }
            DFStatement::CopyTo(_) | DFStatement::Explain(_) => {
                Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {statement:?}"
                )))
            }
        }
    }

    // Determine if some of the tables reference a non-latest version using table function syntax.
    // If so, rename the tables in the query by appending the explicit version to the name, and add
    // it to the schema provider's map inside a new session state.
    // Should become obsolete once `sqlparser-rs` introduces support for some form of the `AS OF`
    // clause: https://en.wikipedia.org/wiki/SQL:2011.
    async fn rewrite_time_travel_query(&self, q: &mut Query) -> Result<SessionState> {
        let mut version_processor = TableVersionProcessor::new(
            self.default_catalog.clone(),
            DEFAULT_SCHEMA.to_string(),
        );
        q.visit(&mut version_processor);

        if version_processor.table_versions.is_empty() {
            // No time-travel syntax detected, just return the regular session state
            return Ok(self.inner.state());
        }

        debug!("Time travel query rewritten to: {}", q);

        // Create a new session context and session state, to avoid potential race
        // conditions leading to schema provider map leaking into other queries (and
        // thus polluting e.g. the information_schema output), or even worse reloading
        // the map and having the versioned query fail during execution.
        let session_ctx = SessionContext::new_with_state(self.inner.state());

        for (table, version) in &version_processor.table_versions {
            let name_with_version =
                TableVersionProcessor::table_with_version(table, version);

            let full_table_name = table.to_string();
            let mut resolved_ref = TableReference::from(full_table_name.as_str())
                .resolve(&self.default_catalog, &self.default_schema);

            // We only support datetime DeltaTable version specification for start
            let table_uuid = self.get_table_uuid(resolved_ref.clone()).await?;
            let table_log_store = self
                .get_internal_object_store()?
                .get_log_store(&table_uuid.to_string());
            let datetime = TableVersionProcessor::version_to_datetime(version)?;

            let mut delta_table = DeltaTable::new(table_log_store, Default::default());
            delta_table.load_with_datetime(datetime).await?;
            let table_provider_for_version = Arc::from(delta_table);

            resolved_ref.table = Arc::from(name_with_version.as_str());

            if !session_ctx.table_exist(resolved_ref.clone())? {
                session_ctx.register_table(resolved_ref, table_provider_for_version)?;
            }
        }

        Ok(session_ctx.state())
    }
}

#[cfg(test)]
mod tests {
    use crate::context::test_utils::in_memory_context_with_test_db;

    #[tokio::test]
    async fn test_plan_insert_normal() {
        let ctx = in_memory_context_with_test_db().await;

        let plan = ctx
            .create_logical_plan(
                "INSERT INTO testcol.some_table (date, value) VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        assert_eq!(
            format!("{plan}"),
            "Dml: op=[Insert Into] table=[testcol.some_table]\
            \n  Projection: CAST(column1 AS Date32) AS date, CAST(column2 AS Float64) AS value\
            \n    Values: (Utf8(\"2022-01-01T12:00:00\"), Int64(42))"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_renaming() {
        let ctx = in_memory_context_with_test_db().await;

        let plan = ctx
            .create_logical_plan(
                "INSERT INTO testcol.some_table (date, value)
                SELECT \"date\" AS my_date, \"value\" AS my_value FROM testdb.testcol.some_table",
            )
            .await
            .unwrap();

        assert_eq!(format!("{plan}"), "Dml: op=[Insert Into] table=[testcol.some_table]\
        \n  Projection: testdb.testcol.some_table.date AS date, testdb.testcol.some_table.value AS value\
        \n    TableScan: testdb.testcol.some_table projection=[date, value]");
    }

    async fn get_logical_plan(query: &str) -> String {
        let ctx = in_memory_context_with_test_db().await;

        let plan = ctx.create_logical_plan(query).await.unwrap();
        format!("{plan}")
    }

    #[tokio::test]
    async fn test_plan_create_schema_name_in_quotes() {
        assert_eq!(
            get_logical_plan("CREATE SCHEMA schema_name;").await,
            "CreateCatalogSchema: \"schema_name\""
        );
        assert_eq!(
            get_logical_plan("CREATE SCHEMA \"schema_name\";").await,
            "CreateCatalogSchema: \"schema_name\""
        );
    }

    #[tokio::test]
    async fn test_plan_rename_table_name_in_quotes() {
        assert_eq!(
            get_logical_plan("ALTER TABLE \"testcol\".\"some_table\" RENAME TO \"testcol\".\"some_table_2\"").await,
            "RenameTable: \"testcol\".\"some_table\" to \"testcol\".\"some_table_2\""
        );
    }

    #[tokio::test]
    async fn test_plan_drop_table_name_in_quotes() {
        assert_eq!(
            get_logical_plan("DROP TABLE \"testcol\".\"some_table\"").await,
            "DropTable: Partial { schema: \"testcol\", table: \"some_table\" } if not exist:=false"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_all() {
        let ctx = in_memory_context_with_test_db().await;

        let plan = ctx
            .create_logical_plan(
                "INSERT INTO testcol.some_table VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        assert_eq!(
            format!("{plan}"),
            "Dml: op=[Insert Into] table=[testcol.some_table]\
            \n  Projection: CAST(column1 AS Date32) AS date, CAST(column2 AS Float64) AS value\
            \n    Values: (Utf8(\"2022-01-01T12:00:00\"), Int64(42))"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_missing_table() {
        let context = in_memory_context_with_test_db().await;

        let err = context
            .create_logical_plan("INSERT INTO testcol.missing_table VALUES(1, 2, 3)")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: table 'testdb.testcol.missing_table' not found"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_values_wrong_number() {
        let ctx = in_memory_context_with_test_db().await;

        let err = ctx
            .create_logical_plan(
                "INSERT INTO testcol.some_table VALUES('2022-01-01T12:00:00')",
            )
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Column count doesn't match insert query!"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_values_duplicate_columns() {
        let ctx = in_memory_context_with_test_db().await;

        let err = ctx
            .create_logical_plan("INSERT INTO testcol.some_table(date, date, value) VALUES('2022-01-01T12:00:00', '2022-01-01T12:00:00', 42)")
            .await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Schema error: Schema contains duplicate unqualified field name date"
        );
    }
}
