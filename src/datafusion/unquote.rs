use sqlparser::ast::{
    AlterTableOperation, Ident, ObjectName, ObjectType, SchemaName, Statement,
    TableFactor, TableWithJoins,
};

pub fn remove_quotes_from_string(possibly_quoted_name: &String) -> String {
    possibly_quoted_name.trim_matches('"').to_string()
}

pub fn remove_quotes_from_ident(possibly_quoted_name: &Ident) -> Ident {
    Ident::new(remove_quotes_from_string(&possibly_quoted_name.value))
}

pub fn remove_quotes_from_indents(column_names: &Vec<Ident>) -> Vec<Ident> {
    column_names
        .iter()
        .map(|col| remove_quotes_from_ident(col))
        .collect()
}

pub fn remove_quotes_from_object_name(name: &ObjectName) -> ObjectName {
    ObjectName(remove_quotes_from_indents(&name.0))
}

pub fn remove_quotes_from_schema_name(name: &SchemaName) -> SchemaName {
    match name {
        SchemaName::Simple(simple_name) => {
            SchemaName::Simple(remove_quotes_from_object_name(&simple_name))
        }
        SchemaName::UnnamedAuthorization(unnamed) => {
            SchemaName::UnnamedAuthorization(unnamed.clone())
        }
        SchemaName::NamedAuthorization(object_name, ident) => {
            SchemaName::NamedAuthorization(
                remove_quotes_from_object_name(&object_name),
                ident.clone(),
            )
        }
    }
}

pub fn remove_quotes_from_alter_table_operation(
    operation: AlterTableOperation,
) -> AlterTableOperation {
    match operation {
        AlterTableOperation::DropConstraint {
            if_exists,
            name,
            cascade,
        } => AlterTableOperation::DropConstraint {
            if_exists,
            name: remove_quotes_from_ident(&name),
            cascade,
        },
        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            cascade,
        } => AlterTableOperation::DropColumn {
            column_name: remove_quotes_from_ident(&column_name),
            if_exists,
            cascade,
        },
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => AlterTableOperation::RenameColumn {
            old_column_name: remove_quotes_from_ident(&old_column_name),
            new_column_name: remove_quotes_from_ident(&new_column_name),
        },
        AlterTableOperation::RenameTable { table_name } => {
            AlterTableOperation::RenameTable {
                table_name: remove_quotes_from_object_name(&table_name),
            }
        }
        AlterTableOperation::ChangeColumn {
            old_name,
            new_name,
            data_type,
            options,
        } => AlterTableOperation::ChangeColumn {
            old_name: remove_quotes_from_ident(&old_name),
            new_name: remove_quotes_from_ident(&new_name),
            data_type,
            options,
        },
        AlterTableOperation::RenameConstraint { old_name, new_name } => {
            AlterTableOperation::RenameConstraint {
                old_name: remove_quotes_from_ident(&old_name),
                new_name: remove_quotes_from_ident(&new_name),
            }
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            AlterTableOperation::AlterColumn {
                column_name: remove_quotes_from_ident(&column_name),
                op,
            }
        }
        AlterTableOperation::AddConstraint(_)
        | AlterTableOperation::DropPartitions { .. }
        | AlterTableOperation::DropPrimaryKey { .. }
        | AlterTableOperation::AddColumn { .. }
        | AlterTableOperation::RenamePartitions { .. }
        | AlterTableOperation::AddPartitions { .. } => operation,
    }
}

pub fn remove_quotes_from_statement(
    statement: sqlparser::ast::Statement,
) -> sqlparser::ast::Statement {
    match statement {
        Statement::Analyze {
            table_name,
            partitions,
            for_columns,
            columns,
            cache_metadata,
            noscan,
            compute_statistics,
        } => Statement::Analyze {
            table_name: remove_quotes_from_object_name(&table_name),
            partitions,
            for_columns,
            columns: remove_quotes_from_indents(&columns),
            cache_metadata,
            noscan,
            compute_statistics,
        },
        // TODO: Query
        Statement::Query { .. } => statement,
        Statement::Truncate {
            table_name,
            partitions,
        } => Statement::Truncate {
            table_name: remove_quotes_from_object_name(&table_name),
            partitions,
        },
        Statement::Insert {
            or,
            into,
            table_name,
            columns,
            overwrite,
            source,
            partitioned,
            after_columns,
            table,
            on,
            returning,
        } => Statement::Insert {
            or,
            into,
            table_name: remove_quotes_from_object_name(&table_name),
            columns: remove_quotes_from_indents(&columns),
            overwrite,
            source,
            partitioned,
            after_columns,
            table,
            on,
            returning,
        },
        Statement::Copy {
            table_name,
            columns,
            to,
            target,
            options,
            legacy_options,
            values,
        } => Statement::Copy {
            table_name: remove_quotes_from_object_name(&table_name),
            columns: remove_quotes_from_indents(&columns),
            to,
            target,
            options,
            legacy_options,
            values,
        },
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        } => Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        },
        Statement::Delete {
            table_name,
            using,
            selection,
            returning,
        } => Statement::Delete {
            table_name, // TODO
            using,
            selection,
            returning,
        },
        Statement::CreateView {
            or_replace,
            materialized,
            name,
            columns,
            query,
            with_options,
        } => Statement::CreateView {
            or_replace,
            materialized,
            name: remove_quotes_from_object_name(&name),
            columns: remove_quotes_from_indents(&columns),
            query,
            with_options,
        },
        Statement::CreateTable {
            or_replace,
            temporary,
            external,
            global,
            if_not_exists,
            name,
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            table_properties,
            with_options,
            file_format,
            location,
            query,
            without_rowid,
            like,
            clone,
            engine,
            default_charset,
            collation,
            on_commit,
            on_cluster,
        } => Statement::CreateTable {
            or_replace,
            temporary,
            external,
            global,
            if_not_exists,
            name: remove_quotes_from_object_name(&name),
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            table_properties,
            with_options,
            file_format,
            location,
            query,
            without_rowid,
            like,
            clone,
            engine,
            default_charset,
            collation,
            on_commit,
            on_cluster,
        },
        Statement::CreateVirtualTable {
            name,
            if_not_exists,
            module_name,
            module_args,
        } => Statement::CreateVirtualTable {
            name: remove_quotes_from_object_name(&name),
            if_not_exists,
            module_name,
            module_args,
        },
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            unique,
            if_not_exists,
        } => Statement::CreateIndex {
            name: remove_quotes_from_object_name(&name),
            table_name,
            columns,
            unique,
            if_not_exists,
        },

        Statement::AlterTable { name, operation } => Statement::AlterTable {
            name: remove_quotes_from_object_name(&name),
            operation: remove_quotes_from_alter_table_operation(operation),
        },
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
        } => Statement::Drop {
            object_type,
            if_exists,
            names: names
                .iter()
                .map(|n| remove_quotes_from_object_name(&n))
                .collect(),
            cascade,
            restrict,
            purge,
        },
        Statement::Declare {
            name,
            binary,
            sensitive,
            scroll,
            hold,
            query,
        } => Statement::Declare {
            name: remove_quotes_from_ident(&name),
            binary,
            sensitive,
            scroll,
            hold,
            query,
        },
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => Statement::CreateSchema {
            schema_name: remove_quotes_from_schema_name(&schema_name),
            if_not_exists,
        },
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location,
            managed_location,
        } => Statement::CreateDatabase {
            db_name: remove_quotes_from_object_name(&db_name),
            if_not_exists,
            location,
            managed_location,
        },
        Statement::CreateFunction {
            temporary,
            name,
            class_name,
            using,
        } => Statement::CreateFunction {
            temporary,
            name: remove_quotes_from_object_name(&name),
            class_name,
            using,
        },
        // Don't change these SQL Statement types:
        Statement::CreateRole { .. }
        | Statement::Msck { .. }
        | Statement::Directory { .. }
        | Statement::Close { .. }
        | Statement::Fetch { .. }
        | Statement::Discard { .. }
        | Statement::SetRole { .. }
        | Statement::SetVariable { .. }
        | Statement::SetNames { .. }
        | Statement::SetNamesDefault { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowCollation { .. }
        | Statement::Use { .. }
        | Statement::StartTransaction { .. }
        | Statement::SetTransaction { .. }
        | Statement::Comment { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Assert { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. }
        | Statement::Deallocate { .. }
        | Statement::Execute { .. }
        | Statement::Prepare { .. }
        | Statement::Kill { .. }
        | Statement::ExplainTable { .. }
        | Statement::Explain { .. }
        | Statement::Savepoint { .. }
        | Statement::Merge { .. }
        | Statement::Cache { .. }
        | Statement::UNCache { .. }
        | Statement::CreateSequence { .. } => statement,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use sqlparser::ast::{Ident, ObjectName, SchemaName};

    use crate::datafusion::unquote::{
        remove_quotes_from_object_name, remove_quotes_from_schema_name,
        remove_quotes_from_string,
    };

    #[rstest]
    fn test_remove_quotes_from_string() {
        assert!(remove_quotes_from_string(&String::from("\"asdf\"")) == "asdf");
    }

    #[rstest]
    fn test_remove_quotes_from_object_name() {
        assert!(
            remove_quotes_from_object_name(&ObjectName(vec![Ident::new("\"table\"")]))
                == ObjectName(vec![Ident::new("table")])
        );
        assert!(
            remove_quotes_from_object_name(&ObjectName(vec![
                Ident::new("\"collection\""),
                Ident::new("\"table\"")
            ])) == ObjectName(vec![Ident::new("collection"), Ident::new("table")])
        );
    }

    #[rstest]
    fn test_remove_quotes_from_schema_name() {
        assert!(
            remove_quotes_from_schema_name(&SchemaName::Simple(ObjectName(vec![
                Ident::new("\"namespace/repository\"")
            ]))) == SchemaName::Simple(ObjectName(vec![Ident::new(
                "namespace/repository"
            )]))
        );
    }
}
