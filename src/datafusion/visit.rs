// Adapted from https://github.com/sqlparser-rs/sqlparser-rs/pull/114 (with a couple of tweaks to
// align it with the latest AST definition, and make everything mut) as a way to enable table name
// rewrites. Note that only the nodes contained under the sqlparser::ast::Statement::Query are kept,
// other functions have been pruned away for ease of maintenance.
// It's worth keeping an eye on what happens with https://github.com/sqlparser-rs/sqlparser-rs/pull/601,
// as it would simplify the rewrite procedure a lot. Also, maybe it's worth trying to autogenerate
// this ourselves?

use sqlparser::ast::{
    BinaryOperator, Cte, DataType, DateTimeField, Expr, Function, FunctionArg,
    FunctionArgExpr, Ident, Join, JoinConstraint, JoinOperator, ObjectName, OrderByExpr,
    Query, Select, SelectItem, SetExpr, SetOperator, TableAlias, TableFactor,
    TableWithJoins, UnaryOperator, Value, Values, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowSpec,
};

/// A trait that represents a visitor that walks through a SQL AST.
///
/// Each function corresponds to a node in the SQL AST, and has a default
/// implementation that visits all of its child nodes. Implementors of this
/// trait can override functions as desired to hook into AST traversal without
/// writing code to traverse the entire AST.
pub trait VisitorMut<'ast> {
    fn visit_query(&mut self, query: &'ast mut Query) {
        visit_query(self, query)
    }

    fn visit_cte(&mut self, cte: &'ast mut Cte) {
        visit_cte(self, cte)
    }

    fn visit_select(&mut self, select: &'ast mut Select) {
        visit_select(self, select)
    }

    fn visit_select_item(&mut self, select_item: &'ast mut SelectItem) {
        visit_select_item(self, select_item)
    }

    fn visit_table_with_joins(&mut self, table_with_joins: &'ast mut TableWithJoins) {
        visit_table_with_joins(self, table_with_joins)
    }

    fn visit_table_factor(&mut self, table_factor: &'ast mut TableFactor) {
        visit_table_factor(self, table_factor)
    }

    fn visit_table_table_factor(
        &mut self,
        name: &'ast mut ObjectName,
        alias: Option<&'ast mut TableAlias>,
        args: &'ast mut Option<Vec<FunctionArg>>,
        with_hints: &'ast mut [Expr],
    ) {
        visit_table_table_factor(self, name, alias, args, with_hints)
    }

    fn visit_derived_table_factor(
        &mut self,
        lateral: bool,
        subquery: &'ast mut Query,
        alias: Option<&'ast mut TableAlias>,
    ) {
        visit_derived_table_factor(self, lateral, subquery, alias)
    }

    fn visit_nested_join_table_factor(
        &mut self,
        table_with_joins: &'ast mut TableWithJoins,
    ) {
        visit_nested_join_table_factor(self, table_with_joins)
    }

    fn visit_table_alias(&mut self, alias: &'ast mut TableAlias) {
        visit_table_alias(self, alias)
    }

    fn visit_join(&mut self, join: &'ast mut Join) {
        visit_join(self, join)
    }

    fn visit_join_operator(&mut self, op: &'ast mut JoinOperator) {
        visit_join_operator(self, op)
    }

    fn visit_join_constraint(&mut self, constraint: &'ast mut JoinConstraint) {
        visit_join_constraint(self, constraint)
    }

    fn visit_where(&mut self, expr: &'ast mut Expr) {
        visit_where(self, expr)
    }

    fn visit_group_by(&mut self, exprs: &'ast mut [Expr]) {
        visit_group_by(self, exprs)
    }

    fn visit_having(&mut self, expr: &'ast mut Expr) {
        visit_having(self, expr)
    }

    fn visit_set_expr(&mut self, set_expr: &'ast mut SetExpr) {
        visit_set_expr(self, set_expr)
    }

    fn visit_set_operation(
        &mut self,
        left: &'ast mut SetExpr,
        op: &'ast mut SetOperator,
        right: &'ast mut SetExpr,
        all: bool,
    ) {
        visit_set_operation(self, left, op, right, all)
    }

    fn visit_set_operator(&mut self, _operator: &'ast mut SetOperator) {}

    fn visit_order_by(&mut self, order_by: &'ast mut OrderByExpr) {
        visit_order_by(self, order_by)
    }

    fn visit_limit(&mut self, expr: &'ast mut Expr) {
        visit_limit(self, expr)
    }

    fn visit_type(&mut self, _data_type: &'ast mut DataType) {}

    fn visit_expr(&mut self, expr: &'ast mut Expr) {
        visit_expr(self, expr)
    }

    fn visit_unnamed_expr(&mut self, expr: &'ast mut Expr) {
        visit_unnamed_expr(self, expr)
    }

    fn visit_expr_with_alias(&mut self, expr: &'ast mut Expr, alias: &'ast mut Ident) {
        visit_expr_with_alias(self, expr, alias)
    }

    fn visit_object_name(&mut self, object_name: &'ast mut ObjectName) {
        visit_object_name(self, object_name)
    }

    fn visit_ident(&mut self, _ident: &'ast mut Ident) {}

    fn visit_compound_identifier(&mut self, idents: &'ast mut [Ident]) {
        visit_compound_identifier(self, idents)
    }

    fn visit_wildcard(&mut self) {}

    fn visit_qualified_wildcard(&mut self, idents: &'ast mut [Ident]) {
        visit_qualified_wildcard(self, idents)
    }

    fn visit_is_null(&mut self, expr: &'ast mut Expr) {
        visit_is_null(self, expr)
    }

    fn visit_is_not_null(&mut self, expr: &'ast mut Expr) {
        visit_is_not_null(self, expr)
    }

    fn visit_in_list(
        &mut self,
        expr: &'ast mut Expr,
        list: &'ast mut [Expr],
        negated: bool,
    ) {
        visit_in_list(self, expr, list, negated)
    }

    fn visit_in_subquery(
        &mut self,
        expr: &'ast mut Expr,
        subquery: &'ast mut Query,
        negated: bool,
    ) {
        visit_in_subquery(self, expr, subquery, negated)
    }

    fn visit_between(
        &mut self,
        expr: &'ast mut Expr,
        low: &'ast mut Expr,
        high: &'ast mut Expr,
        negated: bool,
    ) {
        visit_between(self, expr, low, high, negated)
    }

    fn visit_binary_op(
        &mut self,
        left: &'ast mut Expr,
        op: &'ast mut BinaryOperator,
        right: &'ast mut Expr,
    ) {
        visit_binary_op(self, left, op, right)
    }

    fn visit_binary_operator(&mut self, _op: &'ast mut BinaryOperator) {}

    fn visit_unary_op(&mut self, expr: &'ast mut Expr, op: &'ast mut UnaryOperator) {
        visit_unary_op(self, expr, op)
    }

    fn visit_unary_operator(&mut self, _op: &'ast mut UnaryOperator) {}

    fn visit_cast(&mut self, expr: &'ast mut Expr, data_type: &'ast mut DataType) {
        visit_cast(self, expr, data_type)
    }

    fn visit_collate(&mut self, expr: &'ast mut Expr, collation: &'ast mut ObjectName) {
        visit_collate(self, expr, collation)
    }

    fn visit_extract(&mut self, field: &'ast mut DateTimeField, expr: &'ast mut Expr) {
        visit_extract(self, field, expr)
    }

    fn visit_date_time_field(&mut self, _field: &'ast mut DateTimeField) {}

    fn visit_nested(&mut self, expr: &'ast mut Expr) {
        visit_nested(self, expr)
    }

    fn visit_value(&mut self, _val: &'ast mut Value) {}

    fn visit_function(&mut self, func: &'ast mut Function) {
        visit_function(self, func)
    }

    fn visit_function_arg(&mut self, arg: &'ast mut FunctionArg) {
        visit_function_arg(self, arg)
    }

    fn visit_function_arg_named(
        &mut self,
        ident: &'ast mut Ident,
        func_arg_expr: &'ast mut FunctionArgExpr,
    ) {
        visit_function_arg_named(self, ident, func_arg_expr)
    }

    fn visit_function_arg_expression(
        &mut self,
        func_arg_expr: &'ast mut FunctionArgExpr,
    ) {
        visit_function_arg_expression(self, func_arg_expr)
    }

    fn visit_window_spec(&mut self, window_spec: &'ast mut WindowSpec) {
        visit_window_spec(self, window_spec)
    }

    fn visit_window_frame(&mut self, window_frame: &'ast mut WindowFrame) {
        visit_window_frame(self, window_frame)
    }

    fn visit_window_frame_units(
        &mut self,
        _window_frame_units: &'ast mut WindowFrameUnits,
    ) {
    }

    fn visit_window_frame_bound(
        &mut self,
        _window_frame_bound: &'ast mut WindowFrameBound,
    ) {
    }

    fn visit_case(
        &mut self,
        operand: Option<&'ast mut Expr>,
        conditions: &'ast mut [Expr],
        results: &'ast mut [Expr],
        else_result: Option<&'ast mut Expr>,
    ) {
        visit_case(self, operand, conditions, results, else_result)
    }

    fn visit_exists(&mut self, subquery: &'ast mut Query) {
        visit_exists(self, subquery)
    }

    fn visit_subquery(&mut self, subquery: &'ast mut Query) {
        visit_subquery(self, subquery)
    }

    fn visit_insert(
        &mut self,
        table_name: &'ast mut ObjectName,
        columns: &'ast mut [Ident],
        source: &'ast mut Query,
    ) {
        visit_insert(self, table_name, columns, source)
    }

    fn visit_values(&mut self, values: &'ast mut Values) {
        visit_values(self, values)
    }

    fn visit_values_row(&mut self, row: &'ast mut [Expr]) {
        visit_values_row(self, row)
    }
}

pub fn visit_query<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    query: &'ast mut Query,
) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            visitor.visit_cte(cte);
        }
    }
    visitor.visit_set_expr(&mut query.body);
    for order_by in &mut query.order_by {
        visitor.visit_order_by(order_by);
    }
    if let Some(limit) = &mut query.limit {
        visitor.visit_limit(limit);
    }
}

pub fn visit_cte<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    cte: &'ast mut Cte,
) {
    visitor.visit_table_alias(&mut cte.alias);
    visitor.visit_query(&mut cte.query);
}

pub fn visit_select<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    select: &'ast mut Select,
) {
    for select_item in &mut select.projection {
        visitor.visit_select_item(select_item)
    }
    for table_with_joins in &mut select.from {
        visitor.visit_table_with_joins(table_with_joins)
    }
    if let Some(selection) = &mut select.selection {
        visitor.visit_where(selection);
    }
    if !select.group_by.is_empty() {
        visitor.visit_group_by(&mut select.group_by);
    }
    if let Some(having) = &mut select.having {
        visitor.visit_having(having);
    }
}

pub fn visit_select_item<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    select_item: &'ast mut SelectItem,
) {
    match select_item {
        SelectItem::UnnamedExpr(expr) => visitor.visit_unnamed_expr(expr),
        SelectItem::ExprWithAlias { expr, alias } => {
            visitor.visit_expr_with_alias(expr, alias)
        }
        SelectItem::QualifiedWildcard(object_name) => {
            visitor.visit_qualified_wildcard(&mut object_name.0)
        }
        SelectItem::Wildcard => visitor.visit_wildcard(),
    }
}

pub fn visit_table_with_joins<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    table_with_joins: &'ast mut TableWithJoins,
) {
    visitor.visit_table_factor(&mut table_with_joins.relation);
    for join in &mut table_with_joins.joins {
        visitor.visit_join(join);
    }
}

pub fn visit_table_factor<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    table_factor: &'ast mut TableFactor,
) {
    match table_factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => visitor.visit_table_table_factor(name, alias.as_mut(), args, with_hints),
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => visitor.visit_derived_table_factor(*lateral, subquery, alias.as_mut()),
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            visitor.visit_nested_join_table_factor(table_with_joins);
            if let Some(alias) = alias {
                visitor.visit_table_alias(alias);
            }
        }
        _ => {}
    }
}

pub fn visit_table_table_factor<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast mut ObjectName,
    alias: Option<&'ast mut TableAlias>,
    args: &'ast mut Option<Vec<FunctionArg>>,
    with_hints: &'ast mut [Expr],
) {
    visitor.visit_object_name(name);

    if let Some(func_args) = args {
        for func_arg in func_args {
            visitor.visit_function_arg(func_arg);
        }
    }
    if let Some(alias) = alias {
        visitor.visit_table_alias(alias);
    }
    for expr in with_hints {
        visitor.visit_expr(expr);
    }
}

pub fn visit_derived_table_factor<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    _lateral: bool,
    subquery: &'ast mut Query,
    alias: Option<&'ast mut TableAlias>,
) {
    visitor.visit_subquery(subquery);
    if let Some(alias) = alias {
        visitor.visit_table_alias(alias);
    }
}

pub fn visit_nested_join_table_factor<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    table_with_joins: &'ast mut TableWithJoins,
) {
    visitor.visit_table_with_joins(table_with_joins);
}

pub fn visit_table_alias<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    alias: &'ast mut TableAlias,
) {
    visitor.visit_ident(&mut alias.name);
    for column in &mut alias.columns {
        visitor.visit_ident(column);
    }
}

pub fn visit_join<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    join: &'ast mut Join,
) {
    visitor.visit_table_factor(&mut join.relation);
    visitor.visit_join_operator(&mut join.join_operator);
}

pub fn visit_join_operator<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    op: &'ast mut JoinOperator,
) {
    match op {
        JoinOperator::Inner(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::LeftOuter(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::RightOuter(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::FullOuter(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {
        }
    }
}

pub fn visit_join_constraint<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    constraint: &'ast mut JoinConstraint,
) {
    match constraint {
        JoinConstraint::On(expr) => visitor.visit_expr(expr),
        JoinConstraint::Using(idents) => {
            for ident in idents {
                visitor.visit_ident(ident);
            }
        }
        _ => {}
    }
}

pub fn visit_where<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr);
}

pub fn visit_group_by<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    exprs: &'ast mut [Expr],
) {
    for expr in exprs {
        visitor.visit_expr(expr);
    }
}

pub fn visit_having<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr);
}

pub fn visit_set_expr<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    set_expr: &'ast mut SetExpr,
) {
    match set_expr {
        SetExpr::Select(select) => visitor.visit_select(select),
        SetExpr::Query(query) => visitor.visit_query(query),
        SetExpr::Values(values) => visitor.visit_values(values),
        SetExpr::SetOperation {
            left,
            op,
            right,
            all,
        } => visitor.visit_set_operation(left, op, right, *all),
        // TODO: There is also a new enum option, INSERT, which is actually a top level sqlparser
        // STATEMENT; we may need to enforce no table version specification there but that would
        // entail adding the visit_statement function and thus a bunch of unused code
        _ => {}
    }
}

pub fn visit_set_operation<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    left: &'ast mut SetExpr,
    op: &'ast mut SetOperator,
    right: &'ast mut SetExpr,
    _all: bool,
) {
    visitor.visit_set_expr(left);
    visitor.visit_set_operator(op);
    visitor.visit_set_expr(right);
}

pub fn visit_order_by<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    order_by: &'ast mut OrderByExpr,
) {
    visitor.visit_expr(&mut order_by.expr);
}

pub fn visit_limit<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr)
}

pub fn visit_expr<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    match expr {
        Expr::Identifier(ident) => visitor.visit_ident(ident),
        Expr::CompoundIdentifier(idents) => visitor.visit_compound_identifier(idents),
        Expr::IsNull(expr) => visitor.visit_is_null(expr),
        Expr::IsNotNull(expr) => visitor.visit_is_not_null(expr),
        Expr::InList {
            expr,
            list,
            negated,
        } => visitor.visit_in_list(expr, list, *negated),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => visitor.visit_in_subquery(expr, subquery, *negated),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => visitor.visit_between(expr, low, high, *negated),
        Expr::BinaryOp { left, op, right } => visitor.visit_binary_op(left, op, right),
        Expr::UnaryOp { expr, op } => visitor.visit_unary_op(expr, op),
        Expr::Cast { expr, data_type } => visitor.visit_cast(expr, data_type),
        Expr::Collate { expr, collation } => visitor.visit_collate(expr, collation),
        Expr::Extract { field, expr } => visitor.visit_extract(field, expr),
        Expr::Nested(expr) => visitor.visit_nested(expr),
        Expr::Value(val) => visitor.visit_value(val),
        Expr::Function(func) => visitor.visit_function(func),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => visitor.visit_case(
            operand.as_mut().map(|o| o.as_mut()),
            conditions,
            results,
            else_result.as_mut().map(|r| r.as_mut()),
        ),
        Expr::Exists {
            subquery,
            negated: _,
        } => visitor.visit_subquery(subquery),
        Expr::Subquery(subquery) => visitor.visit_subquery(subquery),
        _ => {}
    }
}

pub fn visit_unnamed_expr<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr);
}

pub fn visit_expr_with_alias<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    alias: &'ast mut Ident,
) {
    visitor.visit_expr(expr);
    visitor.visit_ident(alias);
}

pub fn visit_object_name<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    object_name: &'ast mut ObjectName,
) {
    for ident in &mut object_name.0 {
        visitor.visit_ident(ident)
    }
}

pub fn visit_compound_identifier<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    idents: &'ast mut [Ident],
) {
    for ident in idents {
        visitor.visit_ident(ident);
    }
}

pub fn visit_qualified_wildcard<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    idents: &'ast mut [Ident],
) {
    for ident in idents {
        visitor.visit_ident(ident);
    }
}

pub fn visit_is_null<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr);
}

pub fn visit_is_not_null<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr);
}

pub fn visit_in_list<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    list: &'ast mut [Expr],
    _negated: bool,
) {
    visitor.visit_expr(expr);
    for e in list {
        visitor.visit_expr(e);
    }
}

pub fn visit_in_subquery<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    subquery: &'ast mut Query,
    _negated: bool,
) {
    visitor.visit_expr(expr);
    visitor.visit_query(subquery);
}

pub fn visit_between<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    low: &'ast mut Expr,
    high: &'ast mut Expr,
    _negated: bool,
) {
    visitor.visit_expr(expr);
    visitor.visit_expr(low);
    visitor.visit_expr(high);
}

pub fn visit_binary_op<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    left: &'ast mut Expr,
    op: &'ast mut BinaryOperator,
    right: &'ast mut Expr,
) {
    visitor.visit_expr(left);
    visitor.visit_binary_operator(op);
    visitor.visit_expr(right);
}

pub fn visit_unary_op<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    op: &'ast mut UnaryOperator,
) {
    visitor.visit_expr(expr);
    visitor.visit_unary_operator(op);
}

pub fn visit_cast<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    data_type: &'ast mut DataType,
) {
    visitor.visit_expr(expr);
    visitor.visit_type(data_type);
}

pub fn visit_collate<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
    collation: &'ast mut ObjectName,
) {
    visitor.visit_expr(expr);
    visitor.visit_object_name(collation);
}

pub fn visit_extract<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    field: &'ast mut DateTimeField,
    expr: &'ast mut Expr,
) {
    visitor.visit_date_time_field(field);
    visitor.visit_expr(expr);
}

pub fn visit_nested<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast mut Expr,
) {
    visitor.visit_expr(expr);
}

pub fn visit_function<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    func: &'ast mut Function,
) {
    visitor.visit_object_name(&mut func.name);
    for arg in &mut func.args {
        visitor.visit_function_arg(arg);
    }
    if let Some(over) = &mut func.over {
        visitor.visit_window_spec(over);
    }
}

pub fn visit_function_arg<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    arg: &'ast mut FunctionArg,
) {
    match arg {
        FunctionArg::Named { name, arg } => visitor.visit_function_arg_named(name, arg),
        FunctionArg::Unnamed(expr) => visitor.visit_function_arg_expression(expr),
    }
}

pub fn visit_function_arg_named<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    ident: &'ast mut Ident,
    func_arg_expr: &'ast mut FunctionArgExpr,
) {
    visitor.visit_ident(ident);
    visitor.visit_function_arg_expression(func_arg_expr)
}

pub fn visit_function_arg_expression<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    func_arg_expr: &'ast mut FunctionArgExpr,
) {
    match func_arg_expr {
        FunctionArgExpr::Expr(expr) => visitor.visit_expr(expr),
        FunctionArgExpr::QualifiedWildcard(object_name) => {
            visitor.visit_qualified_wildcard(&mut object_name.0)
        }
        _ => {}
    }
}

pub fn visit_window_spec<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    window_spec: &'ast mut WindowSpec,
) {
    for expr in &mut window_spec.partition_by {
        visitor.visit_expr(expr);
    }
    for order_by in &mut window_spec.order_by {
        visitor.visit_order_by(order_by);
    }
    if let Some(window_frame) = &mut window_spec.window_frame {
        visitor.visit_window_frame(window_frame);
    }
}

pub fn visit_window_frame<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    window_frame: &'ast mut WindowFrame,
) {
    visitor.visit_window_frame_units(&mut window_frame.units);
    visitor.visit_window_frame_bound(&mut window_frame.start_bound);
    if let Some(end_bound) = &mut window_frame.end_bound {
        visitor.visit_window_frame_bound(end_bound);
    }
}

pub fn visit_case<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    operand: Option<&'ast mut Expr>,
    conditions: &'ast mut [Expr],
    results: &'ast mut [Expr],
    else_result: Option<&'ast mut Expr>,
) {
    if let Some(operand) = operand {
        visitor.visit_expr(operand);
    }
    for cond in conditions {
        visitor.visit_expr(cond);
    }
    for res in results {
        visitor.visit_expr(res);
    }
    if let Some(else_result) = else_result {
        visitor.visit_expr(else_result);
    }
}

pub fn visit_exists<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    subquery: &'ast mut Query,
) {
    visitor.visit_query(subquery)
}

pub fn visit_subquery<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    subquery: &'ast mut Query,
) {
    visitor.visit_query(subquery)
}

pub fn visit_insert<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast mut ObjectName,
    columns: &'ast mut [Ident],
    source: &'ast mut Query,
) {
    visitor.visit_object_name(table_name);
    for column in columns {
        visitor.visit_ident(column);
    }
    visitor.visit_query(source);
}

pub fn visit_values<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    values: &'ast mut Values,
) {
    for row in values.0.iter_mut() {
        visitor.visit_values_row(row)
    }
}

pub fn visit_values_row<'ast, V: VisitorMut<'ast> + ?Sized>(
    visitor: &mut V,
    row: &'ast mut [Expr],
) {
    for expr in row {
        visitor.visit_expr(expr)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::Statement;
    use sqlparser::ast::{
        Expr, FunctionArg, ObjectName, Statement as SQLStatement, TableAlias,
    };
    use std::ops::Deref;
    use test_case::test_case;

    use crate::datafusion::parser::DFParser;
    use crate::datafusion::visit::{visit_table_table_factor, VisitorMut};

    pub struct TestTableRenameVisitor<F>
    where
        F: FnMut(&mut ObjectName),
    {
        pub rename_fn: F,
    }

    impl<'ast, F> VisitorMut<'ast> for TestTableRenameVisitor<F>
    where
        F: FnMut(&mut ObjectName),
    {
        fn visit_table_table_factor(
            &mut self,
            name: &'ast mut ObjectName,
            alias: Option<&'ast mut TableAlias>,
            args: &'ast mut Option<Vec<FunctionArg>>,
            with_hints: &'ast mut [Expr],
        ) {
            (self.rename_fn)(name);
            visit_table_table_factor(self, name, alias, args, with_hints)
        }
    }

    #[test_case(
        "SELECT * FROM test_table";
        "Basic select with bare table name")
    ]
    #[test_case(
        "SELECT * FROM some_schema.test_table";
        "Basic select with schema and table name")
    ]
    #[test_case(
        "SELECT * FROM some_db.some_schema.test_table";
        "Basic select with fully qualified table name")
    ]
    fn test_table_name_rewrite(query: &str) {
        let stmts = DFParser::parse_sql(query).unwrap();

        let mut q = if let Statement::Statement(stmt) = &stmts[0] {
            if let SQLStatement::Query(query) = stmt.deref() {
                query.clone()
            } else {
                panic!("Expected Query not matched!");
            }
        } else {
            panic!("Expected Statement not matched!");
        };

        let rename_table_to_aaaa = |name: &mut ObjectName| {
            let table_ind = name.0.len() - 1;
            name.0[table_ind].value = "aaaa".to_string()
        };

        let mut rewriter = TestTableRenameVisitor {
            rename_fn: rename_table_to_aaaa,
        };
        rewriter.visit_query(&mut q);

        // Ensure table name in the original query has been replaced
        assert_eq!(format!("{}", q), query.replace("test_table", "aaaa"),)
    }
}
