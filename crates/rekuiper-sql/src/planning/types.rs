//! Typed expression checking and function-descriptor infrastructure.

use crate::ast::{BinaryOperator, Expr, UnaryOperator};
use crate::functions::builtin_function_metadata;
use rekuiper_core::contracts::{Capability, CapabilityDescriptor, QualificationLevel};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Static expression type used during typed SQL planning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExprType {
    Any,
    Null,
    Boolean,
    Integer,
    Float,
    Numeric,
    String,
    Bytes,
    Array(Box<ExprType>),
    Object,
    Timestamp,
}

impl ExprType {
    /// Returns true if a value of type `self` is assignable to `expected`.
    pub fn is_compatible_with(&self, expected: &ExprType) -> bool {
        if matches!(self, ExprType::Any | ExprType::Null)
            || matches!(expected, ExprType::Any)
            || self == expected
        {
            return true;
        }
        match (self, expected) {
            (ExprType::Integer | ExprType::Float, ExprType::Numeric) => true,
            (ExprType::Numeric, ExprType::Integer | ExprType::Float) => true,
            (ExprType::Integer, ExprType::Float) => true,
            (ExprType::Array(a), ExprType::Array(b)) => a.is_compatible_with(b),
            _ => false,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            ExprType::Any | ExprType::Integer | ExprType::Float | ExprType::Numeric
        )
    }
}

/// Nullability behavior of a function or expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Nullability {
    NonNull,
    Nullable,
    PreserveInput,
}

/// Determinism guarantee of a function or expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Determinism {
    Deterministic,
    NonDeterministic,
}

/// External side-effect classification of a function or expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideEffect {
    Pure,
    ReadExternal,
    MutateExternal,
}

/// Statefulness classification of a function or expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Statefulness {
    Stateless,
    Stateful { supports_snapshot: bool },
}

/// Expected argument arity for a function descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AritySpec {
    Exact(usize),
    Range { min: usize, max: Option<usize> },
}

impl AritySpec {
    pub fn accepts(&self, count: usize) -> bool {
        match *self {
            AritySpec::Exact(n) => count == n,
            AritySpec::Range { min, max } => count >= min && max.is_none_or(|m| count <= m),
        }
    }
}

/// Typed argument metadata for a function parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArgDescriptor {
    pub name: String,
    pub ty: ExprType,
    pub nullable: bool,
}

impl ArgDescriptor {
    pub fn new(name: impl Into<String>, ty: ExprType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
        }
    }
}

/// Full planning descriptor for a SQL function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionDescriptor {
    pub name: String,
    pub arity: AritySpec,
    pub args: Vec<ArgDescriptor>,
    pub variadic_tail: Option<ExprType>,
    pub result_type: ExprType,
    pub nullability: Nullability,
    pub determinism: Determinism,
    pub side_effect: SideEffect,
    pub statefulness: Statefulness,
    pub capabilities: CapabilityDescriptor,
}

impl FunctionDescriptor {
    /// Conservative descriptor returned when metadata is missing.
    /// Disallows durable replay and unsafe pure optimizer rewrites.
    pub fn conservative_unknown(name: &str) -> Self {
        Self {
            name: name.to_ascii_lowercase(),
            arity: AritySpec::Range { min: 0, max: None },
            args: Vec::new(),
            variadic_tail: Some(ExprType::Any),
            result_type: ExprType::Any,
            nullability: Nullability::Nullable,
            determinism: Determinism::NonDeterministic,
            side_effect: SideEffect::MutateExternal,
            statefulness: Statefulness::Stateful {
                supports_snapshot: false,
            },
            capabilities: CapabilityDescriptor::default(),
        }
    }

    /// True only when the function is deterministic, pure, and (if stateful) supports snapshots.
    pub fn allows_durable_replay(&self) -> bool {
        if self.determinism != Determinism::Deterministic
            || self.side_effect != SideEffect::Pure
            || !self
                .capabilities
                .supported
                .contains(&Capability::Deterministic)
            || !self.capabilities.supported.contains(&Capability::Replay)
        {
            return false;
        }
        match self.statefulness {
            Statefulness::Stateless => true,
            Statefulness::Stateful { supports_snapshot } => {
                supports_snapshot
                    && self.capabilities.supported.contains(&Capability::Snapshot)
                    && self.capabilities.supported.contains(&Capability::Restore)
            }
        }
    }

    /// True only when the function is deterministic, pure, and stateless.
    pub fn allows_pure_rewrite(&self) -> bool {
        self.determinism == Determinism::Deterministic
            && self.side_effect == SideEffect::Pure
            && self.statefulness == Statefulness::Stateless
    }
}

/// Schema field definition for typed expression checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSchema {
    pub ty: ExprType,
    pub nullable: bool,
}

/// Schema environment for expression type-checking.
#[derive(Debug, Clone, Default)]
pub struct SchemaEnv {
    pub fields: BTreeMap<String, FieldSchema>,
    pub strict: bool,
}

impl SchemaEnv {
    /// Legacy schemaless environment: unknown identifiers resolve to `ExprType::Any`.
    pub fn schemaless() -> Self {
        Self {
            fields: BTreeMap::new(),
            strict: false,
        }
    }

    /// Strict schema environment: unknown identifiers fail type-checking.
    pub fn strict() -> Self {
        Self {
            fields: BTreeMap::new(),
            strict: true,
        }
    }

    pub fn with_field(mut self, name: impl Into<String>, ty: ExprType, nullable: bool) -> Self {
        self.fields
            .insert(name.into(), FieldSchema { ty, nullable });
        self
    }

    fn lookup(&self, name: &str) -> Option<&FieldSchema> {
        self.fields.get(name).or_else(|| {
            name.rsplit('.')
                .next()
                .and_then(|short| self.fields.get(short))
        })
    }
}

/// Type-checked expression result with propagated execution guarantees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedExpr {
    pub ty: ExprType,
    pub nullable: bool,
    pub determinism: Determinism,
    pub side_effect: SideEffect,
    pub statefulness: Statefulness,
    pub capabilities: CapabilityDescriptor,
}

impl TypedExpr {
    fn pure_stateless(ty: ExprType, nullable: bool) -> Self {
        Self {
            ty,
            nullable,
            determinism: Determinism::Deterministic,
            side_effect: SideEffect::Pure,
            statefulness: Statefulness::Stateless,
            capabilities: pure_capabilities(),
        }
    }

    fn combine(mut self, other: &TypedExpr, ty: ExprType, nullable: bool) -> Self {
        self.ty = ty;
        self.nullable = nullable;
        if other.determinism == Determinism::NonDeterministic {
            self.determinism = Determinism::NonDeterministic;
        }
        self.side_effect = match (self.side_effect, other.side_effect) {
            (SideEffect::MutateExternal, _) | (_, SideEffect::MutateExternal) => {
                SideEffect::MutateExternal
            }
            (SideEffect::ReadExternal, _) | (_, SideEffect::ReadExternal) => {
                SideEffect::ReadExternal
            }
            _ => SideEffect::Pure,
        };
        self.statefulness = match (self.statefulness, other.statefulness) {
            (
                Statefulness::Stateful {
                    supports_snapshot: a,
                },
                Statefulness::Stateful {
                    supports_snapshot: b,
                },
            ) => Statefulness::Stateful {
                supports_snapshot: a && b,
            },
            (s @ Statefulness::Stateful { .. }, _) | (_, s @ Statefulness::Stateful { .. }) => s,
            _ => Statefulness::Stateless,
        };
        self.capabilities = intersect_capabilities(&self.capabilities, &other.capabilities);
        self
    }
}

/// Errors produced by expression type-checking.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TypeCheckError {
    #[error("unknown field `{field}` in strict schema")]
    UnknownField { field: String },
    #[error("function `{name}` expected {expected}, got {actual} arguments")]
    InvalidArity {
        name: String,
        expected: String,
        actual: usize,
    },
    #[error("type mismatch in {context}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        context: String,
        expected: ExprType,
        actual: ExprType,
    },
    #[error("nullability violation in {context}: argument `{arg}` cannot be null")]
    NullabilityViolation { context: String, arg: String },
}

/// Registry of function planning descriptors.
#[derive(Debug, Clone, Default)]
pub struct FunctionDescriptorRegistry {
    descriptors: BTreeMap<String, FunctionDescriptor>,
}

impl FunctionDescriptorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers representative pure, non-deterministic, and stateful functions
    /// via adapters over [`builtin_function_metadata`].
    pub fn with_representative_builtins() -> Self {
        let mut reg = Self::new();
        for meta in builtin_function_metadata() {
            match meta.name {
                "abs" | "ceil" | "ceiling" | "floor" => {
                    reg.register(pure_unary_numeric(meta.name, ExprType::Numeric));
                }
                "round" => reg.register(FunctionDescriptor {
                    name: meta.name.to_string(),
                    arity: AritySpec::Range {
                        min: 1,
                        max: Some(2),
                    },
                    args: vec![
                        ArgDescriptor::new("value", ExprType::Numeric, true),
                        ArgDescriptor::new("precision", ExprType::Integer, false),
                    ],
                    variadic_tail: None,
                    result_type: ExprType::Numeric,
                    nullability: Nullability::PreserveInput,
                    determinism: Determinism::Deterministic,
                    side_effect: SideEffect::Pure,
                    statefulness: Statefulness::Stateless,
                    capabilities: pure_capabilities(),
                }),
                "lower" | "upper" => {
                    reg.register(pure_unary(meta.name, ExprType::String, ExprType::String));
                }
                "length" => {
                    reg.register(pure_unary(meta.name, ExprType::String, ExprType::Integer));
                }
                "concat" => reg.register(FunctionDescriptor {
                    name: meta.name.to_string(),
                    arity: AritySpec::Range { min: 1, max: None },
                    args: vec![ArgDescriptor::new("value", ExprType::Any, true)],
                    variadic_tail: Some(ExprType::Any),
                    result_type: ExprType::String,
                    nullability: Nullability::NonNull,
                    determinism: Determinism::Deterministic,
                    side_effect: SideEffect::Pure,
                    statefulness: Statefulness::Stateless,
                    capabilities: pure_capabilities(),
                }),
                "coalesce" => reg.register(FunctionDescriptor {
                    name: meta.name.to_string(),
                    arity: AritySpec::Range { min: 1, max: None },
                    args: vec![ArgDescriptor::new("value", ExprType::Any, true)],
                    variadic_tail: Some(ExprType::Any),
                    result_type: ExprType::Any,
                    nullability: Nullability::Nullable,
                    determinism: Determinism::Deterministic,
                    side_effect: SideEffect::Pure,
                    statefulness: Statefulness::Stateless,
                    capabilities: pure_capabilities(),
                }),
                "newuuid" | "rand" => reg.register(FunctionDescriptor {
                    name: meta.name.to_string(),
                    arity: AritySpec::Exact(0),
                    args: Vec::new(),
                    variadic_tail: None,
                    result_type: if meta.name == "newuuid" {
                        ExprType::String
                    } else {
                        ExprType::Float
                    },
                    nullability: Nullability::NonNull,
                    determinism: Determinism::NonDeterministic,
                    side_effect: SideEffect::Pure,
                    statefulness: Statefulness::Stateless,
                    capabilities: CapabilityDescriptor::default(),
                }),
                "count" => reg.register(stateful_agg(
                    meta.name,
                    ExprType::Any,
                    ExprType::Integer,
                    Nullability::NonNull,
                )),
                "sum" | "avg" => reg.register(stateful_agg(
                    meta.name,
                    ExprType::Numeric,
                    ExprType::Float,
                    Nullability::Nullable,
                )),
                "lag" => reg.register(FunctionDescriptor {
                    name: meta.name.to_string(),
                    arity: AritySpec::Range {
                        min: 1,
                        max: Some(3),
                    },
                    args: vec![
                        ArgDescriptor::new("expr", ExprType::Any, true),
                        ArgDescriptor::new("offset", ExprType::Integer, false),
                        ArgDescriptor::new("default", ExprType::Any, true),
                    ],
                    variadic_tail: None,
                    result_type: ExprType::Any,
                    nullability: Nullability::Nullable,
                    determinism: Determinism::Deterministic,
                    side_effect: SideEffect::Pure,
                    statefulness: Statefulness::Stateful {
                        supports_snapshot: true,
                    },
                    capabilities: stateful_snapshot_capabilities(),
                }),
                _ => {}
            }
        }
        reg
    }

    pub fn register(&mut self, descriptor: FunctionDescriptor) {
        self.descriptors
            .insert(descriptor.name.to_ascii_lowercase(), descriptor);
    }

    pub fn get(&self, name: &str) -> Option<&FunctionDescriptor> {
        self.descriptors.get(&name.to_ascii_lowercase())
    }

    pub fn resolve_or_conservative(&self, name: &str) -> FunctionDescriptor {
        self.get(name)
            .cloned()
            .unwrap_or_else(|| FunctionDescriptor::conservative_unknown(name))
    }
}

/// Type-checks an expression against `schema` and `registry`.
pub fn type_check_expr(
    expr: &Expr,
    schema: &SchemaEnv,
    registry: &FunctionDescriptorRegistry,
) -> Result<TypedExpr, TypeCheckError> {
    match expr {
        Expr::Wildcard | Expr::WildcardModified { .. } => {
            Ok(TypedExpr::pure_stateless(ExprType::Object, false))
        }
        Expr::Literal(val) => Ok(type_of_literal(val)),
        Expr::Identifier(name) => {
            if let Some(field) = schema.lookup(name) {
                Ok(TypedExpr::pure_stateless(field.ty.clone(), field.nullable))
            } else if schema.strict {
                Err(TypeCheckError::UnknownField {
                    field: name.clone(),
                })
            } else {
                Ok(TypedExpr::pure_stateless(ExprType::Any, true))
            }
        }
        Expr::FieldAccess { parent, .. } => {
            let parent_typed = type_check_expr(parent, schema, registry)?;
            if !parent_typed.ty.is_compatible_with(&ExprType::Object) {
                return Err(TypeCheckError::TypeMismatch {
                    context: "field access".to_string(),
                    expected: ExprType::Object,
                    actual: parent_typed.ty,
                });
            }
            Ok(TypedExpr {
                ty: ExprType::Any,
                nullable: true,
                ..parent_typed
            })
        }
        Expr::Index { base, index } => {
            let base_t = type_check_expr(base, schema, registry)?;
            let idx_t = type_check_expr(index, schema, registry)?;
            let elem_ty = match &base_t.ty {
                ExprType::Array(inner) => (**inner).clone(),
                _ => ExprType::Any,
            };
            Ok(base_t.combine(&idx_t, elem_ty, true))
        }
        Expr::Slice { base, lo, hi } => {
            let mut acc = type_check_expr(base, schema, registry)?;
            for bound in [lo.as_deref(), hi.as_deref()].into_iter().flatten() {
                let bt = type_check_expr(bound, schema, registry)?;
                if !bt.ty.is_compatible_with(&ExprType::Integer) {
                    return Err(TypeCheckError::TypeMismatch {
                        context: "slice bound".to_string(),
                        expected: ExprType::Integer,
                        actual: bt.ty,
                    });
                }
                let ty = acc.ty.clone();
                let nullable = acc.nullable;
                acc = acc.combine(&bt, ty, nullable);
            }
            Ok(acc)
        }
        Expr::UnaryOp { op, expr } => {
            let inner = type_check_expr(expr, schema, registry)?;
            match op {
                UnaryOperator::Not => {
                    require_type("unary NOT", &inner.ty, &ExprType::Boolean)?;
                    Ok(TypedExpr {
                        ty: ExprType::Boolean,
                        ..inner
                    })
                }
                UnaryOperator::Neg => {
                    require_type("unary negation", &inner.ty, &ExprType::Numeric)?;
                    Ok(inner)
                }
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = type_check_expr(left, schema, registry)?;
            let r = type_check_expr(right, schema, registry)?;
            type_check_binary_op(op, l, r)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let e = type_check_expr(expr, schema, registry)?;
            let l = type_check_expr(low, schema, registry)?;
            let h = type_check_expr(high, schema, registry)?;
            let nullable = e.nullable || l.nullable || h.nullable;
            Ok(e.combine(&l, ExprType::Boolean, nullable)
                .combine(&h, ExprType::Boolean, nullable))
        }
        Expr::InList { expr, list, .. } => {
            let mut acc = type_check_expr(expr, schema, registry)?;
            for item in list {
                let it = type_check_expr(item, schema, registry)?;
                acc = acc.combine(&it, ExprType::Boolean, true);
            }
            Ok(TypedExpr {
                ty: ExprType::Boolean,
                ..acc
            })
        }
        Expr::IsNull { expr, .. } => {
            let inner = type_check_expr(expr, schema, registry)?;
            Ok(TypedExpr {
                ty: ExprType::Boolean,
                nullable: false,
                ..inner
            })
        }
        Expr::Call { name, args } => type_check_call(name, args, schema, registry),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let mut acc = match operand {
                Some(op) => type_check_expr(op, schema, registry)?,
                None => TypedExpr::pure_stateless(ExprType::Any, false),
            };
            let mut result_ty = ExprType::Any;
            let mut nullable = else_clause.is_none();
            for (cond, then_expr) in when_clauses {
                let ct = type_check_expr(cond, schema, registry)?;
                if operand.is_none() {
                    require_type("CASE WHEN condition", &ct.ty, &ExprType::Boolean)?;
                }
                let tt = type_check_expr(then_expr, schema, registry)?;
                if result_ty == ExprType::Any {
                    result_ty = tt.ty.clone();
                }
                nullable |= tt.nullable;
                acc = acc.combine(&ct, result_ty.clone(), nullable).combine(
                    &tt,
                    result_ty.clone(),
                    nullable,
                );
            }
            if let Some(el) = else_clause {
                let et = type_check_expr(el, schema, registry)?;
                nullable |= et.nullable;
                acc = acc.combine(&et, result_ty, nullable);
            }
            Ok(acc)
        }
        Expr::Over {
            call,
            partition_by,
            when,
        } => {
            let mut acc = type_check_expr(call, schema, registry)?;
            if let Some(pb) = partition_by {
                let pt = type_check_expr(pb, schema, registry)?;
                let ty = acc.ty.clone();
                let nullable = acc.nullable;
                acc = acc.combine(&pt, ty, nullable);
            }
            if let Some(w) = when {
                let wt = type_check_expr(w, schema, registry)?;
                require_type("OVER WHEN clause", &wt.ty, &ExprType::Boolean)?;
                let ty = acc.ty.clone();
                let nullable = acc.nullable;
                acc = acc.combine(&wt, ty, nullable);
            }
            Ok(acc)
        }
    }
}

fn type_check_call(
    name: &str,
    args: &[Expr],
    schema: &SchemaEnv,
    registry: &FunctionDescriptorRegistry,
) -> Result<TypedExpr, TypeCheckError> {
    let desc = registry.resolve_or_conservative(name);
    if !desc.arity.accepts(args.len()) {
        return Err(TypeCheckError::InvalidArity {
            name: name.to_string(),
            expected: format_arity(desc.arity),
            actual: args.len(),
        });
    }

    let mut acc = TypedExpr {
        ty: desc.result_type.clone(),
        nullable: matches!(desc.nullability, Nullability::Nullable),
        determinism: desc.determinism,
        side_effect: desc.side_effect,
        statefulness: desc.statefulness,
        capabilities: desc.capabilities.clone(),
    };

    let mut any_arg_nullable = false;
    for (idx, arg_expr) in args.iter().enumerate() {
        let arg_t = type_check_expr(arg_expr, schema, registry)?;
        if let Some(param) = desc.args.get(idx) {
            if !param.nullable && arg_t.ty == ExprType::Null {
                return Err(TypeCheckError::NullabilityViolation {
                    context: format!("call `{name}`"),
                    arg: param.name.clone(),
                });
            }
            require_type(
                &format!("call `{name}` arg `{}`", param.name),
                &arg_t.ty,
                &param.ty,
            )?;
        } else if let Some(tail_ty) = &desc.variadic_tail {
            require_type(&format!("call `{name}` variadic arg"), &arg_t.ty, tail_ty)?;
        }
        any_arg_nullable |= arg_t.nullable;
        let next_nullable = match desc.nullability {
            Nullability::NonNull => false,
            Nullability::Nullable => true,
            Nullability::PreserveInput => any_arg_nullable,
        };
        acc = acc.combine(&arg_t, desc.result_type.clone(), next_nullable);
    }

    Ok(acc)
}

fn type_check_binary_op(
    op: &BinaryOperator,
    left: TypedExpr,
    right: TypedExpr,
) -> Result<TypedExpr, TypeCheckError> {
    let nullable = left.nullable || right.nullable;
    let ty = match op {
        BinaryOperator::And | BinaryOperator::Or => {
            require_type("logical operator", &left.ty, &ExprType::Boolean)?;
            require_type("logical operator", &right.ty, &ExprType::Boolean)?;
            ExprType::Boolean
        }
        BinaryOperator::Add
        | BinaryOperator::Sub
        | BinaryOperator::Mul
        | BinaryOperator::Div
        | BinaryOperator::Mod => {
            if !left.ty.is_numeric() {
                return Err(TypeCheckError::TypeMismatch {
                    context: "arithmetic operator".to_string(),
                    expected: ExprType::Numeric,
                    actual: left.ty,
                });
            }
            if !right.ty.is_numeric() {
                return Err(TypeCheckError::TypeMismatch {
                    context: "arithmetic operator".to_string(),
                    expected: ExprType::Numeric,
                    actual: right.ty,
                });
            }
            if left.ty == ExprType::Float || right.ty == ExprType::Float {
                ExprType::Float
            } else if left.ty == ExprType::Integer && right.ty == ExprType::Integer {
                ExprType::Integer
            } else {
                ExprType::Numeric
            }
        }
        BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
            require_type("bitwise operator", &left.ty, &ExprType::Integer)?;
            require_type("bitwise operator", &right.ty, &ExprType::Integer)?;
            ExprType::Integer
        }
        BinaryOperator::Like => {
            require_type("LIKE operator", &left.ty, &ExprType::String)?;
            require_type("LIKE operator", &right.ty, &ExprType::String)?;
            ExprType::Boolean
        }
        BinaryOperator::Eq
        | BinaryOperator::Neq
        | BinaryOperator::Lt
        | BinaryOperator::Lte
        | BinaryOperator::Gt
        | BinaryOperator::Gte => {
            if !left.ty.is_compatible_with(&right.ty) && !right.ty.is_compatible_with(&left.ty) {
                return Err(TypeCheckError::TypeMismatch {
                    context: "comparison operator".to_string(),
                    expected: left.ty,
                    actual: right.ty,
                });
            }
            ExprType::Boolean
        }
    };
    Ok(left.combine(&right, ty, nullable))
}

fn require_type(
    context: &str,
    actual: &ExprType,
    expected: &ExprType,
) -> Result<(), TypeCheckError> {
    if actual.is_compatible_with(expected) {
        Ok(())
    } else {
        Err(TypeCheckError::TypeMismatch {
            context: context.to_string(),
            expected: expected.clone(),
            actual: actual.clone(),
        })
    }
}

fn type_of_literal(val: &Value) -> TypedExpr {
    match val {
        Value::Null => TypedExpr::pure_stateless(ExprType::Null, true),
        Value::Bool(_) => TypedExpr::pure_stateless(ExprType::Boolean, false),
        Value::Number(n) => {
            let ty = if n.is_i64() || n.is_u64() {
                ExprType::Integer
            } else {
                ExprType::Float
            };
            TypedExpr::pure_stateless(ty, false)
        }
        Value::String(_) => TypedExpr::pure_stateless(ExprType::String, false),
        Value::Array(items) => {
            let inner = items
                .first()
                .map(|v| type_of_literal(v).ty)
                .unwrap_or(ExprType::Any);
            TypedExpr::pure_stateless(ExprType::Array(Box::new(inner)), false)
        }
        Value::Object(_) => TypedExpr::pure_stateless(ExprType::Object, false),
    }
}

fn format_arity(arity: AritySpec) -> String {
    match arity {
        AritySpec::Exact(n) => n.to_string(),
        AritySpec::Range { min, max: Some(m) } => format!("{min}..={m}"),
        AritySpec::Range { min, max: None } => format!("{min}+"),
    }
}

fn pure_capabilities() -> CapabilityDescriptor {
    CapabilityDescriptor {
        supported: BTreeSet::from([Capability::Deterministic, Capability::Replay]),
        limits: BTreeMap::new(),
        qualification: QualificationLevel::ComponentTested,
    }
}

fn stateful_snapshot_capabilities() -> CapabilityDescriptor {
    CapabilityDescriptor {
        supported: BTreeSet::from([
            Capability::Deterministic,
            Capability::Replay,
            Capability::Snapshot,
            Capability::Restore,
        ]),
        limits: BTreeMap::new(),
        qualification: QualificationLevel::ComponentTested,
    }
}

fn pure_unary(name: &str, arg_ty: ExprType, result_type: ExprType) -> FunctionDescriptor {
    FunctionDescriptor {
        name: name.to_string(),
        arity: AritySpec::Exact(1),
        args: vec![ArgDescriptor::new("value", arg_ty, true)],
        variadic_tail: None,
        result_type,
        nullability: Nullability::PreserveInput,
        determinism: Determinism::Deterministic,
        side_effect: SideEffect::Pure,
        statefulness: Statefulness::Stateless,
        capabilities: pure_capabilities(),
    }
}

fn pure_unary_numeric(name: &str, result_type: ExprType) -> FunctionDescriptor {
    pure_unary(name, ExprType::Numeric, result_type)
}

fn stateful_agg(
    name: &str,
    arg_ty: ExprType,
    result_type: ExprType,
    nullability: Nullability,
) -> FunctionDescriptor {
    FunctionDescriptor {
        name: name.to_string(),
        arity: AritySpec::Exact(1),
        args: vec![ArgDescriptor::new("value", arg_ty, true)],
        variadic_tail: None,
        result_type,
        nullability,
        determinism: Determinism::Deterministic,
        side_effect: SideEffect::Pure,
        statefulness: Statefulness::Stateful {
            supports_snapshot: true,
        },
        capabilities: stateful_snapshot_capabilities(),
    }
}

fn intersect_capabilities(
    a: &CapabilityDescriptor,
    b: &CapabilityDescriptor,
) -> CapabilityDescriptor {
    let supported = a.supported.intersection(&b.supported).copied().collect();
    let qualification = a.qualification.min(b.qualification);
    CapabilityDescriptor {
        supported,
        limits: BTreeMap::new(),
        qualification,
    }
}
