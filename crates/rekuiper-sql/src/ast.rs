use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Wildcard,
    WildcardModified {
        except: Vec<String>,
        replace: Vec<(Expr, String)>,
    },
    Identifier(String),
    Literal(Value),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    FieldAccess {
        parent: Box<Expr>,
        field: String,
    },
    /// Postfix index: `arr[0]`, `arr[-1]`, `obj["k"]`. Zero-based for
    /// arrays (negative counts back from the end); a string index on an
    /// object is a field lookup. Mirrors
    /// https://ekuiper.org/docs/en/latest/sqls/json_expr.html#index-expression.
    Index {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    /// Postfix slice: `arr[from:to)` (end-exclusive, negatives from the
    /// end, omitted bound means array start/end). Mirrors
    /// https://ekuiper.org/docs/en/latest/sqls/json_expr.html#slicing.
    Slice {
        base: Box<Expr>,
        lo: Option<Box<Expr>>,
        hi: Option<Box<Expr>>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Over {
        call: Box<Expr>,
        partition_by: Option<Box<Expr>>,
        when: Option<Box<Expr>>,
    },
}

impl Expr {
    /// Format an expression in eKuiper's exact internal AST debug/string representation
    /// (e.g. `Call:{ name:abs, args:[s.dev] }`, `binaryExpr:{ a + b }`).
    pub fn to_ekuiper_string(&self) -> String {
        self.to_ekuiper_string_qualified("")
    }

    /// Format an expression in eKuiper's exact internal AST debug/string representation
    /// (e.g. `Call:{ name:abs, args:[s.dev] }`, `binaryExpr:{ a + b }`).
    pub fn to_ekuiper_string_qualified(&self, stream: &str) -> String {
        match self {
            Expr::Wildcard => "*".to_string(),
            Expr::WildcardModified { except, replace } => {
                let mut s = "*".to_string();
                if !except.is_empty() {
                    s.push_str(" EXCEPT (");
                    s.push_str(&except.join(", "));
                    s.push(')');
                }
                if !replace.is_empty() {
                    s.push_str(" REPLACE (");
                    let reps: Vec<String> = replace
                        .iter()
                        .map(|(e, c)| format!("{} AS {}", e.to_ekuiper_string_qualified(stream), c))
                        .collect();
                    s.push_str(&reps.join(", "));
                    s.push(')');
                }
                s
            }
            Expr::Identifier(name) => {
                if !stream.is_empty() && !name.contains('.') {
                    format!("{}.{}", stream, name)
                } else {
                    name.clone()
                }
            }
            Expr::Literal(val) => match val {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "nil".to_string(),
                _ => val.to_string(),
            },
            Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    BinaryOperator::Add => "+",
                    BinaryOperator::Sub => "-",
                    BinaryOperator::Mul => "*",
                    BinaryOperator::Div => "/",
                    BinaryOperator::Mod => "%",
                    BinaryOperator::Eq => "=",
                    BinaryOperator::Neq => "!=",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::Lte => "<=",
                    BinaryOperator::Gt => ">",
                    BinaryOperator::Gte => ">=",
                    BinaryOperator::And => "AND",
                    BinaryOperator::Or => "OR",
                    BinaryOperator::Like => "LIKE",
                    BinaryOperator::BitAnd => "&",
                    BinaryOperator::BitOr => "|",
                    BinaryOperator::BitXor => "^",
                };
                format!(
                    "binaryExpr:{{ {} {} {} }}",
                    left.to_ekuiper_string_qualified(stream),
                    op_str,
                    right.to_ekuiper_string_qualified(stream)
                )
            }
            Expr::UnaryOp { op, expr } => {
                let op_str = match op {
                    UnaryOperator::Not => "NOT",
                    UnaryOperator::Neg => "-",
                };
                format!(
                    "unaryExpr:{{ {} {} }}",
                    op_str,
                    expr.to_ekuiper_string_qualified(stream)
                )
            }
            Expr::FieldAccess { parent, field } => {
                format!("{}.{}", parent.to_ekuiper_string_qualified(""), field)
            }
            Expr::Index { base, index } => {
                format!(
                    "binaryExpr:{{ {}[{}] }}",
                    base.to_ekuiper_string_qualified(stream),
                    index.to_ekuiper_string_qualified(stream)
                )
            }
            Expr::Slice { base, lo, hi } => {
                let l = lo
                    .as_ref()
                    .map(|e| e.to_ekuiper_string_qualified(stream))
                    .unwrap_or_default();
                let h = hi
                    .as_ref()
                    .map(|e| e.to_ekuiper_string_qualified(stream))
                    .unwrap_or_default();
                format!("{}[{}:{}]", base.to_ekuiper_string_qualified(stream), l, h)
            }
            Expr::Call { name, args } => {
                let args_str = args
                    .iter()
                    .map(|a| a.to_ekuiper_string_qualified(stream))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Call:{{ name:{}, args:[{}] }}", name, args_str)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let prefix = if *negated { "NOT BETWEEN" } else { "BETWEEN" };
                format!(
                    "{} {} {} AND {}",
                    expr.to_ekuiper_string_qualified(stream),
                    prefix,
                    low.to_ekuiper_string_qualified(stream),
                    high.to_ekuiper_string_qualified(stream)
                )
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let prefix = if *negated { "NOT IN" } else { "IN" };
                let items = list
                    .iter()
                    .map(|e| e.to_ekuiper_string_qualified(stream))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{} {} ({})",
                    expr.to_ekuiper_string_qualified(stream),
                    prefix,
                    items
                )
            }
            Expr::IsNull { expr, negated } => {
                let suffix = if *negated { "IS NOT NULL" } else { "IS NULL" };
                format!("{} {}", expr.to_ekuiper_string_qualified(stream), suffix)
            }
            Expr::Over { call, .. } => call.to_ekuiper_string_qualified(stream),
            Expr::Case { .. } => "caseExpr".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
    BitAnd,
    BitOr,
    BitXor,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Neg,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimeUnit {
    Dd,
    Hh,
    Mi,
    Ss,
    Ms,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowDef {
    TumblingTime {
        unit: TimeUnit,
        length: u64,
    },
    HoppingTime {
        unit: TimeUnit,
        length: u64,
        interval: u64,
    },
    SlidingTime {
        unit: TimeUnit,
        length: u64,
        delay: Option<u64>,
    },
    Count {
        size: usize,
        interval: Option<usize>,
    },
    /// `SESSIONWINDOW(unit, maxDuration, timeout)`, eKuiper argument order:
    /// the window opens at the first event, extends while events arrive
    /// within `timeout`, and is cut at a natural-time `max_duration` check
    /// once it has lasted at least `max_duration`.
    Session {
        unit: TimeUnit,
        max_duration: u64,
        timeout: u64,
    },
    /// `STATEWINDOW(start_condition[, end_condition])`
    State {
        start_condition: Expr,
        end_condition: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub order: SortOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// `ASOF JOIN`: pairs each left event with the most recent right event
    /// for the same equality key whose timestamp <= left timestamp.
    Asof,
    /// `LEFT ASOF JOIN`: preserves the left event even if no prior right event exists.
    LeftAsof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub target: String,
    /// Optional `AS alias` (or bare alias) for the join target; qualified
    /// references may use either the target name or the alias.
    pub alias: Option<String>,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOp {
    Union,
    UnionAll,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub fields: Vec<Expr>,
    /// Output alias per field (`None` when the field has no `AS alias`).
    pub field_aliases: Vec<Option<String>>,
    pub from: String,
    /// Optional `AS alias` (or bare alias) for the FROM source.
    pub from_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub window: Option<WindowDef>,
    pub window_filter: Option<Expr>,
    pub window_trigger_condition: Option<Expr>,
    pub window_partition_by: Option<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub set_op: Option<(SetOp, Box<SelectStmt>)>,
}

/// One parsed `CREATE STREAM/TABLE` column: the column name plus its
/// declared data type, lowercased (`bigint`, `string`, ...).
#[derive(Debug, Clone, PartialEq)]
pub struct StreamColumn {
    pub name: String,
    pub data_type: String,
    pub default_value: Option<String>,
}

impl StreamColumn {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            default_value: None,
        }
    }

    pub fn with_default(
        name: impl Into<String>,
        data_type: impl Into<String>,
        default_val: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            default_value: Some(default_val.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStreamStmt {
    pub name: String,
    pub fields: Vec<StreamColumn>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub name: String,
    pub fields: Vec<StreamColumn>,
    pub options: HashMap<String, String>,
}
