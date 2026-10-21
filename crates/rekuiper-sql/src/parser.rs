use crate::ast::{
    BinaryOperator, CreateStreamStmt, CreateTableStmt, Expr, JoinClause, JoinType, OrderByItem,
    SelectStmt, SetOp, SortOrder, StreamColumn, TimeUnit, UnaryOperator, WindowDef,
};
use anyhow::{bail, Result};
use std::collections::HashMap;

pub struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let ch = self.input[self.pos..].chars().next().unwrap();
            if ch.is_whitespace() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek_word(&self) -> Option<String> {
        let remaining = &self.input[self.pos..];
        let trimmed = remaining.trim_start();
        let end = trimmed
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(trimmed.len());
        if end == 0 {
            None
        } else {
            Some(trimmed[..end].to_string())
        }
    }

    fn peek_word_is(&self, kw: &str) -> bool {
        if let Some(word) = self.peek_word() {
            word.eq_ignore_ascii_case(kw)
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        self.skip_whitespace();
        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected keyword {}, found EOF", kw))?;
        if word.eq_ignore_ascii_case(kw) {
            self.skip_whitespace();
            self.pos += word.len();
            Ok(())
        } else {
            bail!("Expected keyword {}, found '{}'", kw, word);
        }
    }

    fn match_keyword(&mut self, kw: &str) -> bool {
        self.skip_whitespace();
        if let Some(word) = self.peek_word() {
            if word.eq_ignore_ascii_case(kw) {
                // pos is already after skip_whitespace, word starts at pos
                self.pos += word.len();
                return true;
            }
        }
        false
    }

    fn expect_char(&mut self, c: char) -> Result<()> {
        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(c) {
            self.pos += c.len_utf8();
            Ok(())
        } else {
            bail!("Expected '{}'", c);
        }
    }

    pub fn parse_create_stream(&mut self) -> Result<CreateStreamStmt> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("STREAM")?;
        self.skip_whitespace();

        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected stream name"))?;
        self.skip_whitespace();
        self.pos += word.len();
        let name = word;

        self.skip_whitespace();
        // Optional schema definition in parens: (col TYPE, ...)
        let fields = if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
            self.parse_column_defs()?
        } else if self.peek_word_is("WITH")
            || self.pos >= self.input.len()
            || self.input[self.pos..].starts_with(';')
        {
            Vec::new()
        } else {
            let tok = if let Some(w) = self.peek_word() {
                format!("\"{}\"", w)
            } else {
                let remaining = self.input[self.pos..].trim_start();
                if remaining.starts_with(['"', '\'']) {
                    let inner = &remaining[1..];
                    let word_end = inner
                        .find(|c: char| !c.is_alphanumeric() && c != '_')
                        .unwrap_or(inner.len());
                    format!("\"{}\"", &inner[..word_end])
                } else {
                    let next_chunk = remaining
                        .chars()
                        .next()
                        .map(|c| c.to_string())
                        .unwrap_or_default();
                    format!("\"{}\"", next_chunk)
                }
            };
            bail!("found {}, expected lparen after stream name.", tok);
        };

        let options = self.parse_with_options()?;

        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(';') {
            self.pos += 1;
            self.skip_whitespace();
        }
        if self.pos < self.input.len() {
            let tok = if let Some(w) = self.peek_word() {
                format!("\"{}\"", w)
            } else {
                let remaining = self.input[self.pos..].trim_start();
                let next_chunk = remaining
                    .chars()
                    .next()
                    .map(|c| c.to_string())
                    .unwrap_or_default();
                format!("\"{}\"", next_chunk)
            };
            bail!(
                "found {}, expected semicolon or EOF after stream options.",
                tok
            );
        }

        Ok(CreateStreamStmt {
            name,
            fields,
            options,
        })
    }

    /// Parse `WITH (k = v, ...)` options with quote-aware delimiter scanning.
    fn parse_with_options(&mut self) -> Result<HashMap<String, String>> {
        let mut options = HashMap::new();
        if !self.match_keyword("WITH") {
            return Ok(options);
        }
        self.skip_whitespace();
        if !(self.pos < self.input.len() && self.input[self.pos..].starts_with('(')) {
            return Ok(options);
        }
        self.pos += 1;

        let mut depth = 1usize;
        let mut in_single = false;
        let mut in_double = false;
        let mut close_pos = None;

        for (off, ch) in self.input[self.pos..].char_indices() {
            if in_single {
                if ch == '\'' {
                    in_single = false;
                }
            } else if in_double {
                if ch == '"' {
                    in_double = false;
                }
            } else {
                match ch {
                    '\'' => in_single = true,
                    '"' => in_double = true,
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            close_pos = Some(self.pos + off);
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }

        let close = close_pos.ok_or_else(|| anyhow::anyhow!("Unclosed WITH parenthesis"))?;
        let with_content = &self.input[self.pos..close];
        self.pos = close + 1;

        // Parse key = value pairs separated by comma, respecting quotes
        let mut pairs = Vec::new();
        let mut current = String::new();
        let mut s_quote = false;
        let mut d_quote = false;

        for ch in with_content.chars() {
            if s_quote {
                if ch == '\'' {
                    s_quote = false;
                }
                current.push(ch);
            } else if d_quote {
                if ch == '"' {
                    d_quote = false;
                }
                current.push(ch);
            } else {
                match ch {
                    '\'' => {
                        s_quote = true;
                        current.push(ch);
                    }
                    '"' => {
                        d_quote = true;
                        current.push(ch);
                    }
                    ',' => {
                        pairs.push(std::mem::take(&mut current));
                    }
                    _ => current.push(ch),
                }
            }
        }
        if !current.trim().is_empty() {
            pairs.push(current);
        }

        for part in pairs {
            let part = part.trim();
            if let Some((k, v)) = part.split_once('=') {
                let k = k.trim().trim_matches(['"', '\'', '`']).to_uppercase();
                let v = v.trim();
                let v_stripped = if (v.starts_with('\'') && v.ends_with('\''))
                    || (v.starts_with('"') && v.ends_with('"'))
                {
                    if v.len() >= 2 {
                        &v[1..v.len() - 1]
                    } else {
                        v
                    }
                } else {
                    v
                };
                options.insert(k, v_stripped.to_string());
            }
        }

        Ok(options)
    }

    /// Parse an optional `(name TYPE, ...)` column list, returning no columns
    /// when the next token is not `(`. Types are lowercased; a bare name
    /// with no type defaults to `string`. The scan is depth-aware so
    /// parameterized types like `DECIMAL(10,2)` do not terminate it early.
    fn parse_column_defs(&mut self) -> Result<Vec<StreamColumn>> {
        if !(self.pos < self.input.len() && self.input[self.pos..].starts_with('(')) {
            return Ok(Vec::new());
        }
        let mut depth = 0usize;
        let mut in_single = false;
        let mut in_double = false;
        let mut close_pos = None;
        for (off, ch) in self.input[self.pos..].char_indices() {
            if in_single {
                if ch == '\'' {
                    in_single = false;
                }
            } else if in_double {
                if ch == '"' {
                    in_double = false;
                }
            } else {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            close_pos = Some(self.pos + off);
                            break;
                        }
                    }
                    '\'' => in_single = true,
                    '"' => in_double = true,
                    _ => {}
                }
            }
        }
        let close = close_pos.ok_or_else(|| anyhow::anyhow!("Unclosed parenthesis"))?;
        let inner = &self.input[self.pos + 1..close];
        self.pos = close + 1;

        let mut fields = Vec::new();
        for part in split_top_level_commas(inner) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            // Extract column name: quoted or unquoted
            let (name, remainder) =
                if part.starts_with('"') || part.starts_with('`') || part.starts_with('\'') {
                    let quote = part.chars().next().unwrap();
                    if let Some(close_idx) = part[1..].find(quote) {
                        let name = &part[1..1 + close_idx];
                        let rem = part[1 + close_idx + 1..].trim();
                        (name.to_string(), rem)
                    } else {
                        bail!("Unclosed quote in column definition: {}", part);
                    }
                } else {
                    let first_ws = part.find(char::is_whitespace).unwrap_or(part.len());
                    let name = &part[..first_ws];
                    let rem = part[first_ws..].trim();
                    (name.to_string(), rem)
                };
            if name.is_empty() {
                continue;
            }

            let (raw_type_str, default_val) = split_at_default(remainder);
            let mut raw_type = raw_type_str.trim();
            // Strip trailing NOT NULL / NULL constraints if present
            if let Some(stripped) = raw_type
                .strip_suffix("not null")
                .or_else(|| raw_type.strip_suffix("NOT NULL"))
            {
                raw_type = stripped.trim();
            } else if let Some(stripped) = raw_type
                .strip_suffix("null")
                .or_else(|| raw_type.strip_suffix("NULL"))
            {
                raw_type = stripped.trim();
            }
            let data_type = if raw_type.is_empty() {
                "string".to_string()
            } else {
                raw_type
                    .trim_matches(['"', '\'', '`', ',', ';'])
                    .to_ascii_lowercase()
            };

            if !is_valid_stream_type(&data_type) {
                bail!(
                    "Unsupported or invalid data type '{}' for column '{}'",
                    data_type,
                    name
                );
            }

            let default_value =
                default_val.map(|s| s.trim().trim_end_matches([';', ',']).trim().to_string());

            fields.push(StreamColumn {
                name,
                data_type,
                default_value,
            });
        }
        Ok(fields)
    }

    pub fn parse_create_table(&mut self) -> Result<CreateTableStmt> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("TABLE")?;
        self.skip_whitespace();

        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected table name"))?;
        self.skip_whitespace();
        self.pos += word.len();
        let name = word;

        self.skip_whitespace();
        // Optional schema definition in parens: (col TYPE, ...)
        let fields = if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
            self.parse_column_defs()?
        } else if self.peek_word_is("WITH")
            || self.pos >= self.input.len()
            || self.input[self.pos..].starts_with(';')
        {
            Vec::new()
        } else {
            let tok = if let Some(w) = self.peek_word() {
                format!("\"{}\"", w)
            } else {
                let remaining = self.input[self.pos..].trim_start();
                if remaining.starts_with(['"', '\'']) {
                    let inner = &remaining[1..];
                    let word_end = inner
                        .find(|c: char| !c.is_alphanumeric() && c != '_')
                        .unwrap_or(inner.len());
                    format!("\"{}\"", &inner[..word_end])
                } else {
                    let next_chunk = remaining
                        .chars()
                        .next()
                        .map(|c| c.to_string())
                        .unwrap_or_default();
                    format!("\"{}\"", next_chunk)
                }
            };
            bail!("found {}, expected lparen after table name.", tok);
        };

        let options = self.parse_with_options()?;

        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(';') {
            self.pos += 1;
            self.skip_whitespace();
        }
        if self.pos < self.input.len() {
            let tok = if let Some(w) = self.peek_word() {
                format!("\"{}\"", w)
            } else {
                let remaining = self.input[self.pos..].trim_start();
                let next_chunk = remaining
                    .chars()
                    .next()
                    .map(|c| c.to_string())
                    .unwrap_or_default();
                format!("\"{}\"", next_chunk)
            };
            bail!(
                "found {}, expected semicolon or EOF after table options.",
                tok
            );
        }

        Ok(CreateTableStmt {
            name,
            fields,
            options,
        })
    }

    pub fn parse_select(&mut self) -> Result<SelectStmt> {
        self.expect_keyword("SELECT")?;
        self.skip_whitespace();

        // Fields: wildcard or full expressions, each with an optional AS alias.
        let mut fields = Vec::new();
        let mut field_aliases: Vec<Option<String>> = Vec::new();
        loop {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('*') {
                self.pos += 1;
                self.skip_whitespace();
                let mut except = Vec::new();
                let mut replace = Vec::new();
                let mut is_modified = false;
                loop {
                    self.skip_whitespace();
                    if self.peek_word_is("EXCEPT") {
                        is_modified = true;
                        self.match_keyword("EXCEPT");
                        self.skip_whitespace();
                        self.expect_char('(')?;
                        loop {
                            self.skip_whitespace();
                            let col = self.parse_column_identifier()?;
                            except.push(col);
                            self.skip_whitespace();
                            if self.pos < self.input.len()
                                && self.input[self.pos..].starts_with(',')
                            {
                                self.pos += 1;
                            } else {
                                break;
                            }
                        }
                        self.skip_whitespace();
                        self.expect_char(')')?;
                    } else if self.peek_word_is("REPLACE") {
                        is_modified = true;
                        self.match_keyword("REPLACE");
                        self.skip_whitespace();
                        self.expect_char('(')?;
                        loop {
                            self.skip_whitespace();
                            let rep_expr = self.parse_expr()?;
                            self.skip_whitespace();
                            self.expect_keyword("AS")?;
                            self.skip_whitespace();
                            let col = self.parse_column_identifier()?;
                            replace.push((rep_expr, col));
                            self.skip_whitespace();
                            if self.pos < self.input.len()
                                && self.input[self.pos..].starts_with(',')
                            {
                                self.pos += 1;
                            } else {
                                break;
                            }
                        }
                        self.skip_whitespace();
                        self.expect_char(')')?;
                    } else {
                        break;
                    }
                }
                if is_modified {
                    fields.push(Expr::WildcardModified { except, replace });
                } else {
                    fields.push(Expr::Wildcard);
                }
                field_aliases.push(None);
            } else {
                if self.peek_word_is("FROM") {
                    bail!("Unexpected FROM in select field list");
                }
                let expr = self.parse_expr()?;
                fields.push(expr);
                // Optional alias: AS alias (stored in field_aliases) or bare backtick alias.
                let mut alias: Option<String> = None;
                self.skip_whitespace();
                if self.peek_word_is("AS") {
                    self.match_keyword("AS");
                    self.skip_whitespace();
                    if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
                        alias = Some(self.parse_backtick_identifier()?);
                    } else if let Some(name) = self.peek_word() {
                        if !name.eq_ignore_ascii_case("FROM")
                            && !name.eq_ignore_ascii_case("WHERE")
                            && !name.eq_ignore_ascii_case("AND")
                            && !name.eq_ignore_ascii_case("OR")
                        {
                            self.skip_whitespace();
                            self.pos += name.len();
                            alias = Some(name);
                        }
                    }
                } else if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
                    alias = Some(self.parse_backtick_identifier()?);
                }
                field_aliases.push(alias);
            }

            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                self.pos += 1;
            } else {
                break;
            }
        }

        self.expect_keyword("FROM")?;
        self.skip_whitespace();
        let from = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected stream name after FROM"))?;
        self.skip_whitespace();
        self.pos += from.len();
        // Optional source alias: `FROM s AS a` or `FROM s a`. Clause
        // keywords never count as aliases.
        let from_alias = self.parse_optional_alias()?;

        // Optional JOIN clauses: [LEFT|RIGHT|FULL|CROSS|INNER] [OUTER] JOIN
        // <target> [ON <condition>], one or more in sequence.
        let mut joins = Vec::new();
        loop {
            let is_join = matches!(
                self.peek_word().map(|w| w.to_ascii_uppercase()).as_deref(),
                Some("LEFT")
                    | Some("RIGHT")
                    | Some("FULL")
                    | Some("CROSS")
                    | Some("INNER")
                    | Some("JOIN")
                    | Some("OUTER")
                    | Some("ASOF")
            );
            if !is_join {
                break;
            }
            // Look ahead: only commit when a JOIN header really follows, so a
            // stream merely named e.g. `left` does not break parsing.
            let saved = self.pos;
            let prefix = match self.peek_word().map(|w| w.to_ascii_uppercase()).as_deref() {
                Some("LEFT") | Some("RIGHT") | Some("FULL") | Some("CROSS") | Some("INNER")
                | Some("ASOF") => {
                    let word = self.peek_word().unwrap_or_default();
                    self.skip_whitespace();
                    self.pos += word.len();
                    Some(word.to_ascii_uppercase())
                }
                _ => None,
            };
            let mut is_asof = prefix.as_deref() == Some("ASOF");
            if prefix.as_deref() == Some("LEFT") && self.peek_word_is("ASOF") {
                self.match_keyword("ASOF");
                is_asof = true;
            }
            if self.peek_word_is("OUTER") {
                self.match_keyword("OUTER");
            }
            if !self.match_keyword("JOIN") {
                self.pos = saved;
                break;
            }
            let join_type = match prefix.as_deref() {
                Some("LEFT") if is_asof => JoinType::LeftAsof,
                Some("LEFT") => JoinType::Left,
                Some("RIGHT") => JoinType::Right,
                Some("FULL") => JoinType::Full,
                Some("CROSS") => JoinType::Cross,
                Some("ASOF") => JoinType::Asof,
                _ => JoinType::Inner,
            };
            let target = match self.peek_word() {
                Some(t) => {
                    if matches!(
                        t.to_ascii_uppercase().as_str(),
                        "WHERE" | "GROUP" | "ORDER" | "LIMIT" | "HAVING" | "JOIN" | "ON" | "ASOF"
                    ) {
                        bail!("Expected join target after JOIN, found '{}'", t);
                    }
                    self.skip_whitespace();
                    self.pos += t.len();
                    t
                }
                None => bail!("Expected join target after JOIN"),
            };
            let mut on = None;
            let alias = self.parse_optional_alias()?;
            if self.match_keyword("ON") {
                on = Some(self.parse_expr()?);
            }
            joins.push(JoinClause {
                join_type,
                target,
                alias,
                on,
            });
        }

        let mut where_clause = None;
        if self.match_keyword("WHERE") {
            where_clause = Some(self.parse_expr()?);
        }

        let mut group_by = Vec::new();
        let mut window: Option<WindowDef> = None;
        let mut window_filter: Option<Expr> = None;
        let mut window_trigger_condition: Option<Expr> = None;
        let mut window_partition_by: Option<Expr> = None;
        // GROUP BY <items> — items may include window calls which go to `window`.
        if self.peek_word_is("GROUP") {
            self.expect_keyword("GROUP")?;
            self.expect_keyword("BY")?;
            loop {
                self.skip_whitespace();
                // Allow trailing commas / empty? No — require an expression.
                let item = self.parse_expr()?;
                if let Expr::Over {
                    partition_by, when, ..
                } = &item
                {
                    if window_partition_by.is_none() {
                        window_partition_by = partition_by.as_deref().cloned();
                    }
                    if window_trigger_condition.is_none() {
                        window_trigger_condition = when.as_deref().cloned();
                    }
                }
                // Recognize window calls case-insensitively; they go to `window`,
                // remaining expressions go to `group_by`.
                if let Some(w) = Self::try_parse_window_def(&item)? {
                    window = Some(w);
                    loop {
                        self.skip_whitespace();
                        if self.peek_word_is("FILTER") {
                            self.match_keyword("FILTER");
                            self.expect_char('(')?;
                            self.expect_keyword("WHERE")?;
                            window_filter = Some(self.parse_expr()?);
                            self.expect_char(')')?;
                        } else if self.peek_word_is("OVER") {
                            self.match_keyword("OVER");
                            self.expect_char('(')?;
                            loop {
                                self.skip_whitespace();
                                if self.peek_word_is("WHEN") {
                                    self.match_keyword("WHEN");
                                    window_trigger_condition = Some(self.parse_expr()?);
                                } else if self.peek_word_is("PARTITION") {
                                    self.match_keyword("PARTITION");
                                    self.expect_keyword("BY")?;
                                    window_partition_by = Some(self.parse_expr()?);
                                } else {
                                    break;
                                }
                            }
                            self.expect_char(')')?;
                        } else {
                            break;
                        }
                    }
                } else {
                    group_by.push(item);
                }
                self.skip_whitespace();
                if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }

        let mut having = None;
        if self.match_keyword("HAVING") {
            having = Some(self.parse_expr()?);
        }

        let mut order_by = Vec::new();
        if self.match_keyword("ORDER") {
            self.expect_keyword("BY")?;
            loop {
                self.skip_whitespace();
                let expr = self.parse_expr()?;
                let order = if self.peek_word_is("DESC") {
                    self.match_keyword("DESC");
                    SortOrder::Desc
                } else {
                    if self.peek_word_is("ASC") {
                        self.match_keyword("ASC");
                    }
                    SortOrder::Asc
                };
                order_by.push(OrderByItem { expr, order });
                self.skip_whitespace();
                if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }

        let mut limit: Option<usize> = None;
        if self.match_keyword("LIMIT") {
            self.skip_whitespace();
            let token = self
                .peek_word()
                .ok_or_else(|| anyhow::anyhow!("Expected integer after LIMIT"))?;
            let n: usize = token
                .parse()
                .map_err(|_| anyhow::anyhow!("Expected integer after LIMIT, found '{}'", token))?;
            self.skip_whitespace();
            self.pos += token.len();
            limit = Some(n);
        }

        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(';') {
            self.pos += 1;
        }

        // Set operations bind loosest of all: `SELECT ... UNION [ALL] SELECT ...`.
        // Right-recursive, so `A UNION B UNION C` parses as `A UNION (B UNION C)`.
        let mut set_op: Option<(SetOp, Box<SelectStmt>)> = None;
        if self.match_keyword("UNION") {
            let op = if self.match_keyword("ALL") {
                SetOp::UnionAll
            } else {
                SetOp::Union
            };
            set_op = Some((op, Box::new(self.parse_select()?)));
        }

        Ok(SelectStmt {
            fields,
            field_aliases,
            from,
            from_alias,
            joins,
            where_clause,
            group_by,
            window,
            window_filter,
            window_trigger_condition,
            window_partition_by,
            having,
            order_by,
            limit,
            set_op,
        })
    }

    /// If `expr` is a recognized window call, convert it to a `WindowDef`.
    /// Returns `Ok(None)` for non-window expressions.
    fn try_parse_window_def(expr: &Expr) -> Result<Option<WindowDef>> {
        let (name, args) = match expr {
            Expr::Call { name, args } => (name, args),
            Expr::Over { call, .. } => match &**call {
                Expr::Call { name, args } => (name, args),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        match name.to_ascii_lowercase().as_str() {
            "statewindow" => {
                if args.is_empty() || args.len() > 2 {
                    bail!("STATEWINDOW expects 1 or 2 arguments, got {}", args.len());
                }
                let start_condition = args[0].clone();
                let end_condition = if args.len() == 2 {
                    Some(args[1].clone())
                } else {
                    None
                };
                Ok(Some(WindowDef::State {
                    start_condition,
                    end_condition,
                }))
            }
            "tumblingwindow" => {
                if args.len() != 2 {
                    bail!(
                        "TUMBLINGWINDOW expects 2 arguments (unit, length), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let length = Self::parse_window_u64(&args[1])?;
                Ok(Some(WindowDef::TumblingTime { unit, length }))
            }
            "hoppingwindow" => {
                if args.len() != 3 {
                    bail!(
                        "HOPPINGWINDOW expects 3 arguments (unit, length, interval), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let length = Self::parse_window_u64(&args[1])?;
                let interval = Self::parse_window_u64(&args[2])?;
                Ok(Some(WindowDef::HoppingTime {
                    unit,
                    length,
                    interval,
                }))
            }
            "slidingwindow" => {
                if args.len() != 2 && args.len() != 3 {
                    bail!(
                        "SLIDINGWINDOW expects 2 or 3 arguments (unit, length[, delay]), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let length = Self::parse_window_u64(&args[1])?;
                let delay = if args.len() == 3 {
                    Some(Self::parse_window_u64(&args[2])?)
                } else {
                    None
                };
                Ok(Some(WindowDef::SlidingTime {
                    unit,
                    length,
                    delay,
                }))
            }
            "countwindow" => {
                if args.len() != 1 && args.len() != 2 {
                    bail!(
                        "COUNTWINDOW expects 1 or 2 arguments (size[, interval]), got {}",
                        args.len()
                    );
                }
                let size = Self::parse_window_usize(&args[0])?;
                let interval = if args.len() == 2 {
                    Some(Self::parse_window_usize(&args[1])?)
                } else {
                    None
                };
                Ok(Some(WindowDef::Count { size, interval }))
            }
            "sessionwindow" => {
                if args.len() != 3 {
                    bail!(
                        "SESSIONWINDOW expects 3 arguments (unit, maxDuration, timeout), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let max_duration = Self::parse_window_u64(&args[1])?;
                let timeout = Self::parse_window_u64(&args[2])?;
                if max_duration == 0 || timeout == 0 {
                    bail!("SESSIONWINDOW maxDuration and timeout must be positive");
                }
                Ok(Some(WindowDef::Session {
                    unit,
                    max_duration,
                    timeout,
                }))
            }
            _ => Ok(None),
        }
    }

    fn parse_window_unit(expr: &Expr) -> Result<TimeUnit> {
        match expr {
            Expr::Identifier(name) => Self::time_unit_from_str(name),
            Expr::Literal(serde_json::Value::String(s)) => Self::time_unit_from_str(s),
            _ => bail!("Window time unit must be one of dd, hh, mi, ss, ms"),
        }
    }

    fn time_unit_from_str(s: &str) -> Result<TimeUnit> {
        match s.to_ascii_lowercase().as_str() {
            "dd" => Ok(TimeUnit::Dd),
            "hh" => Ok(TimeUnit::Hh),
            "mi" => Ok(TimeUnit::Mi),
            "ss" => Ok(TimeUnit::Ss),
            "ms" => Ok(TimeUnit::Ms),
            _ => bail!(
                "Unknown time unit '{}', expected one of dd, hh, mi, ss, ms",
                s
            ),
        }
    }

    fn parse_window_u64(expr: &Expr) -> Result<u64> {
        match expr {
            Expr::Literal(v) => {
                if let Some(u) = v.as_u64() {
                    return Ok(u);
                }
                if let Some(i) = v.as_i64() {
                    if i >= 0 {
                        return Ok(i as u64);
                    }
                    bail!("Window length/interval must be non-negative, got {}", i);
                }
                if let Some(f) = v.as_f64() {
                    if f.is_finite() && f >= 0.0 && f.trunc() == f {
                        return Ok(f as u64);
                    }
                    bail!(
                        "Window length/interval must be a non-negative integer, got {}",
                        f
                    );
                }
                bail!("Window length/interval must be an integer")
            }
            _ => bail!("Window length/interval must be an integer literal"),
        }
    }

    fn parse_window_usize(expr: &Expr) -> Result<usize> {
        Ok(Self::parse_window_u64(expr)? as usize)
    }

    pub fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    pub fn parse_expression(input: &str) -> Result<Expr> {
        let mut p = Parser::new(input);
        p.parse_expr()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.match_keyword("OR") {
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_range()?;
        while self.peek_word_is("AND") {
            // Need to be careful: AND could be part of BETWEEN's low AND high,
            // but BETWEEN's AND is already consumed inside parse_range,
            // so any remaining AND here is a logical conjunction.
            // However, to avoid consuming an AND that belongs to an unfinished BETWEEN
            // (should not happen since parse_range consumes it), just consume.
            self.match_keyword("AND");
            let right = self.parse_range()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Range/Set/Null level: [NOT] BETWEEN, [NOT] IN, IS [NOT] NULL
    /// Binds looser than comparison, tighter than AND.
    fn parse_range(&mut self) -> Result<Expr> {
        let left = self.parse_comparison()?;

        self.skip_whitespace();
        let next = self.peek_word();

        let Some(w) = next else {
            return Ok(left);
        };

        if w.eq_ignore_ascii_case("BETWEEN") {
            self.match_keyword("BETWEEN");
            let low = self.parse_comparison()?;
            self.expect_keyword("AND")?;
            let high = self.parse_comparison()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            });
        } else if w.eq_ignore_ascii_case("NOT") {
            // Lookahead: NOT BETWEEN / NOT IN (NOT LIKE is handled in comparison layer,
            // but handle it here as fallback too)
            let saved = self.pos;
            self.match_keyword("NOT");
            if let Some(w2) = self.peek_word() {
                if w2.eq_ignore_ascii_case("BETWEEN") {
                    self.match_keyword("BETWEEN");
                    let low = self.parse_comparison()?;
                    self.expect_keyword("AND")?;
                    let high = self.parse_comparison()?;
                    return Ok(Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: true,
                    });
                } else if w2.eq_ignore_ascii_case("IN") {
                    self.match_keyword("IN");
                    let list = self.parse_in_list()?;
                    return Ok(Expr::InList {
                        expr: Box::new(left),
                        list,
                        negated: true,
                    });
                } else if w2.eq_ignore_ascii_case("LIKE") {
                    self.match_keyword("LIKE");
                    let right = self.parse_comparison()?;
                    let like_expr = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::Like,
                        right: Box::new(right),
                    };
                    return Ok(Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(like_expr),
                    });
                } else {
                    // NOT not followed by BETWEEN/IN/LIKE: restore and return left.
                    self.pos = saved;
                    return Ok(left);
                }
            } else {
                self.pos = saved;
                return Ok(left);
            }
        } else if w.eq_ignore_ascii_case("IN") {
            self.match_keyword("IN");
            let list = self.parse_in_list()?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: false,
            });
        } else if w.eq_ignore_ascii_case("IS") {
            self.match_keyword("IS");
            let negated = if self.peek_word_is("NOT") {
                self.match_keyword("NOT");
                true
            } else {
                false
            };
            self.expect_keyword("NULL")?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        } else if w.eq_ignore_ascii_case("LIKE") {
            // Fallback in case comparison layer didn't consume (should already be consumed,
            // but handle for robustness when left contains range? Actually comparison already
            // handles LIKE, so this branch is normally unreachable. Keep for safety.)
            self.match_keyword("LIKE");
            let right = self.parse_comparison()?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_in_list(&mut self) -> Result<Vec<Expr>> {
        self.skip_whitespace();
        self.expect_char('(')?;
        let mut list = Vec::new();
        self.skip_whitespace();
        // Allow empty list? Treat as empty (IN () is always false). But require ')' handling.
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(')') {
            self.pos += 1;
            return Ok(list);
        }
        loop {
            self.skip_whitespace();
            // Trailing ')' without element would be error unless empty (handled above)
            if self.pos < self.input.len() && self.input[self.pos..].starts_with(')') {
                break;
            }
            let e = self.parse_expr()?;
            list.push(e);
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.skip_whitespace();
        self.expect_char(')')?;
        Ok(list)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_bitor()?;

        self.skip_whitespace();
        // Check for NOT LIKE (infix): left NOT LIKE right
        if self.peek_word_is("NOT") {
            let saved = self.pos;
            self.match_keyword("NOT");
            if self.peek_word_is("LIKE") {
                self.match_keyword("LIKE");
                let right = self.parse_bitor()?;
                let like_expr = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Like,
                    right: Box::new(right),
                };
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(like_expr),
                });
            } else {
                // Not a NOT LIKE: restore so parse_range can handle NOT BETWEEN / NOT IN.
                self.pos = saved;
                return Ok(left);
            }
        }

        if self.peek_word_is("LIKE") {
            self.match_keyword("LIKE");
            let right = self.parse_bitor()?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            });
        }

        self.skip_whitespace();
        if self.pos >= self.input.len() {
            return Ok(left);
        }
        let rem = &self.input[self.pos..];

        let (op, len) = if rem.starts_with(">=") {
            (BinaryOperator::Gte, 2)
        } else if rem.starts_with("<=") {
            (BinaryOperator::Lte, 2)
        } else if rem.starts_with("!=") || rem.starts_with("<>") {
            (BinaryOperator::Neq, 2)
        } else if rem.starts_with("==") {
            (BinaryOperator::Eq, 2)
        } else if rem.starts_with('>') {
            (BinaryOperator::Gt, 1)
        } else if rem.starts_with('<') {
            (BinaryOperator::Lt, 1)
        } else if rem.starts_with('=') {
            (BinaryOperator::Eq, 1)
        } else {
            return Ok(left);
        };

        self.pos += len;
        let right = self.parse_bitor()?;
        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_bitor(&mut self) -> Result<Expr> {
        let mut left = self.parse_bitxor()?;
        loop {
            self.skip_whitespace();
            if self.pos < self.input.len()
                && self.input[self.pos..].starts_with('|')
                && !self.input[self.pos..].starts_with("||")
            {
                self.pos += 1;
                let right = self.parse_bitxor()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::BitOr,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_bitxor(&mut self) -> Result<Expr> {
        let mut left = self.parse_bitand()?;
        loop {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('^') {
                self.pos += 1;
                let right = self.parse_bitand()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::BitXor,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_bitand(&mut self) -> Result<Expr> {
        let mut left = self.parse_additive()?;
        loop {
            self.skip_whitespace();
            if self.pos < self.input.len()
                && self.input[self.pos..].starts_with('&')
                && !self.input[self.pos..].starts_with("&&")
            {
                self.pos += 1;
                let right = self.parse_additive()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::BitAnd,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                break;
            }
            let rem = &self.input[self.pos..];
            if rem.starts_with('+') {
                self.pos += 1;
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Add,
                    right: Box::new(right),
                };
            } else if rem.starts_with('-') {
                self.pos += 1;
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Sub,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                break;
            }
            let rem = &self.input[self.pos..];
            if rem.starts_with('*') {
                self.pos += 1;
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Mul,
                    right: Box::new(right),
                };
            } else if rem.starts_with('/') {
                self.pos += 1;
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Div,
                    right: Box::new(right),
                };
            } else if rem.starts_with('%') {
                self.pos += 1;
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Mod,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        self.skip_whitespace();
        if self.peek_word_is("NOT") {
            self.match_keyword("NOT");
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }
        self.skip_whitespace();
        if self.pos < self.input.len() {
            let rem = &self.input[self.pos..];
            if rem.starts_with('-') {
                self.pos += 1;
                let expr = self.parse_unary()?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    expr: Box::new(expr),
                });
            } else if rem.starts_with('+') {
                // Unary plus: no-op, just consume and recurse
                self.pos += 1;
                return self.parse_unary();
            }
        }
        self.parse_primary()
    }

    /// Object-literal key: a quoted string or a bare identifier.
    fn parse_object_key(&mut self) -> Result<String> {
        self.skip_whitespace();
        if self.pos >= self.input.len() {
            bail!("Unexpected end of object literal");
        }
        let rem = &self.input[self.pos..];
        if rem.starts_with('\'') || rem.starts_with('"') {
            let quote = rem.chars().next().unwrap();
            let rest = &rem[quote.len_utf8()..];
            let end = rest
                .find(quote)
                .ok_or_else(|| anyhow::anyhow!("Unterminated object key"))?;
            let key = rest[..end].to_string();
            self.pos += quote.len_utf8() + end + quote.len_utf8();
            return Ok(key);
        }
        let Some(word) = self.peek_word() else {
            bail!("Expected object key");
        };
        self.skip_whitespace();
        self.pos += word.len();
        Ok(word)
    }

    pub fn parse_backtick_identifier(&mut self) -> Result<String> {
        self.skip_whitespace();
        if !(self.pos < self.input.len() && self.input[self.pos..].starts_with('`')) {
            bail!("Expected backtick identifier");
        }
        self.pos += 1;
        let start = self.pos;
        while self.pos < self.input.len() && !self.input[self.pos..].starts_with('`') {
            self.pos += 1;
        }
        if self.pos >= self.input.len() {
            bail!("Unclosed backtick identifier");
        }
        let ident = self.input[start..self.pos].to_string();
        self.pos += 1;
        Ok(ident)
    }

    pub fn parse_column_identifier(&mut self) -> Result<String> {
        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
            self.parse_backtick_identifier()
        } else {
            let col = self
                .peek_word()
                .ok_or_else(|| anyhow::anyhow!("Expected column identifier"))?;
            self.pos += col.len();
            Ok(col)
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        self.skip_whitespace();
        if self.pos >= self.input.len() {
            bail!("Unexpected end of expression");
        }

        let rem = &self.input[self.pos..];
        // Backtick identifier: `identifier`
        if rem.starts_with('`') {
            let ident = self.parse_backtick_identifier()?;
            return self.parse_postfix(Expr::Identifier(ident));
        }

        // Parentheses: (...)
        if rem.starts_with('(') {
            self.pos += 1;
            let expr = self.parse_expr()?;
            self.skip_whitespace();
            self.expect_char(')')?;
            return self.parse_postfix(expr);
        }

        // String literal
        if rem.starts_with('\'') || rem.starts_with('"') {
            let quote = rem.chars().next().unwrap();
            let rest = &rem[quote.len_utf8()..];
            let end = rest
                .find(quote)
                .ok_or_else(|| anyhow::anyhow!("Unterminated string literal"))?;
            let s = &rest[..end];
            self.pos += quote.len_utf8() + end + quote.len_utf8();
            return self.parse_postfix(Expr::Literal(serde_json::Value::String(s.to_string())));
        }

        // Array literal: [e1, e2, ...] — desugared to array_create(...).
        if rem.starts_with('[') {
            self.pos += 1;
            let mut args = Vec::new();
            self.skip_whitespace();
            if !(self.pos < self.input.len() && self.input[self.pos..].starts_with(']')) {
                loop {
                    self.skip_whitespace();
                    if self.pos < self.input.len() && self.input[self.pos..].starts_with(']') {
                        break;
                    }
                    args.push(self.parse_expr()?);
                    self.skip_whitespace();
                    if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
            }
            self.skip_whitespace();
            self.expect_char(']')?;
            return self.parse_postfix(Expr::Call {
                name: "array_create".to_string(),
                args,
            });
        }

        // Object literal: {"k": v, ...} — desugared to json_map("k", v, ...).
        // Keys may be quoted strings or bare identifiers.
        if rem.starts_with('{') {
            self.pos += 1;
            let mut args = Vec::new();
            self.skip_whitespace();
            if !(self.pos < self.input.len() && self.input[self.pos..].starts_with('}')) {
                loop {
                    self.skip_whitespace();
                    if self.pos < self.input.len() && self.input[self.pos..].starts_with('}') {
                        break;
                    }
                    let key = self.parse_object_key()?;
                    self.skip_whitespace();
                    self.expect_char(':')?;
                    let val = self.parse_expr()?;
                    args.push(Expr::Literal(serde_json::Value::String(key)));
                    args.push(val);
                    self.skip_whitespace();
                    if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
            }
            self.skip_whitespace();
            self.expect_char('}')?;
            return self.parse_postfix(Expr::Call {
                name: "json_map".to_string(),
                args,
            });
        }

        // Numeric literal: digit or '.' followed by digit
        if Self::is_number_start(rem) {
            let num = self.parse_number()?;
            return self.parse_postfix(num);
        }

        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected expression token"))?;
        // Consume word: pos is at word start after skip_whitespace
        self.skip_whitespace();
        self.pos += word.len();

        if word.eq_ignore_ascii_case("true") {
            self.parse_postfix(Expr::Literal(serde_json::Value::Bool(true)))
        } else if word.eq_ignore_ascii_case("false") {
            self.parse_postfix(Expr::Literal(serde_json::Value::Bool(false)))
        } else if word.eq_ignore_ascii_case("null") {
            self.parse_postfix(Expr::Literal(serde_json::Value::Null))
        } else if word.eq_ignore_ascii_case("CASE") {
            let case_expr = self.parse_case()?;
            self.parse_postfix(case_expr)
        } else {
            // Function call: name '(' args ')' — usable in SELECT and WHERE/expressions.
            // Allow optional whitespace between name and '('.
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
                self.pos += 1;
                let mut args = Vec::new();
                self.skip_whitespace();
                if !(self.pos < self.input.len() && self.input[self.pos..].starts_with(')')) {
                    loop {
                        self.skip_whitespace();
                        // Support '*' as wildcard arg (e.g. count(*)) for forward-compat.
                        if self.pos < self.input.len() && self.input[self.pos..].starts_with('*') {
                            // Only treat as lone '*' (followed by ',' or ')').
                            let after_star = &self.input[self.pos + 1..];
                            let after_trim = after_star.trim_start();
                            if after_trim.starts_with(',') || after_trim.starts_with(')') {
                                self.pos += 1;
                                args.push(Expr::Wildcard);
                                self.skip_whitespace();
                                if self.pos < self.input.len()
                                    && self.input[self.pos..].starts_with(',')
                                {
                                    self.pos += 1;
                                    continue;
                                } else {
                                    break;
                                }
                            }
                        }
                        let arg = self.parse_expr()?;
                        args.push(arg);
                        self.skip_whitespace();
                        if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                            self.pos += 1;
                        } else {
                            break;
                        }
                    }
                }
                self.skip_whitespace();
                self.expect_char(')')?;
                let call_expr = Expr::Call { name: word, args };
                // Analytic OVER clause: func(...) OVER ([PARTITION BY expr] [WHEN cond]).
                self.skip_whitespace();
                if self.match_keyword("OVER") {
                    self.skip_whitespace();
                    self.expect_char('(')?;
                    let mut partition_by = None;
                    let mut when = None;
                    loop {
                        self.skip_whitespace();
                        if self.peek_word_is("PARTITION") {
                            self.match_keyword("PARTITION");
                            self.expect_keyword("BY")?;
                            partition_by = Some(Box::new(self.parse_expr()?));
                        } else if self.peek_word_is("WHEN") {
                            self.match_keyword("WHEN");
                            when = Some(Box::new(self.parse_expr()?));
                        } else {
                            break;
                        }
                    }
                    self.skip_whitespace();
                    self.expect_char(')')?;
                    let over_expr = Expr::Over {
                        call: Box::new(call_expr),
                        partition_by,
                        when,
                    };
                    return self.parse_postfix(over_expr);
                }
                return self.parse_postfix(call_expr);
            }
            let expr = Expr::Identifier(word);
            self.parse_postfix(expr)
        }
    }

    fn parse_postfix(&mut self, mut expr: Expr) -> Result<Expr> {
        loop {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('.') {
                self.pos += 1;
                self.skip_whitespace();
                let field = if self.pos < self.input.len()
                    && self.input[self.pos..].starts_with('*')
                {
                    self.pos += 1;
                    "*".to_string()
                } else if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
                    self.parse_backtick_identifier()?
                } else {
                    let f = self
                        .peek_word()
                        .ok_or_else(|| anyhow::anyhow!("Expected field name after '.'"))?;
                    self.pos += f.len();
                    f
                };
                expr = Expr::FieldAccess {
                    parent: Box::new(expr),
                    field,
                };
            } else if self.pos + 1 < self.input.len() && self.input[self.pos..].starts_with("->") {
                self.pos += 2;
                self.skip_whitespace();
                let field = if self.pos < self.input.len()
                    && (self.input[self.pos..].starts_with('\'')
                        || self.input[self.pos..].starts_with('"'))
                {
                    let quote = self.input[self.pos..].chars().next().unwrap();
                    let rest = &self.input[self.pos + quote.len_utf8()..];
                    let end = rest
                        .find(quote)
                        .ok_or_else(|| anyhow::anyhow!("Unterminated field name after '->'"))?;
                    let f = rest[..end].to_string();
                    self.pos += quote.len_utf8() + end + quote.len_utf8();
                    f
                } else if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
                    self.parse_backtick_identifier()?
                } else {
                    let w = self
                        .peek_word()
                        .ok_or_else(|| anyhow::anyhow!("Expected field name after '->'"))?;
                    self.pos += w.len();
                    w
                };
                expr = Expr::FieldAccess {
                    parent: Box::new(expr),
                    field,
                };
            } else if self.pos < self.input.len() && self.input[self.pos..].starts_with('[') {
                self.pos += 1;
                self.skip_whitespace();
                if self.pos < self.input.len() && self.input[self.pos..].starts_with(':') {
                    self.pos += 1;
                    self.skip_whitespace();
                    let hi =
                        if self.pos < self.input.len() && self.input[self.pos..].starts_with(']') {
                            None
                        } else {
                            Some(Box::new(self.parse_expr()?))
                        };
                    self.skip_whitespace();
                    self.expect_char(']')?;
                    expr = Expr::Slice {
                        base: Box::new(expr),
                        lo: None,
                        hi,
                    };
                } else {
                    let first = self.parse_expr()?;
                    self.skip_whitespace();
                    if self.pos < self.input.len() && self.input[self.pos..].starts_with(':') {
                        self.pos += 1;
                        self.skip_whitespace();
                        let hi = if self.pos < self.input.len()
                            && self.input[self.pos..].starts_with(']')
                        {
                            None
                        } else {
                            Some(Box::new(self.parse_expr()?))
                        };
                        self.skip_whitespace();
                        self.expect_char(']')?;
                        expr = Expr::Slice {
                            base: Box::new(expr),
                            lo: Some(Box::new(first)),
                            hi,
                        };
                    } else {
                        self.expect_char(']')?;
                        expr = Expr::Index {
                            base: Box::new(expr),
                            index: Box::new(first),
                        };
                    }
                }
            } else {
                break;
            }
        }
        Ok(expr)
    }

    /// Optional source alias after FROM/JOIN targets: `AS a` or bare `a`.
    /// Clause keywords never count as aliases.
    fn parse_optional_alias(&mut self) -> Result<Option<String>> {
        self.skip_whitespace();
        if self.match_keyword("AS") {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
                return Ok(Some(self.parse_backtick_identifier()?));
            }
            let name = self
                .peek_word()
                .ok_or_else(|| anyhow::anyhow!("Expected alias after AS"))?;
            self.skip_whitespace();
            self.pos += name.len();
            return Ok(Some(name));
        }
        if self.pos < self.input.len() && self.input[self.pos..].starts_with('`') {
            return Ok(Some(self.parse_backtick_identifier()?));
        }
        const RESERVED: &[&str] = &[
            "WHERE",
            "GROUP",
            "ORDER",
            "LIMIT",
            "HAVING",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
            "OUTER",
            "ASOF",
            "ON",
            "UNION",
            "ALL",
            "SELECT",
            "FROM",
            "AS",
            "DESC",
            "ASC",
            "OVER",
            "PARTITION",
            "BY",
            "BETWEEN",
            "IN",
            "LIKE",
            "IS",
            "NOT",
            "NULL",
            "AND",
            "OR",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
        ];
        if let Some(w) = self.peek_word() {
            if !RESERVED.iter().any(|r| w.eq_ignore_ascii_case(r)) {
                self.skip_whitespace();
                self.pos += w.len();
                return Ok(Some(w));
            }
        }
        Ok(None)
    }

    /// Parse a CASE expression. Called after the `CASE` keyword is consumed.
    /// Supports both forms:
    /// - searched: `CASE WHEN <cond> THEN <result> ... [ELSE <result>] END`
    /// - simple:   `CASE <operand> WHEN <value> THEN <result> ... [ELSE <result>] END`
    fn parse_case(&mut self) -> Result<Expr> {
        // Searched CASE when the next word is WHEN; otherwise parse the operand.
        let operand = if self.peek_word_is("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };

        let mut when_clauses = Vec::new();
        while self.match_keyword("WHEN") {
            let condition = self.parse_expr()?;
            self.expect_keyword("THEN")?;
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }
        if when_clauses.is_empty() {
            bail!("CASE expression requires at least one WHEN clause");
        }

        let else_clause = if self.match_keyword("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword("END")?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn is_number_start(s: &str) -> bool {
        let mut chars = s.chars();
        match chars.next() {
            Some(c) if c.is_ascii_digit() => true,
            Some('.') => matches!(chars.next(), Some(n) if n.is_ascii_digit()),
            _ => false,
        }
    }

    fn parse_number(&mut self) -> Result<Expr> {
        self.skip_whitespace();
        let start = self.pos;
        let bytes = self.input.as_bytes();
        let mut i = self.pos;
        // Integer part
        while i < self.input.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        // Fractional part
        if i < self.input.len() && bytes[i] == b'.' {
            // Only treat '.' as decimal if followed by digit OR if we already have digits
            // (to support "25.0" and ".2" but not "a.b" which is handled as identifier path;
            // here we are already in number branch so at least one side is digit)
            let next_is_digit = i + 1 < self.input.len() && bytes[i + 1].is_ascii_digit();
            let has_int_part = i > start;
            if next_is_digit || has_int_part {
                // Consume '.' if followed by digit, or if has int part allow trailing '.'?
                // Require digit after '.' to be a valid float unless it's like "25."?
                // For simplicity, consume '.' and following digits (if any).
                i += 1;
                while i < self.input.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
        }
        // Exponent part
        if i < self.input.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
            let mut j = i + 1;
            if j < self.input.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                j += 1;
            }
            let exp_start = j;
            while j < self.input.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > exp_start {
                i = j;
            }
            // else: not a valid exponent, leave 'e' unconsumed
        }

        let text = &self.input[start..i];
        if text.is_empty() || text == "." {
            bail!("Invalid number literal");
        }
        self.pos = i;

        // Try integer first if no float markers
        if !text.contains('.') && !text.contains('e') && !text.contains('E') {
            if let Ok(n) = text.parse::<i64>() {
                return Ok(Expr::Literal(serde_json::json!(n)));
            }
            // Fall back to u64 (large positive) then f64
            if let Ok(n) = text.parse::<u64>() {
                return Ok(Expr::Literal(serde_json::json!(n)));
            }
        }
        if let Ok(f) = text.parse::<f64>() {
            return Ok(Expr::Literal(serde_json::json!(f)));
        }
        bail!("Invalid number literal '{}'", text)
    }
}

/// Split on top-level commas, ignoring commas nested inside parentheses or
/// quotes (e.g. the `10,2` in `DECIMAL(10,2)`).
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut start = 0usize;
    for (off, ch) in s.char_indices() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
        } else if in_double {
            if ch == '"' {
                in_double = false;
            }
        } else {
            match ch {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    parts.push(&s[start..off]);
                    start = off + 1;
                }
                '\'' => in_single = true,
                '"' => in_double = true,
                _ => {}
            }
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Split at `DEFAULT` keyword (case-insensitive) at depth 0 (outside parentheses and quotes).
fn split_at_default(s: &str) -> (&str, Option<&str>) {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        let ch = bytes[i] as char;
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
        } else if in_double {
            if ch == '"' {
                in_double = false;
            }
        } else {
            match ch {
                '(' | '[' => depth += 1,
                ')' | ']' => depth = depth.saturating_sub(1),
                '\'' => in_single = true,
                '"' => in_double = true,
                _ if depth == 0 && s[i..].to_ascii_lowercase().starts_with("default") => {
                    let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
                    let after_idx = i + 7;
                    let after_ok = after_idx >= len || bytes[after_idx].is_ascii_whitespace();
                    if before_ok && after_ok {
                        let type_part = s[..i].trim();
                        let default_part = s[after_idx..].trim();
                        return (type_part, Some(default_part));
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    (s.trim(), None)
}

/// Validate whether a declared stream or table column data type is supported.
pub fn is_valid_stream_type(ty: &str) -> bool {
    let t = ty.trim().to_ascii_lowercase();
    if t.is_empty() {
        return false;
    }
    match t.as_str() {
        "bigint" | "int" | "integer" | "smallint" | "tinyint" => true,
        "float" | "double" | "real" | "numeric" | "decimal" => true,
        "string" | "text" | "varchar" | "char" => true,
        "datetime" | "timestamp" | "date" | "time" => true,
        "boolean" | "bool" => true,
        "bytea" | "binary" | "blob" => true,
        "array" => true,
        "struct" => true,
        "json" | "object" | "map" => true,
        _ => {
            if (t.starts_with("decimal(") && t.ends_with(')'))
                || (t.starts_with("numeric(") && t.ends_with(')'))
            {
                let inner = t[t.find('(').unwrap() + 1..t.len() - 1].trim();
                let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
                (parts.len() == 1 || parts.len() == 2)
                    && parts
                        .iter()
                        .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
            } else if (t.starts_with("varchar(") && t.ends_with(')'))
                || (t.starts_with("char(") && t.ends_with(')'))
            {
                let inner = t[t.find('(').unwrap() + 1..t.len() - 1].trim();
                !inner.is_empty() && inner.chars().all(|c| c.is_ascii_digit())
            } else if (t.starts_with("array(") && t.ends_with(')'))
                || (t.starts_with("array[") && t.ends_with(']'))
            {
                let inner = t[6..t.len() - 1].trim();
                if inner.is_empty() {
                    true
                } else {
                    is_valid_stream_type(inner)
                }
            } else if t.ends_with("[]") {
                is_valid_stream_type(t[..t.len() - 2].trim())
            } else if t.starts_with("struct(") && t.ends_with(')') {
                let inner = t[7..t.len() - 1].trim();
                if inner.is_empty() {
                    true
                } else {
                    for field_part in split_top_level_commas(inner) {
                        let field_part = field_part.trim();
                        if field_part.is_empty() {
                            continue;
                        }
                        let tokens: Vec<&str> = field_part.split_whitespace().collect();
                        if tokens.len() == 1 {
                            if !is_valid_stream_type(tokens[0]) {
                                return false;
                            }
                        } else if let Some((_, field_type)) =
                            field_part.split_once(char::is_whitespace)
                        {
                            if !is_valid_stream_type(field_type.trim()) {
                                return false;
                            }
                        }
                    }
                    true
                }
            } else {
                false
            }
        }
    }
}

pub fn parse_expression(input: &str) -> Result<Expr> {
    Parser::parse_expression(input)
}
