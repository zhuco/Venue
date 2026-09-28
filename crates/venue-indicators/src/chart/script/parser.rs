use super::*;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;

#[derive(Clone, Debug)]
struct Token {
    text: String,
    line: usize,
    quoted: bool,
}

fn lex(source: &str) -> Result<Vec<Token>, String> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err("源码超过 32 KiB".into());
    }
    let chars: Vec<char> = source.chars().collect();
    let (mut i, mut line) = (0, 1);
    let mut result = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            if c == '\n' {
                line += 1;
            }
            i += 1;
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        let quoted = c == '\'' || c == '"';
        let text = if quoted {
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != c {
                if chars[i] == '\n' {
                    return Err(format!("第{line}行：字符串不能跨行"));
                }
                if chars[i] == '\\' {
                    i += 1;
                }
                let ch = chars
                    .get(i)
                    .ok_or_else(|| format!("第{line}行：未结束字符串"))?;
                s.push(*ch);
                i += 1;
            }
            if i == chars.len() {
                return Err(format!("第{line}行：未结束字符串"));
            }
            if s.len() > 512 {
                return Err(format!("第{line}行：单个字符串最多512字节"));
            }
            i += 1;
            s
        } else if c.is_ascii_alphabetic() || c == '_' {
            i += 1;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || matches!(chars[i], '_' | '.'))
            {
                i += 1;
            }
            chars[start..i].iter().collect()
        } else if c.is_ascii_digit() || c == '.' {
            i += 1;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            chars[start..i].iter().collect()
        } else {
            i += 1;
            if i < chars.len()
                && matches!(
                    (c, chars[i]),
                    ('&', '&') | ('|', '|') | ('=', '=') | ('!', '=') | ('<', '=') | ('>', '=')
                )
            {
                i += 1;
            }
            chars[start..i].iter().collect()
        };
        result.push(Token { text, line, quoted });
    }
    Ok(result)
}

pub(super) fn compile(spec: &ScriptSpec) -> Result<ScriptEngine, String> {
    let mut p = Parser {
        tokens: lex(&spec.source)?,
        cursor: 0,
        names: HashMap::new(),
        tuples: HashMap::new(),
        plot_ids: HashMap::new(),
        depth: 0,
        engine: ScriptEngine {
            id: spec.id,
            nodes: Vec::new(),
            plots: Vec::new(),
            fills: Vec::new(),
            labels: Vec::new(),
            alerts: Vec::new(),
        },
    };
    for field in ["open", "high", "low", "close", "volume"] {
        let n = p.add(Expr::Field(field.into()))?;
        p.names.insert(field.into(), n);
    }
    for name in ["syminfo.ticker", "syminfo.prefix"] {
        let n = p.add(Expr::Field(name.into()))?;
        p.names.insert(name.into(), n);
    }
    while p.cursor < p.tokens.len() {
        if p.eat(";") {
            continue;
        }
        if p.eat("[") {
            let mut names = vec![p.identifier()?];
            while p.eat(",") {
                names.push(p.identifier()?);
            }
            p.require("]")?;
            p.require("=")?;
            let node = p.expression(0)?;
            let values = p
                .tuples
                .get(&node)
                .cloned()
                .ok_or_else(|| p.error("右侧不是多值函数"))?;
            if values.len() != names.len() {
                return Err(p.error("多值赋值数量不匹配"));
            }
            for (name, value) in names.into_iter().zip(values) {
                p.bind(name, value)?;
            }
        } else if p.tokens.get(p.cursor + 1).is_some_and(|t| t.text == "=") {
            let name = p.identifier()?;
            p.require("=")?;
            let value = p.expression(0)?;
            p.bind(name, value)?;
        } else {
            p.expression(0)?;
        }
        p.eat(";");
    }
    if p.engine.plots.is_empty() && p.engine.labels.is_empty() {
        return Err("至少需要一个 plot 或 plotText".into());
    }
    if p.engine
        .nodes
        .iter()
        .map(|n| {
            n.keep
                + match &n.expr {
                    Expr::ValueWhen(_, _, count, _) => count + 1,
                    Expr::Sma(_, sma) => sma.warmup_period(),
                    _ => 0,
                }
        })
        .sum::<usize>()
        > 16_384
    {
        return Err("历史状态超过16384个值，请缩短周期或减少表达式".into());
    }
    validate_types(&p.engine)?;
    Ok(p.engine)
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    names: HashMap<String, usize>,
    tuples: HashMap<usize, Vec<usize>>,
    plot_ids: HashMap<usize, usize>,
    depth: usize,
    engine: ScriptEngine,
}
impl Parser {
    fn error(&self, msg: &str) -> String {
        format!(
            "第{}行：{msg}",
            self.tokens
                .get(self.cursor)
                .or_else(|| self.tokens.last())
                .map_or(1, |t| t.line)
        )
    }
    fn eat(&mut self, s: &str) -> bool {
        if self
            .tokens
            .get(self.cursor)
            .is_some_and(|t| !t.quoted && t.text == s)
        {
            self.cursor += 1;
            true
        } else {
            false
        }
    }
    fn require(&mut self, s: &str) -> Result<(), String> {
        if self.eat(s) {
            Ok(())
        } else {
            Err(self.error(&format!("需要 {s}")))
        }
    }
    fn identifier(&mut self) -> Result<String, String> {
        let t = self
            .tokens
            .get(self.cursor)
            .ok_or_else(|| self.error("缺少变量名"))?;
        if t.quoted
            || !t
                .text
                .starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        {
            return Err(self.error("无效变量名"));
        }
        self.cursor += 1;
        Ok(t.text.clone())
    }
    fn bind(&mut self, name: String, value: usize) -> Result<(), String> {
        if self.names.contains_key(&name) {
            return Err(self.error("变量重复定义；不支持可变赋值"));
        }
        self.names.insert(name, value);
        Ok(())
    }
    fn add(&mut self, expr: Expr) -> Result<usize, String> {
        if self.engine.nodes.len() >= 2048 {
            return Err(self.error("表达式超过2048个"));
        }
        let id = self.engine.nodes.len();
        self.engine.nodes.push(Node {
            expr,
            history: VecDeque::new(),
            keep: 0,
        });
        Ok(id)
    }
    fn string(&mut self, s: &str) -> Result<usize, String> {
        self.add(Expr::Literal(Value::Text(s.into())))
    }
    fn number(&mut self, n: i64) -> Result<usize, String> {
        self.add(Expr::Literal(Value::Number(Some(Decimal::from(n)))))
    }
    fn constant(&self, n: usize) -> Result<Decimal, String> {
        match &self.engine.nodes[n].expr {
            Expr::Literal(Value::Number(Some(v))) => Ok(*v),
            _ => Err(self.error("参数必须为数字常量或 input 默认值")),
        }
    }
    fn count(&self, n: usize, min: usize) -> Result<usize, String> {
        let v = self.constant(n)?;
        let count = v
            .to_usize()
            .filter(|c| *c >= min && *c <= 1000 && v.fract().is_zero());
        count.ok_or_else(|| self.error("周期/历史索引必须为整数，最大1000"))
    }
    fn text_constant(&self, n: usize) -> Result<String, String> {
        match &self.engine.nodes[n].expr {
            Expr::Literal(Value::Text(v)) => Ok(v.clone()),
            _ => Err(self.error("参数必须为字符串常量")),
        }
    }
    fn keep(&mut self, n: usize, count: usize) {
        self.engine.nodes[n].keep = self.engine.nodes[n].keep.max(count);
    }
    fn lag(&mut self, n: usize, count: usize) -> Result<usize, String> {
        self.keep(n, count);
        self.add(Expr::Lag(n, count))
    }
    fn binary(&mut self, op: &str, a: usize, b: usize) -> Result<usize, String> {
        self.add(Expr::Binary(op.into(), a, b))
    }
    fn expression(&mut self, min: u8) -> Result<usize, String> {
        self.depth += 1;
        if self.depth > 64 {
            return Err(self.error("表达式嵌套超过64层"));
        }
        let result = self.expression_inner(min);
        self.depth -= 1;
        result
    }
    fn expression_inner(&mut self, min: u8) -> Result<usize, String> {
        let t = self
            .tokens
            .get(self.cursor)
            .cloned()
            .ok_or_else(|| self.error("缺少表达式"))?;
        self.cursor += 1;
        let mut left = if t.quoted {
            self.string(&t.text)?
        } else if t.text == "(" {
            let n = self.expression(0)?;
            self.require(")")?;
            n
        } else if matches!(t.text.as_str(), "not" | "!" | "-" | "+") {
            let n = self.expression(7)?;
            self.add(Expr::Unary(t.text, n))?
        } else if let Ok(v) = t.text.parse::<Decimal>() {
            self.add(Expr::Literal(Value::Number(Some(v))))?
        } else if t.text == "true" || t.text == "false" {
            self.number(i64::from(t.text == "true"))?
        } else if self.eat("(") {
            let (mut args, mut named) = (Vec::new(), HashMap::new());
            if !self.eat(")") {
                loop {
                    if self
                        .tokens
                        .get(self.cursor + 1)
                        .is_some_and(|t| t.text == "=")
                    {
                        let key = self.identifier()?;
                        self.require("=")?;
                        let n = self.expression(0)?;
                        if named.insert(key, n).is_some() {
                            return Err(self.error("重复命名参数"));
                        }
                    } else {
                        args.push(self.expression(0)?);
                    }
                    if self.eat(")") {
                        break;
                    }
                    self.require(",")?;
                }
            }
            self.call(&t.text, args, named)?
        } else {
            *self
                .names
                .get(&t.text)
                .ok_or_else(|| self.error(&format!("未知变量 {}", t.text)))?
        };
        loop {
            if self.eat("[") {
                let index = self.expression(0)?;
                let count = self.count(index, 0)?;
                self.require("]")?;
                left = self.lag(left, count)?;
                continue;
            }
            let Some(op) = self.tokens.get(self.cursor).map(|t| t.text.clone()) else {
                break;
            };
            if op == "?" && min == 0 {
                self.cursor += 1;
                let yes = self.expression(0)?;
                self.require(":")?;
                let no = self.expression(0)?;
                left = self.add(Expr::Select(left, yes, no))?;
                continue;
            }
            let precedence = match op.as_str() {
                "||" => 1,
                "&&" => 2,
                "==" | "!=" => 3,
                "<" | ">" | "<=" | ">=" => 4,
                "+" | "-" => 5,
                "*" | "/" => 6,
                _ => break,
            };
            if precedence < min {
                break;
            }
            self.cursor += 1;
            let right = self.expression(precedence + 1)?;
            left = self.binary(&op, left, right)?;
        }
        Ok(left)
    }
    fn call(
        &mut self,
        name: &str,
        a: Vec<usize>,
        mut kw: HashMap<String, usize>,
    ) -> Result<usize, String> {
        let allowed: &[&str] = match name {
            "input" => &["title"],
            "plot" => &["title", "color", "lineWidth"],
            "fill" => &["color"],
            "plotText" => &[
                "title",
                "text",
                "refSeries",
                "bgColor",
                "color",
                "fontSize",
                "placement",
            ],
            "alertcondition" => &["title", "direction"],
            _ => &[],
        };
        if kw.keys().any(|k| !allowed.contains(&k.as_str())) {
            return Err(self.error("不支持的命名参数"));
        }
        let valid = match name {
            "ema" | "sma" | "rsi" | "highest" | "lowest" | "max" | "min" | "ref" | "fill" => {
                a.len() == 2
            }
            "valuewhen" => a.len() == 3,
            "macd" => a.len() == 6,
            "ichimoku" => a.len() == 5,
            "input" | "abs" | "td" | "plot" | "plotText" | "alertcondition" | "log" => a.len() == 1,
            _ => return Err(self.error(&format!("不支持函数 {name}"))),
        };
        if !valid {
            return Err(self.error(&format!("{name} 参数数量错误")));
        }
        match name {
            "input" => {
                self.constant(a[0])?;
                Ok(a[0])
            }
            "log" => self.number(0),
            "ema" | "sma" | "rsi" => {
                let p = self.count(a[1], 1)?;
                let expr = match name {
                    "ema" => Expr::Ema(a[0], Ema::new(p).map_err(|e| e.to_string())?),
                    "sma" => Expr::Sma(a[0], Sma::new(p).map_err(|e| e.to_string())?),
                    _ => Expr::Rsi(a[0], Rsi::new(p).map_err(|e| e.to_string())?),
                };
                self.add(expr)
            }
            "highest" | "lowest" => {
                let p = self.count(a[1], 1)?;
                self.keep(a[0], p - 1);
                self.add(Expr::Extreme(a[0], p, name == "highest"))
            }
            "ref" => {
                let p = self.count(a[1], 0)?;
                self.lag(a[0], p)
            }
            "abs" => self.add(Expr::Unary("abs".into(), a[0])),
            "max" | "min" => self.binary(name, a[0], a[1]),
            "valuewhen" => {
                let p = self.count(a[2], 0)?;
                self.add(Expr::ValueWhen(a[0], a[1], p, VecDeque::new()))
            }
            "td" => {
                self.keep(a[0], 4);
                let n = self.add(Expr::Td(a[0], 0))?;
                self.tuples.insert(n, vec![n]);
                Ok(n)
            }
            "macd" => {
                if self.text_constant(a[4])? != "EMA" || self.text_constant(a[5])? != "EMA" {
                    return Err(self.error("MACD 仅支持 EMA/EMA"));
                }
                if self.count(a[1], 1)? >= self.count(a[2], 1)? {
                    return Err(self.error("MACD 慢周期必须大于快周期"));
                }
                let fast = self.call("ema", vec![a[0], a[1]], HashMap::new())?;
                let slow = self.call("ema", vec![a[0], a[2]], HashMap::new())?;
                let dif = self.binary("-", fast, slow)?;
                let dea = self.call("ema", vec![dif, a[3]], HashMap::new())?;
                let hist = self.binary("-", dif, dea)?;
                let two = self.number(2)?;
                let hist = self.binary("*", hist, two)?;
                self.tuples.insert(dif, vec![dif, dea, hist]);
                Ok(dif)
            }
            "ichimoku" => {
                let high = self.names["high"];
                let low = self.names["low"];
                let two = self.number(2)?;
                let mut mids = Vec::new();
                for period in &a[1..4] {
                    let h = self.call("highest", vec![high, *period], HashMap::new())?;
                    let l = self.call("lowest", vec![low, *period], HashMap::new())?;
                    let sum = self.binary("+", h, l)?;
                    mids.push(self.binary("/", sum, two)?);
                }
                let shift = self.count(a[4], 0)?;
                let sum = self.binary("+", mids[0], mids[1])?;
                let span = self.binary("/", sum, two)?;
                let span_a = self.lag(span, shift)?;
                let span_b = self.lag(mids[2], shift)?;
                self.tuples
                    .insert(mids[0], vec![mids[0], mids[1], a[0], span_a, span_b]);
                Ok(mids[0])
            }
            "plot" => {
                if self.engine.plots.len() >= 32 {
                    return Err(self.error("最多32条曲线"));
                }
                let color = match kw.remove("color") {
                    Some(n) => n,
                    None => self.string("#00E5FF")?,
                };
                let width = kw
                    .remove("lineWidth")
                    .map(|n| self.count(n, 1))
                    .transpose()?
                    .unwrap_or(1)
                    .min(4) as u8;
                let title = kw
                    .remove("title")
                    .map(|n| self.text_constant(n))
                    .transpose()?
                    .unwrap_or_else(|| "Plot".into());
                let index = self.engine.plots.len();
                self.engine.plots.push(Plot {
                    value: a[0],
                    color,
                    width,
                    title,
                });
                let n = self.number(index as i64)?;
                self.plot_ids.insert(n, index);
                Ok(n)
            }
            "fill" => {
                if self.engine.fills.len() >= 32 {
                    return Err(self.error("最多32个填充"));
                }
                let first = *self
                    .plot_ids
                    .get(&a[0])
                    .ok_or_else(|| self.error("fill 必须引用 plot 返回值"))?;
                let second = *self
                    .plot_ids
                    .get(&a[1])
                    .ok_or_else(|| self.error("fill 必须引用 plot 返回值"))?;
                let color = match kw.remove("color") {
                    Some(n) => n,
                    None => self.string("#00E676")?,
                };
                self.engine.fills.push((first, second, color));
                self.number(0)
            }
            "plotText" => {
                if self.engine.labels.len() >= 32 {
                    return Err(self.error("最多32个标记条件"));
                }
                let value = kw.remove("refSeries").unwrap_or(self.names["close"]);
                let text = match kw.remove("text") {
                    Some(n) => n,
                    None => self.string("Signal")?,
                };
                let color = match kw.remove("color") {
                    Some(n) => n,
                    None => self.string("#FFFFFF")?,
                };
                let background = match kw.remove("bgColor") {
                    Some(n) => n,
                    None => self.string("#00695C")?,
                };
                let below = kw
                    .remove("placement")
                    .map(|n| self.text_constant(n))
                    .transpose()?
                    .is_some_and(|s| s == "bottom");
                let font_size = kw
                    .remove("fontSize")
                    .map(|n| self.count(n, 1))
                    .transpose()?
                    .unwrap_or(11)
                    .clamp(8, 24) as u8;
                self.engine.labels.push(Label {
                    condition: a[0],
                    value,
                    text,
                    color,
                    background,
                    below,
                    font_size,
                });
                self.number(0)
            }
            "alertcondition" => {
                if self.engine.alerts.len() >= 32 {
                    return Err(self.error("最多32个信号条件"));
                }
                let title = kw
                    .remove("title")
                    .map(|n| self.text_constant(n))
                    .transpose()?
                    .unwrap_or_else(|| "Signal".into());
                self.engine.alerts.push((a[0], title));
                self.number(0)
            }
            _ => Err(self.error("不支持的函数")),
        }
    }
}

fn validate_types(engine: &ScriptEngine) -> Result<(), String> {
    let mut text: Vec<bool> = Vec::new();
    for node in &engine.nodes {
        let (kind, valid) = match &node.expr {
            Expr::Literal(Value::Text(_)) => (true, true),
            Expr::Literal(_) => (false, true),
            Expr::Field(name) => (name.starts_with("syminfo."), true),
            Expr::Unary(_, a)
            | Expr::Ema(a, _)
            | Expr::Sma(a, _)
            | Expr::Rsi(a, _)
            | Expr::Extreme(a, _, _)
            | Expr::Td(a, _) => (false, !text[*a]),
            Expr::Binary(_, a, b) => (false, !text[*a] && !text[*b]),
            Expr::Select(c, a, b) => (text[*a], !text[*c] && text[*a] == text[*b]),
            Expr::Lag(a, _) => (text[*a], true),
            Expr::ValueWhen(c, a, _, _) => (text[*a], !text[*c]),
        };
        if !valid {
            return Err("表达式类型错误：数字与字符串不能混用".into());
        }
        text.push(kind);
    }
    if engine.plots.iter().any(|p| text[p.value] || !text[p.color])
        || engine.fills.iter().any(|(_, _, c)| !text[*c])
        || engine.labels.iter().any(|p| {
            text[p.condition]
                || text[p.value]
                || !text[p.text]
                || !text[p.color]
                || !text[p.background]
        })
        || engine.alerts.iter().any(|(c, _)| text[*c])
    {
        return Err("绘图参数类型错误：价格/条件须为数字，颜色/文字须为字符串".into());
    }
    Ok(())
}
