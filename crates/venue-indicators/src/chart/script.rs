//! Bounded, causal chart expressions. No execution, I/O or notification side effects.
mod parser;
mod runtime;
#[cfg(test)]
mod tests;

use super::{Ema, Rsi, Sma};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const MAX_SOURCE_BYTES: usize = 32_768;
pub const MAX_SCRIPTS: usize = 4;
pub const COMPATIBILITY: &str = "AIScript 子集：EMA/RSI 使用 Venue 预热；TD 为与前4根比较的连续计数（±13后重计），非认证 TD Countdown；一目云使用高低价中点并滞后读取先行线。保留源码 TD 正负号。高量K线 POC 不是逐价成交量 POC。alertcondition 仅生成本地图表信号。";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScriptSpec {
    pub id: u64,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptLine {
    pub value: Option<Decimal>,
    pub color: String,
    pub width: u8,
    pub title: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptFill {
    pub first: usize,
    pub second: usize,
    pub color: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptLabel {
    pub value: Decimal,
    pub text: String,
    pub color: String,
    pub background: String,
    pub below: bool,
    pub font_size: u8,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptFrame {
    pub id: u64,
    pub lines: Vec<ScriptLine>,
    pub fills: Vec<ScriptFill>,
    pub labels: Vec<ScriptLabel>,
    pub alerts: Vec<String>,
}

#[derive(Clone, Debug)]
enum Value {
    Number(Option<Decimal>),
    Text(String),
}
impl Value {
    fn number(&self) -> Option<Decimal> {
        if let Self::Number(v) = self { *v } else { None }
    }
    fn truth(&self) -> bool {
        self.number().is_some_and(|v| !v.is_zero())
    }
    fn text(&self) -> String {
        if let Self::Text(v) = self {
            v.clone()
        } else {
            String::new()
        }
    }
    fn boolean(v: bool) -> Self {
        Self::Number(Some(if v { Decimal::ONE } else { Decimal::ZERO }))
    }
}
#[derive(Clone, Debug)]
enum Expr {
    Literal(Value),
    Field(String),
    Unary(String, usize),
    Binary(String, usize, usize),
    Select(usize, usize, usize),
    Lag(usize, usize),
    Ema(usize, Ema),
    Sma(usize, Sma),
    Rsi(usize, Rsi),
    Extreme(usize, usize, bool),
    ValueWhen(usize, usize, usize, VecDeque<Value>),
    Td(usize, i32),
}
#[derive(Clone, Debug)]
struct Node {
    expr: Expr,
    history: VecDeque<Value>,
    keep: usize,
}
#[derive(Clone, Debug)]
struct Plot {
    value: usize,
    color: usize,
    width: u8,
    title: String,
}
#[derive(Clone, Debug)]
struct Label {
    condition: usize,
    value: usize,
    text: usize,
    color: usize,
    background: usize,
    below: bool,
    font_size: u8,
}
#[derive(Clone, Debug)]
pub struct ScriptEngine {
    id: u64,
    nodes: Vec<Node>,
    plots: Vec<Plot>,
    fills: Vec<(usize, usize, usize)>,
    labels: Vec<Label>,
    alerts: Vec<(usize, String)>,
}
impl ScriptEngine {
    pub fn compile(spec: &ScriptSpec) -> Result<Self, String> {
        parser::compile(spec)
    }
}
