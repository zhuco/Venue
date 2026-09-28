use super::*;
use venue_domain::PublicBar;

impl ScriptEngine {
    pub fn reset(&mut self) {
        for node in &mut self.nodes {
            node.history.clear();
            match &mut node.expr {
                Expr::Ema(_, state) => state.reset(),
                Expr::Sma(_, state) => state.reset(),
                Expr::Rsi(_, state) => state.reset(),
                Expr::ValueWhen(_, _, _, values) => values.clear(),
                Expr::Td(_, count) => *count = 0,
                _ => {}
            }
        }
    }
    pub fn update(&mut self, bar: &PublicBar) -> ScriptFrame {
        let mut values: Vec<Value> = Vec::with_capacity(self.nodes.len());
        for i in 0..self.nodes.len() {
            let (prior, rest) = self.nodes.split_at_mut(i);
            let value = match &mut rest[0].expr {
                Expr::Literal(v) => v.clone(),
                Expr::Field(field) if field == "syminfo.ticker" => {
                    Value::Text(bar.symbol.to_string())
                }
                Expr::Field(field) if field == "syminfo.prefix" => Value::Text(String::new()),
                Expr::Field(field) => Value::Number(match field.as_str() {
                    "open" => Some(bar.open.value()),
                    "high" => Some(bar.high.value()),
                    "low" => Some(bar.low.value()),
                    "close" => Some(bar.close.value()),
                    "volume" => match &bar.base_volume {
                        venue_domain::FieldState::Known(v) => Some(*v),
                        _ => None,
                    },
                    _ => None,
                }),
                Expr::Unary(op, a) => unary(op, &values[*a]),
                Expr::Binary(op, a, b) => binary(op, &values[*a], &values[*b]),
                Expr::Select(c, a, b) => values[if values[*c].truth() { *a } else { *b }].clone(),
                Expr::Lag(a, n) => {
                    if *n == 0 {
                        values[*a].clone()
                    } else {
                        prior[*a]
                            .history
                            .iter()
                            .rev()
                            .nth(*n - 1)
                            .cloned()
                            .unwrap_or(Value::Number(None))
                    }
                }
                Expr::Ema(a, state) => {
                    Value::Number(match values[*a].number().map(|v| state.update_value(v)) {
                        Some(Ok(value)) => value,
                        _ => {
                            state.reset();
                            None
                        }
                    })
                }
                Expr::Sma(a, state) => {
                    Value::Number(match values[*a].number().map(|v| state.update_value(v)) {
                        Some(Ok(value)) => value,
                        _ => {
                            state.reset();
                            None
                        }
                    })
                }
                Expr::Rsi(a, state) => {
                    Value::Number(match values[*a].number().map(|v| state.update_close(v)) {
                        Some(Ok(value)) => value,
                        _ => {
                            state.reset();
                            None
                        }
                    })
                }
                Expr::Extreme(a, n, maximum) => {
                    let window = std::iter::once(&values[*a])
                        .chain(prior[*a].history.iter().rev())
                        .take(*n)
                        .map(Value::number)
                        .collect::<Option<Vec<_>>>();
                    Value::Number(window.filter(|v| v.len() == *n).and_then(|v| {
                        if *maximum {
                            v.into_iter().max()
                        } else {
                            v.into_iter().min()
                        }
                    }))
                }
                Expr::ValueWhen(c, a, n, history) => {
                    if values[*c].truth() {
                        history.push_front(values[*a].clone());
                        history.truncate(*n + 1);
                    }
                    history.get(*n).cloned().unwrap_or(Value::Number(None))
                }
                Expr::Td(a, count) => {
                    let before = prior[*a]
                        .history
                        .iter()
                        .rev()
                        .nth(3)
                        .and_then(Value::number);
                    if let (Some(current), Some(before)) = (values[*a].number(), before) {
                        let direction = if current > before {
                            1
                        } else if current < before {
                            -1
                        } else {
                            0
                        };
                        *count = if direction == 0 {
                            0
                        } else if count.signum() == direction && count.abs() < 13 {
                            *count + direction
                        } else {
                            direction
                        };
                        Value::Number(Some(Decimal::from(*count)))
                    } else {
                        *count = 0;
                        Value::Number(None)
                    }
                }
            };
            values.push(value);
        }
        let frame = ScriptFrame {
            id: self.id,
            lines: self
                .plots
                .iter()
                .map(|p| ScriptLine {
                    value: values[p.value].number(),
                    color: values[p.color].text(),
                    width: p.width,
                    title: p.title.clone(),
                })
                .collect(),
            fills: self
                .fills
                .iter()
                .map(|(a, b, c)| ScriptFill {
                    first: *a,
                    second: *b,
                    color: values[*c].text(),
                })
                .collect(),
            labels: self
                .labels
                .iter()
                .filter(|p| values[p.condition].truth())
                .filter_map(|p| {
                    Some(ScriptLabel {
                        value: values[p.value].number()?,
                        text: values[p.text].text(),
                        color: values[p.color].text(),
                        background: values[p.background].text(),
                        below: p.below,
                        font_size: p.font_size,
                    })
                })
                .collect(),
            alerts: self
                .alerts
                .iter()
                .filter(|(c, _)| values[*c].truth())
                .map(|(_, title)| title.clone())
                .collect(),
        };
        for (node, value) in self.nodes.iter_mut().zip(values) {
            if node.keep > 0 {
                node.history.push_back(value);
                while node.history.len() > node.keep {
                    node.history.pop_front();
                }
            }
        }
        frame
    }
}

fn unary(op: &str, a: &Value) -> Value {
    if op == "not" || op == "!" {
        return Value::boolean(!a.truth());
    }
    Value::Number(a.number().and_then(|v| match op {
        "-" => Decimal::ZERO.checked_sub(v),
        "abs" => Some(v.abs()),
        _ => Some(v),
    }))
}
fn binary(op: &str, a: &Value, b: &Value) -> Value {
    if op == "&&" {
        return Value::boolean(a.truth() && b.truth());
    }
    if op == "||" {
        return Value::boolean(a.truth() || b.truth());
    }
    let (Some(a), Some(b)) = (a.number(), b.number()) else {
        return Value::Number(None);
    };
    match op {
        "==" => Value::boolean(a == b),
        "!=" => Value::boolean(a != b),
        ">" => Value::boolean(a > b),
        "<" => Value::boolean(a < b),
        ">=" => Value::boolean(a >= b),
        "<=" => Value::boolean(a <= b),
        _ => Value::Number(match op {
            "+" => a.checked_add(b),
            "-" => a.checked_sub(b),
            "*" => a.checked_mul(b),
            "/" => a.checked_div(b),
            "max" => Some(a.max(b)),
            "min" => Some(a.min(b)),
            _ => None,
        }),
    }
}
