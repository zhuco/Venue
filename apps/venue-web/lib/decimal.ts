// Display-only arithmetic. Original API decimal strings are never rewritten for submission.
export function sumDecimals(values: readonly string[], absolute = false): string {
  let total = 0n;
  let scale = 0;
  for (const value of values) {
    if (typeof value !== "string" || value.length > 128 || !/^-?\d+(?:\.\d+)?$/.test(value))
      return "—";
    const negative = value.startsWith("-");
    const [integer, fraction = ""] = (negative ? value.slice(1) : value).split(".");
    const nextScale = Math.max(scale, fraction.length);
    let units = BigInt(integer + fraction) * 10n ** BigInt(nextScale - fraction.length);
    if (negative && !absolute) units = -units;
    total = total * 10n ** BigInt(nextScale - scale) + units;
    scale = nextScale;
  }
  const negative = total < 0n;
  const digits = (negative ? -total : total).toString().padStart(scale + 1, "0");
  const result = scale ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}` : digits;
  return `${negative ? "-" : ""}${result}`;
}
