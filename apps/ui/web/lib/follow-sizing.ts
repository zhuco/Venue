/** Decimal shifts keep the submitted percentage exact, including small fractions. */
export function percentToRatio(value: string): string {
  const input = value.trim();
  if (input.length > 28 || !/^\d+(?:\.\d+)?$/.test(input) || !/[1-9]/.test(input) || Number(input) > 100) throw new Error("主单比例须大于 0 且不超过 100%");
  const [whole, fraction = ""] = input.split(".");
  const digits = whole.padStart(3, "0");
  return `${digits.slice(0, -2).replace(/^0+(?=\d)/, "")}.${digits.slice(-2)}${fraction}`.replace(/0+$/, "").replace(/\.$/, "");
}
export function ratioToPercent(value: string): string {
  const [whole, fraction = ""] = value.split(".");
  const digits = fraction.padEnd(2, "0");
  return `${whole}${digits.slice(0, 2)}.${digits.slice(2)}`.replace(/^0+(?=\d)/, "").replace(/0+$/, "").replace(/\.$/, "");
}
