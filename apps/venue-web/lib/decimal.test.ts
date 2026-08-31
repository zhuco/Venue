import assert from "node:assert/strict";
import test from "node:test";
import { sumDecimals } from "./decimal";

test("display totals preserve exact large and tiny decimal values", () => {
  assert.equal(sumDecimals(["9007199254740993.01", "0.00000000000000000001"]), "9007199254740993.01000000000000000001");
  assert.equal(sumDecimals(["0.1", "0.2"]), "0.3");
  assert.equal(sumDecimals(["-5.00", "2.125"]), "-2.875");
  assert.equal(sumDecimals(["-5.00", "2.125"], true), "7.125");
  assert.equal(sumDecimals(["-0.00"]), "0.00");
  assert.equal(sumDecimals([]), "0");
});

test("malformed or unbounded decimals never become a misleading total", () => {
  for (const value of ["NaN", "Infinity", "1e20", "", "1,000", "9".repeat(129)])
    assert.equal(sumDecimals([value]), "—");
});
