import assert from "node:assert/strict";
import test from "node:test";
import { percentToRatio, ratioToPercent } from "./follow-sizing";
test("source percentages remain exact and bounded", () => {
  for (const [percent, ratio] of [["50", "0.5"], ["100", "1"], ["0.001", "0.00001"], ["12.34567890123456789", "0.1234567890123456789"]]) {
    assert.equal(percentToRatio(percent), ratio);
    assert.equal(ratioToPercent(ratio), percent);
  }
  for (const bad of ["0", "-1", "101", "1e2", "NaN", "", "0.0"]) assert.throws(() => percentToRatio(bad));
});
