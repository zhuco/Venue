import test from "node:test";
import assert from "node:assert/strict";
import { verificationFeedback } from "./customer-api";
test("HTTP success does not imply credential verification success", () => {
  assert.equal(verificationFeedback("verified").success, true);
  for (const state of ["permission_denied", "invalid_credentials", "mode_mismatch", "network_unavailable", "account_conflict", undefined, "unknown"]) {
    const result = verificationFeedback(state);
    assert.equal(result.success, false);
    assert.notEqual(result.message, "请求已处理。");
  }
});
