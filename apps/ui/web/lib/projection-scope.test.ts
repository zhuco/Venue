import assert from "node:assert/strict";
import test from "node:test";
import { relationInScope, scopedSnapshot } from "./projection-scope";

test("snapshot preserves authorized relations and never exposes another account's ledger", () => {
  const a = { mode: "LIVE", trading_account_id: "a" };
  const b = { mode: "LIVE", trading_account_id: "b" };
  const relation = { relation: { relation_id: "r-a", leader: a, follower: a } };
  const output = scopedSnapshot({ schema_version: 2, generated_ms: 1, connection: "LIVE",
    accounts: [a, b], strategies: [{ ...a, instance_id: "owned" }, { ...b, instance_id: "foreign" },
      { ...a, instance_id: "ambiguous" }, { ...b, instance_id: "ambiguous" }],
    copy_relations: [{ relation_id: "r-a" }, { relation_id: "r-b" }],
    ledger: [{ instance_id: "owned" }, { instance_id: "foreign" }, { instance_id: "ambiguous" }],
    markets: [], private_extra: "must not forward" }, [relation], ["a"]);
  assert.deepEqual(output.accounts, [a]);
  assert.deepEqual(output.copy_relations, [{ relation_id: "r-a" }]);
  assert.deepEqual(output.ledger, [{ instance_id: "owned" }]);
  assert.equal(output.private_extra, undefined);
  assert.equal(relationInScope({ relation: { leader: b, follower: a } }, ["a"]), false);
  assert.equal(relationInScope(null, ["a"]), false);
});
