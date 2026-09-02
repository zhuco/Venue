import assert from "node:assert/strict";
import test from "node:test";
import { bindingAllowed } from "../app/api/control/relations/route";

const scoped = { venue: "Binance", mode: "LIVE", trading_account_id: "account-a", instance_id: "leader", symbol: "BTC/USDT" };
const foreign = { ...scoped, trading_account_id: "account-b", instance_id: "foreign-leader" };

test("relation mutation rejects a leader or follower outside the server session account scope", () => {
  assert.equal(bindingAllowed(scoped, [{ binding: scoped }, { binding: foreign }], ["account-a"]), true);
  assert.equal(bindingAllowed(foreign, [{ binding: foreign }], ["account-a"]), false);
  assert.equal(bindingAllowed(scoped, [{ binding: foreign }], ["account-a"]), false);
});
