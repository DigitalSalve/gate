// change 6 contract cell-naming: cellName / parseCellName.
import { test } from "node:test";
import assert from "node:assert/strict";
import { cellName, parseCellName } from "../src/index.ts";
import { callerIdentity } from "../src/util.ts";

test("no identity -> the upstream name", () => {
  assert.equal(cellName("tracker", null), "tracker");
});

test("identity -> <upstream>@<first 16 hex>", async () => {
  const id = await callerIdentity(new Headers({ "x-api-key": "tracker_api_secret" }));
  assert.match(id, /^[0-9a-f]{64}$/);
  const name = cellName("tracker", id);
  assert.equal(name, `tracker@${id.slice(0, 16)}`);
  assert.match(name, /^tracker@[0-9a-f]{16}$/);
  assert.equal(name.includes("tracker_api_secret"), false);
});

test("different credentials -> different cells; same credential -> same cell", async () => {
  const a = cellName("tracker", await callerIdentity(new Headers({ "x-api-key": "a" })));
  const a2 = cellName("tracker", await callerIdentity(new Headers({ "x-api-key": "a" })));
  const b = cellName("tracker", await callerIdentity(new Headers({ "x-api-key": "b" })));
  assert.equal(a, a2);
  assert.notEqual(a, b);
});

test("parseCellName round-trips both forms", async () => {
  assert.deepEqual(parseCellName("tracker"), { upstream: "tracker", caller: null });
  const id = await callerIdentity(new Headers({ authorization: "Bearer x" }));
  assert.deepEqual(parseCellName(cellName("tracker", id)), { upstream: "tracker", caller: id.slice(0, 16) });
});

test("parseCellName rejects malformed caller parts", () => {
  for (const raw of [
    "tracker@",
    "tracker@abc",
    "tracker@0123456789abcdef0", // 17 hex
    "tracker@0123456789ABCDEF", // upper case
    "tracker@0123456789abcdeg",
    "tracker@0123456789abcdef@0123456789abcdef", // splits on the FIRST @
    "@0123456789abcdef",
    "",
  ]) {
    assert.equal(parseCellName(raw), null, raw);
  }
});
