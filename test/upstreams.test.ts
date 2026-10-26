// change 6 contract upstream-config-per-caller: parseUpstreams and the shipped wrangler.jsonc table.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseUpstreams } from "../src/index.ts";

function shippedTable(): string {
  const text = readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8");
  const m = text.match(/"UPSTREAMS":\s*("(?:[^"\\]|\\.)*")/);
  assert.ok(m, "wrangler.jsonc has vars.UPSTREAMS");
  return JSON.parse(m[1]) as string;
}

test("caller fields are copied when they are positive numbers", () => {
  const t = parseUpstreams(JSON.stringify({ a: { target: "http://h:1", rate: 10, burst: 20, caller_rate: 4, caller_burst: 7 } }));
  assert.equal(t.a.caller_rate, 4);
  assert.equal(t.a.caller_burst, 7);
});

test("caller fields are dropped when absent, zero, negative or not a number", () => {
  for (const bad of [undefined, 0, -1, "4", null, true]) {
    const t = parseUpstreams(JSON.stringify({ a: { target: "http://h:1", caller_rate: bad, caller_burst: bad } }));
    assert.equal("caller_rate" in t.a, false, `caller_rate=${String(bad)}`);
    assert.equal("caller_burst" in t.a, false, `caller_burst=${String(bad)}`);
  }
});

test("caller_burst may be absent while caller_rate is set (the Worker defaults it to 10)", () => {
  const t = parseUpstreams(JSON.stringify({ a: { target: "http://h:1", caller_rate: 2 } }));
  assert.equal(t.a.caller_rate, 2);
  assert.equal(t.a.caller_burst, undefined);
});

test("shipped tracker entry is per-caller: rate 20 burst 40 (change 12 ruling), caller 4 / 10", () => {
  const t = parseUpstreams(shippedTable());
  assert.deepEqual(t.tracker, {
    target: "http://192.0.2.54:3000/",
    rate: 20,
    burst: 40,
    caller_rate: 4,
    caller_burst: 10,
    dedupe: true,
    store_bodies: true,
  });
});

test("shipped secrets entry is unchanged and has no caller fields", () => {
  const t = parseUpstreams(shippedTable());
  assert.deepEqual(t.secrets, {
    target: "http://192.0.2.50/",
    rate: 5,
    burst: 20,
    dedupe: true,
    store_bodies: false,
  });
  assert.deepEqual(Object.keys(t.secrets).sort(), ["burst", "dedupe", "rate", "store_bodies", "target"]);
});

test("invalid rows are still skipped", () => {
  const t = parseUpstreams(JSON.stringify({ a: { target: "file:///etc/passwd" }, b: { rate: 1 }, c: "x" }));
  assert.deepEqual(t, {});
});
