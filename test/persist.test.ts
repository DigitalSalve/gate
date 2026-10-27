// change 8 contracts for the host's RateLimiter (advisory bucket): interval flush, reload, reset.
// The Meter half of these persistence tests lives with the meter package (packages/meter/test).
import { test } from "node:test";
import assert from "node:assert/strict";
import { RateLimiter } from "../src/index.ts";

class CountingStorage {
  map = new Map<string, unknown>();
  alarm: number | null = null;
  puts: string[] = [];
  deletes: string[] = [];
  setAlarms: number[] = [];
  gets = 0;
  failGets = 0;
  failPuts = 0;
  async get<T>(k: string) {
    this.gets++;
    if (this.failGets > 0) {
      this.failGets--;
      throw new Error("storage down");
    }
    return structuredClone(this.map.get(k)) as T | undefined;
  }
  async put(k: string, v: unknown) {
    if (this.failPuts > 0) {
      this.failPuts--;
      throw new Error("put failed");
    }
    this.puts.push(k);
    this.map.set(k, structuredClone(v));
  }
  async delete(k: string) {
    this.deletes.push(k);
    return this.map.delete(k);
  }
  async deleteAll() { this.map.clear(); }
  async list<T>(o?: { prefix?: string }) {
    return new Map([...this.map].filter(([k]) => !o?.prefix || k.startsWith(o.prefix))) as Map<string, T>;
  }
  async setAlarm(t: number) { this.setAlarms.push(t); this.alarm = t; }
  async getAlarm() { return this.alarm; }
  async deleteAlarm() { this.alarm = null; }
  putsOf(key: string) { return this.puts.filter((k) => k === key).length; }
  stateWrites() { return this.puts.filter((k) => k === "bucket" || k === "counters").length; }
}

// ---- RateLimiter ----

function limiterOn(storage: CountingStorage) {
  return new RateLimiter({ storage } as unknown as DurableObjectState, {});
}

async function limit(l: RateLimiter, qs = "rate=5&burst=10") {
  const res = await l.fetch(new Request(`http://limiter/limit/k?${qs}`));
  return { status: res.status, body: (await res.json()) as Record<string, unknown> };
}

async function status(l: RateLimiter) {
  return (await (await l.fetch(new Request("http://limiter/status/k"))).json()) as Record<string, unknown>;
}

test("RateLimiter: /limit never puts while rate/burst are unchanged; one alarm, one flush", async () => {
  const s = new CountingStorage();
  const l = limiterOn(s);
  const results = [];
  for (let i = 0; i < 12; i++) results.push((await limit(l)).status);
  assert.deepEqual(results.slice(0, 10), Array(10).fill(200));
  assert.equal(results[10], 429);
  assert.equal(s.puts.length, 0);
  assert.equal(s.setAlarms.length, 1);
  const st = await status(l);
  assert.equal(st.allowed, 10);
  assert.equal(st.denied, 2);
  await l.alarm();
  assert.equal(s.putsOf("bucket"), 1);
  await l.alarm();
  assert.equal(s.putsOf("bucket"), 1, "clean: no second flush");
});

test("RateLimiter: reload from storage; unflushed calls lost; rate change writes through", async () => {
  const s = new CountingStorage();
  const a = limiterOn(s);
  for (let i = 0; i < 3; i++) await limit(a);
  await a.alarm();
  await limit(a); // lost on "crash"
  const b = limiterOn(s);
  const st = await status(b);
  assert.equal(st.allowed, 3);
  assert.equal(st.rate, 5);
  await limit(b, "rate=2&burst=4");
  assert.equal(s.putsOf("bucket"), 2, "a real re-parameterisation is persisted at once");
  assert.equal((s.map.get("bucket") as { rate: number }).rate, 2);
});

test("RateLimiter: /status creates no state; DELETE clears memory and storage", async () => {
  const s = new CountingStorage();
  const l = limiterOn(s);
  await status(l);
  assert.equal(s.puts.length + s.setAlarms.length, 0);
  for (let i = 0; i < 4; i++) await limit(l);
  await l.alarm();
  const res = await l.fetch(new Request("http://limiter/limit/k", { method: "DELETE" }));
  assert.deepEqual(await res.json(), { reset: true });
  assert.equal(s.map.size, 0);
  assert.equal(s.alarm, null);
  const st = await status(l);
  assert.equal(st.allowed, 0);
  assert.equal(st.tokens, 10);
  await l.alarm();
  assert.equal(s.map.size, 0);
});

test("RateLimiter: junk rate/burst never writes through and never poisons the bucket", async () => {
  const s = new CountingStorage();
  const l = limiterOn(s);
  for (let i = 0; i < 5; i++) await limit(l, "rate=abc&burst=xyz");
  assert.equal(s.puts.length, 0);
  const st = await status(l);
  assert.equal(st.rate, 5);
  assert.equal(st.burst, 10);
});

test("RateLimiter: an alarm whose put fails stays dirty and re-arms", async () => {
  const s = new CountingStorage();
  const l = limiterOn(s);
  await limit(l);
  s.failPuts = 1;
  await l.alarm();
  assert.equal(s.setAlarms.length, 2);
  await l.alarm();
  assert.equal((s.map.get("bucket") as { allowed: number }).allowed, 1);
});
