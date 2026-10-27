// change 6 amendment contract meter-admit-wait-budget: /admit honours an optional max_wait_ms.
import { test } from "node:test";
import assert from "node:assert/strict";
import { Meter, WAIT_CAP_MS, waitBudget } from "../meter.ts";

class MemoryStorage {
  map = new Map<string, unknown>();
  alarm: number | null = null;
  async get<T>(k: string) { return structuredClone(this.map.get(k)) as T | undefined; }
  async put(k: string, v: unknown) { this.map.set(k, structuredClone(v)); }
  async delete(k: string) { return this.map.delete(k); }
  async deleteAll() { this.map.clear(); }
  async list<T>(o?: { prefix?: string }) {
    return new Map([...this.map].filter(([k]) => !o?.prefix || k.startsWith(o.prefix))) as Map<string, T>;
  }
  async setAlarm(t: number) { this.alarm = t; }
  async getAlarm() { return this.alarm; }
}

function meter() {
  return new Meter({ storage: new MemoryStorage() } as unknown as DurableObjectState);
}

async function admit(m: Meter, body: Record<string, unknown>) {
  const res = await m.fetch(new Request("http://meter/admit", { method: "POST", body: JSON.stringify(body) }));
  return (await res.json()) as Record<string, unknown>;
}

async function counters(m: Meter, now: number) {
  const res = await m.fetch(new Request(`http://meter/?now=${now}`));
  return ((await res.json()) as { counters: Record<string, number>; bucket: { tokens: number } });
}

test("waitBudget: absent or junk => full cap; clamped to [0, cap]", () => {
  assert.equal(waitBudget(undefined), WAIT_CAP_MS);
  assert.equal(waitBudget(null), WAIT_CAP_MS);
  assert.equal(waitBudget("abc"), WAIT_CAP_MS);
  assert.equal(waitBudget(Number.NaN), WAIT_CAP_MS);
  assert.equal(waitBudget(-5), 0);
  assert.equal(waitBudget(10_000), 10_000);
  assert.equal(waitBudget(90_000), WAIT_CAP_MS);
});

test("max_wait_ms below the wait rejects without holding a reservation", async () => {
  const m = meter();
  const now = 1_000_000;
  // rate 1/s, burst 1: first admit free, second waits 1 s.
  assert.equal((await admit(m, { now, default_rate: 1, default_burst: 1 })).wait_ms, 0);
  const r = await admit(m, { now, max_wait_ms: 500 });
  assert.equal(r.reject, true);
  assert.equal(r.retry_after_s, 1);
  const s = await counters(m, now);
  assert.equal(s.counters.rejected_cap, 1);
  assert.equal(s.counters.admitted, 1);
  assert.equal(s.bucket.tokens, 0, "the rejected admit held no reservation");
  const ok = await admit(m, { now, max_wait_ms: 1_000 });
  assert.equal(ok.wait_ms, 1000);
  assert.equal(ok.release_at, now + 1000);
});

test("max_wait_ms 0 still admits when a token is free; absent keeps the 30 s cap", async () => {
  const m = meter();
  const now = 2_000_000;
  assert.equal((await admit(m, { now, default_rate: 1, default_burst: 2, max_wait_ms: 0 })).wait_ms, 0);
  for (let i = 0; i < 31; i++) assert.equal((await admit(m, { now })).reject, undefined, `admit ${i}`); // last one waits exactly 30 s
  assert.equal((await admit(m, { now })).reject, true);
});
