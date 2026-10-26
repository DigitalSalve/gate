// change 12 contracts caller-cell-flag, idle-prune-alarm (owner ruling 2026-10-26: idle caller cells are
// pruned after 24 h; shared cells never). A counting fake storage proves what is deleted and armed.
import { test } from "node:test";
import assert from "node:assert/strict";
import { Meter, IDLE_PRUNE_MS } from "../src/meter.ts";
import { FLUSH_INTERVAL_MS } from "../src/persist.ts";

class Storage {
  map = new Map<string, unknown>();
  alarm: number | null = null;
  setAlarms: number[] = [];
  deleteAlls = 0;
  async get<T>(k: string) { return structuredClone(this.map.get(k)) as T | undefined; }
  async put(k: string, v: unknown) { this.map.set(k, structuredClone(v)); }
  async delete(k: string) { return this.map.delete(k); }
  async deleteAll() { this.deleteAlls++; this.map.clear(); }
  async list<T>(o?: { prefix?: string }) {
    return new Map([...this.map].filter(([k]) => !o?.prefix || k.startsWith(o.prefix))) as Map<string, T>;
  }
  async setAlarm(t: number) { this.setAlarms.push(t); this.alarm = t; }
  async getAlarm() { return this.alarm; }
  async deleteAlarm() { this.alarm = null; }
}

const meterOn = (s: Storage) => new Meter({ storage: s } as unknown as DurableObjectState);
const CALLER = { default_rate: 4, default_burst: 10, caller: true };

async function post(m: Meter, path: string, body: Record<string, unknown>) {
  const res = await m.fetch(new Request(`http://meter${path}`, { method: "POST", body: JSON.stringify(body) }));
  return (await res.json()) as Record<string, unknown>;
}
async function report(m: Meter) {
  return (await (await m.fetch(new Request("http://meter/"))).json()) as {
    bucket: { rate: number; burst: number };
    counters: Record<string, number>;
    idle: { caller: boolean; last_touched: number | null; prune_at: number | null };
  };
}

test("caller cell idle >= 24 h: the alarm deletes all storage, clears memory, re-arms nothing", async () => {
  const s = new Storage();
  const m = meterOn(s);
  await post(m, "/admit", CALLER);
  await post(m, "/record", { status: 200, caller: true });
  await m.alarm(); // flush: bucket, counters, cell meta
  assert.deepEqual((s.map.get("cell") as { caller: boolean }).caller, true, "cell meta flushed with the state");
  s.map.set("dd:stray", { at: 0, release_at: 0, done: true, replayable: false });
  m.lastTouched = Date.now() - IDLE_PRUNE_MS;
  const armed = s.setAlarms.length;
  s.alarm = null;
  await m.alarm();
  assert.equal(s.map.size, 0, "bucket, counters, cell meta and dd entries all deleted");
  assert.equal(s.alarm, null);
  assert.equal(s.setAlarms.length, armed, "no alarm re-armed: the object can evict");
  assert.equal(m.bucket, null);
  assert.equal(m.caller, false);
  assert.equal(m.lastTouched, null);
  assert.equal(m.counters.admitted, 0);
  assert.equal(m.persist.dirty, false);
});

test("a shared cell (never sent caller: true) is never pruned, however idle", async () => {
  const s = new Storage();
  const m = meterOn(s);
  await post(m, "/admit", { default_rate: 20, default_burst: 40 });
  await m.alarm();
  m.lastTouched = Date.now() - 10 * IDLE_PRUNE_MS;
  const armed = s.setAlarms.length;
  await m.alarm();
  assert.equal(s.deleteAlls, 0);
  assert.equal((s.map.get("bucket") as { rate: number }).rate, 20);
  assert.equal(s.setAlarms.length, armed, "a clean shared cell still holds no alarm (change 8)");
  assert.equal((await report(m)).idle.prune_at, null);
});

test("a touched caller cell is not pruned: the alarm sleeps until last_touched + 24 h", async () => {
  const s = new Storage();
  const m = meterOn(s);
  await post(m, "/admit", CALLER);
  await m.alarm();
  m.lastTouched = Date.now() - IDLE_PRUNE_MS + 60_000; // one minute short of the deadline
  await m.alarm();
  assert.equal(s.deleteAlls, 0);
  assert.ok(s.map.has("bucket"));
  assert.equal(s.setAlarms.at(-1), m.lastTouched + IDLE_PRUNE_MS);
  // A fresh touch pushes the deadline out again.
  await post(m, "/record", { status: 200, caller: true });
  await m.alarm();
  await m.alarm();
  assert.equal(s.deleteAlls, 0);
  assert.ok(s.setAlarms.at(-1)! >= Date.now() + IDLE_PRUNE_MS - 1_000);
});

test("idle re-arm never spins every 10 s; a busy caller cell costs one setAlarm per interval", async () => {
  const s = new Storage();
  const m = meterOn(s);
  for (let i = 0; i < 20; i++) await post(m, "/admit", CALLER);
  assert.equal(s.setAlarms.length, 1, "one alarm for the busy interval");
  await m.alarm(); // flush; armed one interval out in case traffic continues
  assert.equal(s.setAlarms.length, 2);
  assert.ok(s.setAlarms[1] <= Date.now() + FLUSH_INTERVAL_MS);
  for (let i = 0; i < 20; i++) await post(m, "/admit", CALLER);
  assert.equal(s.setAlarms.length, 2, "touches inside the interval find the alarm already due");
  await m.alarm(); // flush the second interval
  await m.alarm(); // clean: arm the prune deadline, not another 10 s
  const deadline = s.setAlarms.at(-1)!;
  assert.equal(deadline, m.lastTouched! + IDLE_PRUNE_MS);
  for (let i = 0; i < 5; i++) await m.alarm(); // stray wakes while clean
  assert.ok(s.setAlarms.slice(-5).every((t) => t === deadline), "every clean re-arm is the 24 h deadline");
  assert.ok(s.setAlarms.every((t, i) => i < 3 || t === deadline), "no 10 s loop once the cell is clean");
});

test("cell meta survives a reload: a fresh instance with a stale caller cell prunes on its alarm", async () => {
  const s = new Storage();
  const a = meterOn(s);
  await post(a, "/admit", CALLER);
  await a.alarm();
  const meta = s.map.get("cell") as { caller: boolean; last_touched: number };
  assert.equal(meta.caller, true);
  assert.equal(typeof meta.last_touched, "number");
  s.map.set("cell", { caller: true, last_touched: Date.now() - IDLE_PRUNE_MS - 1 });
  const b = meterOn(s);
  await b.alarm();
  assert.equal(s.map.size, 0);
  assert.equal(s.alarm, null);
});

test("a pruned cell re-seeds from the admit defaults (caller 4/10), like a fresh cell", async () => {
  const s = new Storage();
  const m = meterOn(s);
  await post(m, "/admit", CALLER);
  await m.fetch(new Request("http://meter/?rate=1&burst=2")); // operator tweak, gone with the prune
  await m.alarm();
  m.lastTouched = Date.now() - IDLE_PRUNE_MS;
  await m.alarm();
  assert.equal(s.map.size, 0);
  await post(m, "/admit", CALLER);
  const r = await report(m);
  assert.equal(r.bucket.rate, 4);
  assert.equal(r.bucket.burst, 10);
  assert.equal(r.counters.admitted, 1);
  assert.equal(r.idle.caller, true);
  assert.ok(r.idle.prune_at! >= Date.now() + IDLE_PRUNE_MS - 1_000);
  assert.ok(s.alarm !== null && s.alarm <= Date.now() + FLUSH_INTERVAL_MS, "dirty again: the 10 s flush alarm");
});

test("a dirty caller cell is never pruned, even with a stale last_touched", async () => {
  const s = new Storage();
  const m = meterOn(s);
  await post(m, "/admit", CALLER);
  m.lastTouched = Date.now() - IDLE_PRUNE_MS;
  await m.alarm();
  assert.equal(s.deleteAlls, 0);
  assert.ok(s.map.has("bucket"), "flushed instead");
});

test("a prune whose deleteAll fails keeps the cell prunable and retries on the next alarm", async () => {
  const s = new Storage();
  const m = meterOn(s);
  await post(m, "/admit", CALLER);
  await m.alarm();
  m.lastTouched = Date.now() - IDLE_PRUNE_MS;
  const real = s.deleteAll.bind(s);
  s.deleteAll = async () => { throw new Error("bucket down"); };
  await m.alarm();
  assert.ok(s.map.has("bucket"), "nothing deleted");
  assert.equal(m.caller, true);
  assert.ok(s.alarm !== null && s.alarm <= Date.now() + FLUSH_INTERVAL_MS, "re-armed to retry");
  assert.ok(s.alarm >= Date.now() + FLUSH_INTERVAL_MS - 1_000, "retried one interval out, not every second");
  s.deleteAll = real;
  await m.alarm();
  assert.equal(s.map.size, 0);
  assert.equal(s.alarm, null);
});
