// change 8 contracts actor-memory-state, load-on-first-touch, interval-flush-alarm,
// synchronous-dedupe-and-admin-writes, report-persistence-field.
// A counting fake storage proves which requests write durably (each write = an LTX object on the actor runtime).
import { test } from "node:test";
import assert from "node:assert/strict";
import { Meter, DEDUPE_TTL_MS } from "../meter.ts";
import { FLUSH_INTERVAL_MS } from "../persist.ts";

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

function meterOn(storage: CountingStorage, extra: Record<string, unknown> = {}) {
  return new Meter({ storage, ...extra } as unknown as DurableObjectState);
}

async function post(m: Meter, path: string, body: Record<string, unknown>) {
  const res = await m.fetch(new Request(`http://meter${path}`, { method: "POST", body: JSON.stringify(body) }));
  return (await res.json()) as Record<string, unknown>;
}

async function report(m: Meter, qs = "") {
  const res = await m.fetch(new Request(`http://meter/${qs}`));
  return (await res.json()) as {
    bucket: { tokens: number; rate: number; burst: number };
    counters: Record<string, number>;
    dedupe_entries: number;
    persistence: { interval_ms: number; dirty: boolean; last_flush: number | null };
  };
}

test("FLUSH_INTERVAL_MS sits inside the owner's 10-30 s range", () => {
  assert.ok(FLUSH_INTERVAL_MS >= 10_000 && FLUSH_INTERVAL_MS <= 30_000);
});

test("Meter: /admit and /record never put bucket or counters; one alarm per interval", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  const before = Date.now();
  for (let i = 0; i < 50; i++) {
    await post(m, "/admit", { default_rate: 1000, default_burst: 1000 });
    await post(m, "/record", { status: 200, waited_ms: 0 });
  }
  assert.equal(s.stateWrites(), 0, "no bucket/counters put on the request path");
  assert.equal(s.setAlarms.length, 1, "one alarm armed for the whole interval");
  assert.ok(s.setAlarms[0] >= before + FLUSH_INTERVAL_MS && s.setAlarms[0] <= Date.now() + FLUSH_INTERVAL_MS);
  const r = await report(m);
  assert.equal(r.counters.admitted, 50, "the report reads memory");
  assert.equal(r.counters.forwarded, 50);
  assert.equal(r.persistence.dirty, true);
  assert.equal(r.persistence.last_flush, null);
});

test("Meter: the alarm flushes once when dirty, nothing when clean, and does not re-arm when idle", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  for (let i = 0; i < 10; i++) await post(m, "/admit", {});
  await m.alarm();
  assert.equal(s.putsOf("bucket"), 1);
  assert.equal(s.putsOf("counters"), 1);
  assert.equal(s.setAlarms.length, 1, "idle after the flush: no re-arm");
  assert.equal((s.map.get("counters") as { admitted: number }).admitted, 10);
  const r = await report(m);
  assert.equal(r.persistence.dirty, false);
  assert.ok(typeof r.persistence.last_flush === "number");

  await m.alarm(); // a stray alarm on a clean object writes nothing
  assert.equal(s.stateWrites(), 2);

  for (let i = 0; i < 10; i++) await post(m, "/admit", {});
  assert.equal(s.setAlarms.length, 2, "the next interval arms once more");
  await m.alarm();
  assert.equal(s.putsOf("counters"), 2, "one flush per dirty interval");
  assert.equal((s.map.get("counters") as { admitted: number }).admitted, 20);
});

test("Meter: dedupe entries are put synchronously; the purge shares the flush alarm", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  const a = await post(m, "/admit", { fp: "abc" });
  assert.ok(s.map.has("dd:abc"), "placeholder durable before /admit answers");
  assert.deepEqual(s.puts, ["dd:abc"], "the placeholder is the only put");
  await post(m, "/record", { fp: "abc", dd_at: a.dd_at, status: 201, store_bodies: true, body_b64: "eA==" });
  assert.equal((s.map.get("dd:abc") as { done: boolean }).done, true, "finalised before /record answers");
  assert.deepEqual(s.puts, ["dd:abc", "dd:abc"]);
  assert.equal(s.setAlarms.length, 1, "dedupe purge does not schedule a second alarm");

  await m.alarm();
  assert.ok(s.map.has("dd:abc"), "a live entry survives the alarm");
  assert.equal(s.setAlarms.length, 2, "re-armed once for the remaining dedupe entry");
  const ddAt = (s.map.get("dd:abc") as { at: number }).at;
  assert.ok(s.setAlarms[1] >= ddAt + DEDUPE_TTL_MS, "armed for the entry's expiry, not every interval");
  assert.ok(s.setAlarms[1] <= ddAt + DEDUPE_TTL_MS + 1_000);

  // An entry from a previous instance, past the window: the alarm deletes it, keeps the live one.
  s.map.set("dd:old", { at: Date.now() - DEDUPE_TTL_MS - 1, release_at: 0, done: true, replayable: false });
  await m.alarm();
  assert.equal(s.map.has("dd:old"), false);
  assert.ok(s.map.has("dd:abc"));
  // Only expired entries left: deleted, nothing re-armed.
  const fresh = meterOn(s);
  s.map.delete("dd:abc");
  s.map.set("dd:gone", { at: Date.now() - DEDUPE_TTL_MS - 1, release_at: 0, done: true, replayable: false });
  const armed = s.setAlarms.length;
  await fresh.alarm();
  assert.equal(s.map.has("dd:gone"), false);
  assert.equal(s.setAlarms.length, armed, "idle: no re-arm");
});

test("Meter: an aborted /record deletes its placeholder synchronously", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  const a = await post(m, "/admit", { fp: "zz" });
  await post(m, "/record", { fp: "zz", dd_at: a.dd_at, aborted: true, status: 0 });
  assert.equal(s.map.has("dd:zz"), false);
  assert.equal(s.stateWrites(), 0);
});

test("Meter: a fresh instance reloads flushed state; unflushed changes are lost (at most one interval)", async () => {
  const s = new CountingStorage();
  const a = meterOn(s);
  for (let i = 0; i < 3; i++) await post(a, "/admit", { default_rate: 4, default_burst: 10 });
  await a.alarm();
  for (let i = 0; i < 2; i++) await post(a, "/admit", {}); // crash before the next flush

  const b = meterOn(s);
  const r = await report(b);
  assert.equal(r.counters.admitted, 3);
  assert.equal(r.bucket.rate, 4);
  assert.equal(r.bucket.burst, 10);
  assert.ok(r.bucket.tokens <= 7.01, "the reloaded bucket carries the spent tokens");
});

test("Meter: a stored bucket keeps its stored rate/burst over admit defaults on reload", async () => {
  const s = new CountingStorage();
  s.map.set("bucket", { tokens: 3, updated: Date.now(), rate: 7, burst: 3, backoff_until: 0 });
  const m = meterOn(s);
  await post(m, "/admit", { default_rate: 4, default_burst: 10 });
  const r = await report(m);
  assert.equal(r.bucket.rate, 7);
  assert.equal(r.bucket.burst, 3);
});

test("Meter: blockConcurrencyWhile loads in the constructor when the runtime has it", async () => {
  const s = new CountingStorage();
  s.map.set("counters", { admitted: 9, forwarded: 0, deduped: 0, dropped_abort: 0, rejected_cap: 0, upstream_errors: 0, retries: 0, waited_ms: [0, 0, 0, 0, 0, 0, 0] });
  let gated = 0;
  const m = meterOn(s, {
    blockConcurrencyWhile: async <T>(fn: () => Promise<T>) => {
      gated++;
      return fn();
    },
  });
  assert.equal(gated, 1, "load started inside the input gate");
  assert.ok(s.gets >= 2, "bucket and counters read before any event");
  const getsAfterLoad = s.gets;
  const r = await report(m);
  assert.equal(r.counters.admitted, 9);
  assert.equal(s.gets, getsAfterLoad, "no second load");
});

test("Meter: a failed load is retried on the next event, never poisons the instance", async () => {
  const s = new CountingStorage();
  s.failGets = 1;
  const m = meterOn(s);
  await assert.rejects(m.fetch(new Request("http://meter/")));
  const r = await report(m);
  assert.equal(r.counters.admitted, 0);
});

test("Meter: GET ?rate=&burst= persists immediately; the report reads in-memory counters", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  for (let i = 0; i < 4; i++) await post(m, "/admit", { default_rate: 4, default_burst: 10 });
  const r = await report(m, "?rate=10&burst=20");
  assert.equal(s.putsOf("bucket"), 1, "re-parameterisation written through");
  assert.equal((s.map.get("bucket") as { rate: number }).rate, 10);
  assert.equal(r.bucket.rate, 10);
  assert.equal(r.bucket.burst, 20);
  assert.equal(r.counters.admitted, 4);
  const plain = await report(m);
  assert.equal(s.putsOf("bucket"), 1, "a plain GET writes nothing");
  assert.equal(plain.bucket.rate, 10);
  // Reload: the re-parameterised bucket survives even before a flush.
  assert.equal((await report(meterOn(s))).bucket.rate, 10);
});

test("Meter: a plain GET on a never-touched cell creates no state", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  const r = await report(m);
  assert.equal(r.bucket.rate, 5);
  assert.equal(s.puts.length, 0);
  assert.equal(s.setAlarms.length, 0);
});

test("Meter: DELETE clears memory and storage", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  await post(m, "/admit", { fp: "q", default_rate: 2, default_burst: 2 });
  await m.alarm();
  for (let i = 0; i < 3; i++) await post(m, "/admit", {});
  const res = await m.fetch(new Request("http://meter/", { method: "DELETE" }));
  assert.deepEqual(await res.json(), { reset: true });
  assert.equal(s.map.size, 0, "storage emptied");
  assert.equal(s.alarm, null, "alarm removed");
  const r = await report(m);
  assert.equal(r.counters.admitted, 0, "memory cleared");
  assert.equal(r.bucket.rate, 5, "bucket back to a fresh default");
  assert.equal(r.persistence.dirty, false);
  await m.alarm();
  assert.equal(s.map.size, 0, "a late alarm writes nothing back");
  // A new admit after DELETE seeds from its own defaults again.
  await post(m, "/admit", { default_rate: 4, default_burst: 10 });
  assert.equal((await report(m)).bucket.rate, 4);
});

test("Meter: a failed flush stays dirty and re-arms", async () => {
  const s = new CountingStorage();
  const m = meterOn(s);
  await post(m, "/admit", {});
  s.failPuts = 1;
  await m.alarm();
  assert.equal(s.setAlarms.length, 2, "re-armed after the failure");
  assert.equal((await report(m)).persistence.dirty, true);
  await m.alarm();
  assert.equal((s.map.get("counters") as { admitted: number }).admitted, 1);
});

// ---- review fixes (2026-10-26) ----

test("Meter: /record on a fresh instance never seeds a default bucket (owner rate survives restart)", async () => {
  const s = new CountingStorage();
  const a = meterOn(s);
  await post(a, "/admit", { default_rate: 4, default_burst: 10 }); // caller cell, memory only
  const b = meterOn(s); // restart before the first flush
  await post(b, "/record", { status: 503, upstream_error: true });
  await b.alarm();
  assert.equal(s.map.has("bucket"), false, "no 5/10 bucket persisted");
  assert.equal((s.map.get("counters") as { forwarded: number }).forwarded, 1, "counters still move");
  await post(b, "/admit", { default_rate: 4, default_burst: 10 });
  assert.equal((await report(b)).bucket.rate, 4);
});

test("Meter: a flush is one storage transaction when the runtime offers transaction()", async () => {
  const s = new CountingStorage() as CountingStorage & { transaction?: unknown; txns: number };
  s.txns = 0;
  s.transaction = async (fn: (t: unknown) => Promise<unknown>) => {
    s.txns++;
    return fn({ get: (k: string) => s.get(k), put: (k: string, v: unknown) => s.put(k, v), delete: (k: string) => s.delete(k) });
  };
  s.map.set("dd:old", { at: Date.now() - DEDUPE_TTL_MS - 1, release_at: 0, done: true, replayable: false });
  const m = meterOn(s);
  for (let i = 0; i < 5; i++) await post(m, "/admit", {});
  await m.alarm();
  assert.equal(s.txns, 1, "bucket + counters + expired delete in one transaction");
  assert.equal(s.putsOf("counters"), 1);
  assert.equal(s.map.has("dd:old"), false);
});

test("Meter: an alarm that throws keeps dirty state armed", async () => {
  const s = new CountingStorage() as CountingStorage & { list: unknown };
  const m = meterOn(s);
  await post(m, "/admit", {});
  const realList = s.list.bind(s);
  s.list = async () => { throw new Error("storage blip"); };
  await m.alarm();
  assert.equal(s.setAlarms.length, 2, "re-armed after the failure");
  s.list = realList;
  await m.alarm();
  assert.equal((s.map.get("counters") as { admitted: number }).admitted, 1);
});

test("Meter: the purge never deletes a placeholder re-minted during its list() await", async () => {
  const s = new CountingStorage() as CountingStorage & { list: unknown };
  const m = meterOn(s);
  s.map.set("dd:X", { at: Date.now() - DEDUPE_TTL_MS - 1, release_at: 0, done: false, replayable: false });
  const realList = s.list.bind(s);
  s.list = async (o?: { prefix?: string }) => {
    const snapshot = await realList(o);
    await post(m, "/admit", { fp: "X" }); // expired entry => a new placeholder for the same fp
    return snapshot;
  };
  await m.alarm();
  assert.ok(s.map.has("dd:X"), "the new placeholder survives");
  assert.equal((s.map.get("dd:X") as { done: boolean; at: number }).done, false);
  assert.ok((s.map.get("dd:X") as { at: number }).at > Date.now() - DEDUPE_TTL_MS);
});

test("Meter: a reloaded instance with a stored alarm does not arm a second one", async () => {
  const s = new CountingStorage();
  s.alarm = Date.now() + 5_000;
  const m = meterOn(s);
  await post(m, "/admit", {});
  assert.equal(s.setAlarms.length, 0);
});
