// change 6 contracts two-level-admission + admin-per-caller-cell.
// Round trip: Worker -> fake METER namespace (records every call) -> fake upstream (global fetch).
import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import worker, { cellName, type Env } from "../src/index.ts";
import { Meter } from "../packages/meter/meter.ts";
import { callerIdentity } from "../src/util.ts";

const UPSTREAMS = JSON.stringify({
  tracker: { target: "http://tracker.test:3000", rate: 10, burst: 20, caller_rate: 4, caller_burst: 10, dedupe: true, store_bodies: true },
  nocb: { target: "http://nocb.test", rate: 3, caller_rate: 2, dedupe: true, store_bodies: false },
  secrets: { target: "http://secrets.test:80", rate: 5, burst: 20, dedupe: true, store_bodies: false },
});
const KEY = "tracker_api_0123456789secret";

interface Call {
  cell: string;
  path: string;
  method: string;
  body: Record<string, unknown> | null;
  search: string;
}
type Handler = (call: Call) => Record<string, unknown> | Error;

let calls: Call[];
let upstreamCalls: { url: string; method: string; at: number }[];
let handlers: Map<string, Handler>;
let upstreamStatus: number;
const realFetch = globalThis.fetch;

function fakeNamespace(): DurableObjectNamespace {
  return {
    idFromName: (name: string) => ({ toString: () => name, name }) as unknown as DurableObjectId,
    get: (id: DurableObjectId) => {
      const cell = (id as unknown as { name: string }).name;
      return {
        async fetch(req: Request) {
          const url = new URL(req.url);
          const text = req.method === "POST" ? await req.text() : "";
          const call: Call = { cell, path: url.pathname, method: req.method, body: text ? JSON.parse(text) : null, search: url.search };
          calls.push(call);
          const h = handlers.get(`${cell} ${url.pathname}`) ?? handlers.get(`* ${url.pathname}`);
          const out = h ? h(call) : defaultReply(call);
          if (out instanceof Error) throw out;
          return Response.json(out);
        },
      } as DurableObjectStub;
    },
  } as unknown as DurableObjectNamespace;
}

function defaultReply(call: Call): Record<string, unknown> {
  if (call.path === "/admit") return { release_at: Date.now(), wait_ms: 0, dedupe_wait: false };
  if (call.path === "/record") return { recorded: true };
  if (call.path === "/peek") return { miss: true };
  return { bucket: { cell: call.cell } };
}

function env(meter = fakeNamespace()): Env {
  return { LIMITER: fakeNamespace(), METER: meter, UPSTREAMS };
}

function ctx() {
  const pending: Promise<unknown>[] = [];
  return {
    waitUntil: (p: Promise<unknown>) => void pending.push(p),
    passThroughOnException() {},
    drain: () => Promise.all(pending),
  };
}

async function run(req: Request, e: Env = env()) {
  const c = ctx();
  const res = await worker.fetch(req, e, c as unknown as ExecutionContext);
  const text = await res.text();
  await c.drain();
  return { res, text };
}

async function trackerCell(headers: Record<string, string>) {
  return cellName("tracker", await callerIdentity(new Headers(headers)));
}

beforeEach(() => {
  calls = [];
  upstreamCalls = [];
  handlers = new Map();
  upstreamStatus = 200;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    upstreamCalls.push({ url: String(input), method: init?.method ?? "GET", at: Date.now() });
    return new Response(JSON.stringify({ ok: true }), { status: upstreamStatus, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
});
afterEach(() => {
  globalThis.fetch = realFetch;
});

test("keyed read on a per-caller upstream: caller admit, then upstream admit, forward, record both", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  const { res } = await run(new Request("http://gate/via/tracker/api/v1/x?y=1", { headers: { "x-api-key": KEY } }));
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-gate"), "gate");
  assert.equal(res.headers.get("x-gate-cell"), cell);
  assert.match(res.headers.get("x-gate-cell") ?? "", /^tracker@[0-9a-f]{16}$/);

  // A read has no fingerprint, so there is no /peek: caller cell first, then the shared cell.
  const admits = calls.filter((c) => c.path === "/admit");
  assert.deepEqual(calls.filter((c) => c.path !== "/record").map((c) => `${c.cell} ${c.path}`), [`${cell} /admit`, "tracker /admit"]);
  assert.equal(admits[0].body?.fp, null);
  assert.equal(admits[0].body?.default_rate, 4);
  assert.equal(admits[0].body?.default_burst, 10);
  assert.equal(admits[0].body?.path, "/api/v1/x");
  assert.equal(admits[0].body?.query, "?y=1");
  assert.equal(admits[1].body?.default_rate, 10);
  assert.equal(admits[1].body?.default_burst, 20);
  assert.equal(admits[1].body?.fp, null);
  assert.equal(typeof admits[1].body?.max_wait_ms, "number");
  assert.ok((admits[1].body!.max_wait_ms as number) <= 30_000 && (admits[1].body!.max_wait_ms as number) > 29_000);

  assert.equal(upstreamCalls.length, 1);
  assert.equal(upstreamCalls[0].url, "http://tracker.test:3000/api/v1/x?y=1");

  const records = calls.filter((c) => c.path === "/record");
  assert.deepEqual(records.map((c) => c.cell).sort(), ["tracker", cell].sort());
  const callerRec = records.find((c) => c.cell === cell)!.body!;
  assert.equal(callerRec.store_bodies, false);
  assert.equal(callerRec.status, 200);
  assert.equal("fp" in callerRec, false);
  assert.equal("body_b64" in callerRec, false);
  assert.equal("headers" in callerRec, false);
  const upRec = records.find((c) => c.cell === "tracker")!.body!;
  assert.equal(upRec.store_bodies, true);
  assert.equal(upRec.status, 200);
});

test("credential never appears in a cell name or response header", async () => {
  const { res } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  for (const c of calls) assert.equal(c.cell.includes(KEY), false);
  for (const [, v] of res.headers) assert.equal(v.includes(KEY), false);
});

test("keyed write: call order is upstream /peek -> caller /admit -> upstream /admit; fp and dedupe stay upstream", async () => {
  const cell = await trackerCell({ authorization: "Bearer t" });
  handlers.set("tracker /admit", () => ({ release_at: Date.now(), wait_ms: 0, dedupe_wait: false, dd_at: 123 }));
  const { res } = await run(
    new Request("http://gate/via/tracker/api/v1/issues/", { method: "POST", headers: { authorization: "Bearer t" }, body: '{"n":1}' }),
  );
  assert.equal(res.status, 200);
  assert.deepEqual(
    calls.filter((c) => c.path !== "/record").map((c) => `${c.cell} ${c.path}`),
    ["tracker /peek", `${cell} /admit`, "tracker /admit"],
  );
  const peek = calls.find((c) => c.cell === "tracker" && c.path === "/peek")!;
  const up = calls.find((c) => c.cell === "tracker" && c.path === "/admit")!;
  const caller = calls.find((c) => c.cell === cell && c.path === "/admit")!;
  assert.equal(typeof up.body?.fp, "string");
  assert.equal(peek.body?.fp, up.body?.fp);
  assert.equal(caller.body?.fp, null);
  const upRec = calls.find((c) => c.cell === "tracker" && c.path === "/record")!;
  assert.equal(upRec.body?.fp, up.body?.fp);
  assert.equal(upRec.body?.dd_at, 123);
  assert.equal(typeof upRec.body?.body_b64, "string");
  const callerRec = calls.find((c) => c.cell === cell && c.path === "/record")!;
  assert.deepEqual(Object.keys(callerRec.body!).sort(), ["caller", "retried", "status", "store_bodies", "upstream_error", "waited_ms"]);
  assert.equal(callerRec.body!.caller, true, "change 12: caller-cell calls carry caller: true");
  assert.equal(caller.body?.caller, true);
  // The shared cell never learns it is a caller cell, so it is never pruned.
  for (const c of calls.filter((c) => c.cell === "tracker")) assert.equal(c.body?.caller, undefined);
  // No meter call ever touches the caller cell with a fingerprint.
  assert.ok(calls.filter((c) => c.cell === cell).every((c) => c.body?.fp == null));
});

test("release time is the later of the two cells (shared cell admitted at the caller's release)", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  let callerAt = 0;
  let upAt = 0;
  handlers.set(`${cell} /admit`, () => {
    callerAt = Date.now();
    return { release_at: callerAt + 250, wait_ms: 250 };
  });
  handlers.set("tracker /admit", (call) => {
    upAt = Date.now();
    assert.ok((call.body!.max_wait_ms as number) <= 30_000 - 240, `budget ${call.body!.max_wait_ms}`);
    return { release_at: upAt + 20, wait_ms: 20 };
  });
  await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.equal(upstreamCalls.length, 1);
  assert.ok(upAt - callerAt >= 240, `shared cell asked ${upAt - callerAt} ms after the caller cell`);
  assert.ok(upstreamCalls[0].at - callerAt >= 260, `forwarded after ${upstreamCalls[0].at - callerAt} ms`);

  calls = [];
  upstreamCalls = [];
  handlers.set(`${cell} /admit`, () => {
    callerAt = Date.now();
    return { release_at: callerAt, wait_ms: 0 };
  });
  handlers.set("tracker /admit", () => ({ release_at: Date.now() + 250, wait_ms: 250 }));
  await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.ok(upstreamCalls[0].at - callerAt >= 240, `forwarded after ${upstreamCalls[0].at - callerAt} ms`);
});

test("upstream-cell reject after the caller cell admitted: 503 with the upstream Retry-After", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set("tracker /admit", () => ({ reject: true, retry_after_s: 7 }));
  const { res, text } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("retry-after"), "7");
  assert.equal(res.headers.get("x-gate-cell"), "tracker");
  assert.equal(JSON.parse(text).error, "rate_limited");
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), [`${cell} /admit`, "tracker /admit"]);
  assert.equal(upstreamCalls.length, 0);
});

test("replayable dedupe hit at /peek: replay, no token spent in the caller or the upstream cell", async () => {
  handlers.set("tracker /peek", () => ({ dedupe: { status: 201, headers: { "content-type": "application/json" }, body_b64: btoa('{"id":1}') } }));
  const { res, text } = await run(
    new Request("http://gate/via/tracker/a", { method: "POST", headers: { "x-api-key": KEY }, body: "x" }),
  );
  assert.equal(res.status, 201);
  assert.equal(res.headers.get("x-gate-dedupe"), "hit");
  assert.equal(res.headers.get("x-gate-cell"), "tracker");
  assert.equal(text, '{"id":1}');
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["tracker /peek"]);
  assert.equal(upstreamCalls.length, 0);
});

test("dedupe hit that lands between /peek and the upstream /admit still replays", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set("tracker /admit", () => ({ dedupe: { status: 201, headers: {}, body_b64: btoa("late") } }));
  const { res, text } = await run(
    new Request("http://gate/via/tracker/a", { method: "POST", headers: { "x-api-key": KEY }, body: "x" }),
  );
  assert.equal(res.status, 201);
  assert.equal(res.headers.get("x-gate-dedupe"), "hit");
  assert.equal(text, "late");
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["tracker /peek", `${cell} /admit`, "tracker /admit"]);
  assert.equal(upstreamCalls.length, 0);
});

test("peek pending: the duplicate is admitted, then waits for the original via dedupe_wait", async () => {
  let peeks = 0;
  handlers.set("tracker /peek", () => {
    peeks++;
    return peeks < 3 ? { pending: true } : { dedupe: { status: 200, headers: {}, body_b64: btoa("orig") } };
  });
  handlers.set("tracker /admit", () => ({ release_at: Date.now(), wait_ms: 0, dedupe_wait: true }));
  const { res, text } = await run(
    new Request("http://gate/via/tracker/a", { method: "POST", headers: { "x-api-key": KEY }, body: "x" }),
  );
  assert.equal(res.headers.get("x-gate-dedupe"), "hit");
  assert.equal(text, "orig");
  assert.equal(upstreamCalls.length, 0);
  // The duplicate never owns the placeholder, so it records nothing that could clobber it.
  assert.equal(calls.filter((c) => c.path === "/record").length, 0);
});

test("caller-cell reject on a write: 503 with the caller's Retry-After, upstream cell only peeked", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set(`${cell} /admit`, () => ({ reject: true, retry_after_s: 4 }));
  const { res, text } = await run(
    new Request("http://gate/via/tracker/a", { method: "POST", headers: { "x-api-key": KEY }, body: "w" }),
  );
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("retry-after"), "4");
  assert.equal(res.headers.get("x-gate-cell"), cell);
  const body = JSON.parse(text);
  assert.equal(body.error, "rate_limited");
  assert.equal(body.retry_after_s, 4);
  assert.equal(upstreamCalls.length, 0);
  // No upstream /admit (no shared token, no placeholder) and no /record anywhere (nothing to release).
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["tracker /peek", `${cell} /admit`]);
});

test("caller-cell reject on a read never calls the upstream cell", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set(`${cell} /admit`, () => ({ reject: true, retry_after_s: 2 }));
  const { res } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.equal(res.status, 503);
  assert.deepEqual(calls.map((c) => c.cell), [cell]);
});

test("abort during the caller wait: caller cell records the abort, upstream cell never called", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set(`${cell} /admit`, () => ({ release_at: Date.now() + 5_000, wait_ms: 5_000 }));
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 30);
  const t0 = Date.now();
  const { res } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY }, signal: ac.signal }));
  assert.ok(Date.now() - t0 < 2_000);
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("x-gate-cell"), cell);
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), [`${cell} /admit`, `${cell} /record`]);
  assert.equal(calls[1].body?.aborted, true);
  assert.equal(upstreamCalls.length, 0);
});

test("upstream /peek throwing -> bypass, caller cell untouched", async () => {
  handlers.set("tracker /peek", () => new Error("meter down"));
  const { res } = await run(new Request("http://gate/via/tracker/a", { method: "POST", headers: { "x-api-key": KEY }, body: "x" }));
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-gate-meter"), "bypass");
  assert.equal(res.headers.get("x-gate-cell"), "tracker");
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["tracker /peek"]);
  assert.equal(upstreamCalls.length, 1);
});

test("upstream meter throwing after the caller cell admitted -> bypass, counted in the caller cell", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set("tracker /admit", () => new Error("meter down"));
  const { res } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-gate-meter"), "bypass");
  assert.equal(res.headers.get("x-gate-cell"), cell);
  assert.equal(upstreamCalls.length, 1);
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), [`${cell} /admit`, "tracker /admit", `${cell} /record`]);
  assert.equal(calls[2].body?.status, 200);
});

test("anonymous: upstream meter throwing -> bypass, as today", async () => {
  handlers.set("tracker /admit", () => new Error("meter down"));
  const { res } = await run(new Request("http://gate/via/tracker/a"));
  assert.equal(res.headers.get("x-gate-meter"), "bypass");
  assert.equal(res.headers.get("x-gate-cell"), "tracker");
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["tracker /admit"]);
});

test("abort during the shared-cell wait on the per-caller path: both cells record, placeholder released", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set("tracker /admit", () => ({ release_at: Date.now() + 5_000, wait_ms: 5_000, dedupe_wait: false, dd_at: 77 }));
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 30);
  const { res } = await run(
    new Request("http://gate/via/tracker/a", { method: "POST", headers: { "x-api-key": KEY }, body: "w", signal: ac.signal }),
  );
  assert.equal(res.status, 503);
  assert.equal(upstreamCalls.length, 0);
  const recs = calls.filter((c) => c.path === "/record");
  assert.deepEqual(recs.map((c) => c.cell).sort(), ["tracker", cell].sort());
  const up = recs.find((c) => c.cell === "tracker")!.body!;
  assert.equal(up.aborted, true);
  assert.equal(up.dd_at, 77);
  assert.equal(up.fp, calls.find((c) => c.cell === "tracker" && c.path === "/admit")!.body!.fp);
  assert.equal(recs.find((c) => c.cell === cell)!.body!.aborted, true);
});

test("caller meter throwing -> bypass", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  handlers.set(`${cell} /admit`, () => new Error("caller cell down"));
  const { res } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-gate-meter"), "bypass");
  assert.equal(res.headers.get("x-gate-cell"), cell);
  assert.equal(upstreamCalls.length, 1);
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), [`${cell} /admit`]);
});

test("anonymous request on a per-caller upstream: upstream cell only", async () => {
  const { res } = await run(new Request("http://gate/via/tracker/a"));
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-gate-cell"), "tracker");
  assert.deepEqual([...new Set(calls.map((c) => c.cell))], ["tracker"]);
});

test("anonymous write on a per-caller upstream: today's single-cell flow, no /peek, no max_wait_ms", async () => {
  const { res } = await run(new Request("http://gate/via/tracker/a", { method: "POST", body: "x" }));
  assert.equal(res.status, 200);
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["tracker /admit", "tracker /record"]);
  assert.deepEqual(Object.keys(calls[0].body!).sort(), ["default_burst", "default_rate", "fp", "method", "now", "path", "query"]);
});

test("transparent mode (X-Gate-Upstream) uses the same two levels", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  const { res } = await run(new Request("http://gate/api/v1/x", { headers: { "x-api-key": KEY, "x-gate-upstream": "tracker" } }));
  assert.equal(res.headers.get("x-gate-cell"), cell);
  assert.deepEqual(calls.filter((c) => c.path === "/admit").map((c) => c.cell), [cell, "tracker"]);
});

test("caller_burst absent -> caller cell seeded with burst 10", async () => {
  const cell = cellName("nocb", await callerIdentity(new Headers({ "x-api-key": KEY })));
  await run(new Request("http://gate/via/nocb/a", { headers: { "x-api-key": KEY } }));
  const caller = calls.find((c) => c.cell === cell && c.path === "/admit")!;
  assert.equal(caller.body?.default_rate, 2);
  assert.equal(caller.body?.default_burst, 10);
});

test("secrets (no caller fields): one shared cell, same admit/record payloads as before", async () => {
  const { res } = await run(
    new Request("http://gate/via/secrets/api/v3/secrets", { method: "POST", headers: { authorization: "Bearer s" }, body: "{}" }),
  );
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-gate-cell"), "secrets");
  assert.deepEqual(calls.map((c) => `${c.cell} ${c.path}`), ["secrets /admit", "secrets /record"]);
  const admit = calls[0].body!;
  assert.deepEqual(Object.keys(admit).sort(), ["default_burst", "default_rate", "fp", "method", "now", "path", "query"]);
  assert.equal(admit.default_rate, 5);
  assert.equal(admit.default_burst, 20);
  assert.equal(typeof admit.fp, "string");
  const rec = calls[1].body!;
  assert.equal(rec.store_bodies, false);
  assert.equal("body_b64" in rec, false);
  assert.equal(rec.fp, admit.fp);
});

test("retryable upstream status: both cells get the intermediate and the final record", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  upstreamStatus = 503;
  const { res } = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }));
  assert.equal(res.status, 503);
  assert.equal(upstreamCalls.length, 2);
  const recs = calls.filter((c) => c.path === "/record");
  assert.equal(recs.filter((c) => c.cell === "tracker").length, 2);
  assert.equal(recs.filter((c) => c.cell === cell).length, 2);
  assert.ok(recs.filter((c) => c.cell === cell).every((c) => c.body?.upstream_error === true));
});

// ---- admin-per-caller-cell ----

test("GET /meter/tracker addresses the upstream cell; /meter/tracker@<hash16> the caller cell", async () => {
  const cell = await trackerCell({ "x-api-key": KEY });
  let r = await run(new Request("http://gate/meter/tracker?rate=10&burst=20"));
  assert.equal(r.res.status, 200);
  assert.equal(calls.at(-1)!.cell, "tracker");
  assert.equal(calls.at(-1)!.search, "?rate=10&burst=20");
  r = await run(new Request(`http://gate/meter/${cell}`));
  assert.equal(r.res.status, 200);
  assert.equal(calls.at(-1)!.cell, cell);
  r = await run(new Request(`http://gate/meter/${cell}`, { method: "DELETE" }));
  assert.equal(calls.at(-1)!.method, "DELETE");
  assert.equal(calls.at(-1)!.cell, cell);
});

test("admin 404s: unknown upstream, malformed caller, caller form on a shared-cell upstream", async () => {
  let r = await run(new Request("http://gate/meter/nope@0123456789abcdef"));
  assert.equal(r.res.status, 404);
  assert.equal(JSON.parse(r.text).error, "unknown upstream");
  r = await run(new Request("http://gate/meter/nope"));
  assert.equal(JSON.parse(r.text).error, "unknown upstream");
  for (const bad of ["tracker@xyz", "tracker@0123456789ABCDEF", "tracker@", "secrets@0123456789abcdef"]) {
    r = await run(new Request(`http://gate/meter/${bad}`));
    assert.equal(r.res.status, 404, bad);
    assert.equal(JSON.parse(r.text).error, "unknown cell", bad);
  }
  assert.equal(calls.length, 0, "no Meter instance minted for a rejected admin path");
});

// ---- round trip against the real Meter DO (in-memory storage) ----

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
  async deleteAlarm() { this.alarm = null; }
}

function realNamespace() {
  const cells = new Map<string, Meter>();
  const turns = new Map<string, Promise<unknown>>();
  const ns = {
    idFromName: (name: string) => ({ name, toString: () => name }),
    get: (id: { name: string }) => {
      if (!cells.has(id.name)) cells.set(id.name, new Meter({ storage: new MemoryStorage() } as unknown as DurableObjectState));
      const m = cells.get(id.name)!;
      // A real DO runs one turn at a time; serialise here too, or concurrent read-modify-write
      // turns lose counter updates and a fairness assertion could pass by accident.
      return {
        fetch: (req: Request) => {
          const prev = turns.get(id.name) ?? Promise.resolve();
          const next = prev.then(() => m.fetch(req));
          turns.set(id.name, next.catch(() => undefined));
          return next;
        },
      };
    },
  };
  return { ns: ns as unknown as DurableObjectNamespace, cells };
}

test("real Meter: caller cell is created at 4/10, upstream at 10/20, both count the request", async () => {
  const { ns, cells } = realNamespace();
  const e = env(ns);
  const cell = await trackerCell({ "x-api-key": KEY });
  await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": KEY } }), e);
  assert.deepEqual([...cells.keys()].sort(), ["tracker", cell].sort());
  const up = JSON.parse((await run(new Request("http://gate/meter/tracker"), e)).text);
  const caller = JSON.parse((await run(new Request(`http://gate/meter/${cell}`), e)).text);
  assert.equal(up.bucket.rate, 10);
  assert.equal(up.bucket.burst, 20);
  assert.equal(caller.bucket.rate, 4);
  assert.equal(caller.bucket.burst, 10);
  assert.equal(up.counters.admitted, 1);
  assert.equal(caller.counters.admitted, 1);
  assert.equal(caller.counters.forwarded, 1);
  assert.equal(caller.dedupe_entries, 0);
});

test("real Meter: one caller exhausting its cell is paced while another caller is not", async () => {
  const { ns } = realNamespace();
  const e = env(ns);
  const hogCell = await trackerCell({ "x-api-key": "hog" });
  // First touch creates the hog's cell at 4/10; then shrink it so the next reservation lands past
  // the 30 s wait cap (0.01 tokens/s, burst 1: one token left, then ~100 s per token).
  await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "hog" } }), e);
  await run(new Request(`http://gate/meter/${hogCell}?rate=0.01&burst=1`), e);
  const last = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "hog" } }), e);
  assert.equal(last.res.status, 200);
  const hog = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "hog" } }), e);
  assert.equal(hog.res.status, 503);
  assert.equal(hog.res.headers.get("x-gate-cell"), hogCell);
  const other = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "other" } }), e);
  assert.equal(other.res.status, 200);
  assert.equal(other.res.headers.get("x-gate-cell"), await trackerCell({ "x-api-key": "other" }));
});

async function meterState(e: Env, cell: string) {
  return JSON.parse((await run(new Request(`http://gate/meter/${cell}`), e)).text);
}

test("real Meter: a replayed duplicate is counted deduped once and spends no token in either cell", async () => {
  const { ns, cells } = realNamespace();
  const e = env(ns);
  const cell = await trackerCell({ "x-api-key": KEY });
  const write = () => new Request("http://gate/via/tracker/api/v1/issues/", { method: "POST", headers: { "x-api-key": KEY }, body: '{"n":1}' });
  const first = await run(write(), e);
  assert.equal(first.res.status, 200);
  assert.equal(first.res.headers.get("x-gate-dedupe"), null);
  const second = await run(write(), e);
  assert.equal(second.res.status, 200);
  assert.equal(second.res.headers.get("x-gate-dedupe"), "hit");
  assert.equal(second.text, first.text);
  assert.equal(upstreamCalls.length, 1);
  const up = await meterState(e, "tracker");
  const caller = await meterState(e, cell);
  assert.equal(up.counters.admitted, 1);
  assert.equal(up.counters.deduped, 1);
  assert.equal(up.counters.forwarded, 1);
  assert.equal(caller.counters.admitted, 1);
  assert.equal(caller.counters.deduped, 0);
  assert.equal(caller.dedupe_entries, 0);
  assert.ok(cells.has(cell));
});

test("real Meter: a caller-level reject leaves the upstream cell untouched", async () => {
  const { ns } = realNamespace();
  const e = env(ns);
  const hogCell = await trackerCell({ "x-api-key": "hog" });
  await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "hog" } }), e);
  await run(new Request(`http://gate/meter/${hogCell}?rate=0.01&burst=1`), e);
  await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "hog" } }), e);
  const before = await meterState(e, "tracker");
  const w = await run(new Request("http://gate/via/tracker/w", { method: "POST", headers: { "x-api-key": "hog" }, body: "z" }), e);
  assert.equal(w.res.status, 503);
  assert.equal(w.res.headers.get("x-gate-cell"), hogCell);
  const after = await meterState(e, "tracker");
  assert.deepEqual(after.counters, before.counters, "no admitted/dropped_abort/rejected_cap movement upstream");
  assert.equal(after.dedupe_entries, before.dedupe_entries, "no placeholder minted");
  assert.equal(after.bucket.tokens >= before.bucket.tokens, true, "no shared token spent");
  assert.equal((await meterState(e, hogCell)).counters.rejected_cap, 1);
});

test("real Meter: a flooding key reserves shared tokens only at its own pace; another caller is not queued", async () => {
  const { ns } = realNamespace();
  const e = env(ns);
  const ac = new AbortController();
  // 40 concurrent requests from one key: the caller cell (4/s, burst 10) lets 10 through now and
  // queues the rest for up to ~7.5 s. Under the old order all 40 reserved a shared token at once.
  const flood = Array.from({ length: 40 }, () =>
    run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "hog" }, signal: ac.signal }), e),
  );
  const deadline = Date.now() + 2_000;
  while (upstreamCalls.length < 10 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  assert.equal(upstreamCalls.length, 10);
  const mid = await meterState(e, "tracker");
  assert.ok(mid.counters.admitted <= 11, `shared cell admitted ${mid.counters.admitted} of the flood`);
  assert.ok(mid.bucket.tokens >= 9, `shared tokens left ${mid.bucket.tokens}`);

  const t0 = Date.now();
  const other = await run(new Request("http://gate/via/tracker/a", { headers: { "x-api-key": "other" } }), e);
  assert.equal(other.res.status, 200);
  assert.ok(Date.now() - t0 < 200, `other caller waited ${Date.now() - t0} ms`);

  ac.abort();
  const results = await Promise.all(flood);
  assert.equal(results.filter((r) => r.res.status === 200).length >= 10, true);
  const end = await meterState(e, "tracker");
  assert.equal(end.counters.dropped_abort, 0, "aborts during the caller wait never reach the shared cell");
  assert.ok((await meterState(e, await trackerCell({ "x-api-key": "hog" }))).counters.dropped_abort >= 25);
});
