// gate: estate-wide traffic governor on the actor runtime (self-hosted actors, the gate host).
//
// Doctrine: hold, never drop. Agents wait; the upstream sees a steady rate.
//
//   GET    /health, /                          -> ok
//   GET    /limit/<key>?cost=1&rate=10&burst=20 -> 200 {allowed:true,...} | 429 {allowed:false,retry_after_ms}
//   GET    /status/<key>                        -> 200 bucket state
//   DELETE /limit/<key>                         -> 200 {reset:true}
//   ANY    /via/<upstream>/<path...>            -> metered forward to env.UPSTREAMS[upstream].target
//   GET    /meter/<upstream>?rate=&burst=       -> Meter bucket + counters (params re-parameterise)
//   DELETE /meter/<upstream>                    -> reset bucket, counters and dedupe table
//   GET|DELETE /meter/<upstream>@<hash16>       -> same, for one caller's cell (change 6)
//
// Per-caller fairness (change 6): an upstream with `caller_rate` is metered at two levels. The
// upstream cell paces the service as a whole; a second cell per caller identity
// (`<upstream>@<first 16 hex of callerIdentity>`) stops one runaway caller starving the rest.
// Admission is caller-first: /peek (dedupe replay, free) -> caller /admit + wait -> upstream /admit.
// Every /via response names the deciding cell in `X-Gate-Cell`.
//
// The RateLimiter DO (advisory /limit, used by the change 7 agent hook) keeps its API.
// The Meter DO (src/meter.ts) owns the /via admission model.
// Both keep their state in actor memory and flush it once per interval (change 8, src/persist.ts).

import { Meter, WAIT_CAP_MS, DEDUPE_BODY_CAP, DEFAULT_BURST } from "../packages/meter/meter.ts";
import { buildForwardHeaders, buildResponseHeaders, headersToObject } from "./headers.ts";
import { callerIdentity, fingerprint, fromBase64, readBody, toBase64, sleep } from "./util.ts";
import { FLUSH_INTERVAL_MS, Once, Persistence, exclusive, startLoad } from "../packages/meter/persist.ts";

export { Meter };

const WRITE_METHODS: ReadonlySet<string> = new Set(["POST", "PUT", "PATCH", "DELETE"]);
const DEDUPE_MAX_REQUEST = 1024 * 1024; // > 1 MiB: forward, but no fingerprint and no replay
export const MAX_BUFFER = 8 * 1024 * 1024; // > 8 MiB of ACTUAL bytes: stream through, no dedupe, no retry
const RETRY_STATUSES: ReadonlySet<number> = new Set([429, 502, 503, 504]);
// 502/504 usually mean "the response was lost", not "it never ran". Replaying a non-idempotent
// write on those would create the duplicate the fingerprint exists to prevent, so writes retry
// only on the statuses that reliably mean the upstream never processed the request. All four
// still trip the backoff. Deviation from the design of record, recorded for owner confirmation.
const WRITE_RETRY_STATUSES: ReadonlySet<number> = new Set([429, 503]);
const RETRY_DELAY_MS = 1000;
const PEEK_POLL_START_MS = 50;
const PEEK_POLL_MAX_MS = 500;
const BODYLESS_STATUSES: ReadonlySet<number> = new Set([204, 205, 304]);

interface Bucket {
  tokens: number;
  updated: number;
  rate: number;
  burst: number;
  allowed: number;
  denied: number;
}

/**
 * Advisory token bucket for `/limit` (change 7 agent hook). change 8: the bucket lives in actor memory
 * and is flushed at most once per FLUSH_INTERVAL_MS by the object's alarm; only a real change of
 * rate/burst (or DELETE) writes synchronously. Storage key `bucket` is unchanged.
 */
export class RateLimiter {
  state: DurableObjectState;
  bucket: Bucket | null = null;
  readonly persist: Persistence;
  private readonly loader: Once;

  constructor(state: DurableObjectState, _env: unknown) {
    this.state = state;
    this.persist = new Persistence(state.storage);
    this.loader = new Once(async () => {
      const stored = await state.storage.get<Bucket>("bucket");
      await this.persist.loadAlarm();
      this.bucket = stored ?? null;
    });
    startLoad(state, this.loader);
  }

  async fetch(request: Request): Promise<Response> {
    await this.loader.run();
    const url = new URL(request.url);
    const now = Date.now();
    const cost = Math.max(1, Number(url.searchParams.get("cost") ?? 1));
    // A junk rate/burst (`rate=abc`) is ignored: NaN would poison the bucket and, because
    // NaN !== NaN, count as a change on every call and write through each time.
    const rateParam = Number(url.searchParams.get("rate") ?? 5);
    const burstParam = Number(url.searchParams.get("burst") ?? 10);
    const rateOk = Number.isFinite(rateParam) && rateParam > 0;
    const burstOk = Number.isFinite(burstParam) && burstParam > 0;
    const rate = rateOk ? rateParam : 5;
    const burst = burstOk ? burstParam : 10;
    const fresh = (): Bucket => ({ tokens: burst, updated: now, rate, burst, allowed: 0, denied: 0 });

    if (request.method === "DELETE") {
      await exclusive(this.state, async () => {
        this.bucket = null;
        this.persist.reset();
        await this.state.storage.deleteAll();
        if (typeof this.state.storage.deleteAlarm === "function") await this.state.storage.deleteAlarm();
      });
      if (this.persist.dirty) {
        this.persist.alarmAt = null;
        await this.persist.markDirty(Date.now());
      }
      return Response.json({ reset: true });
    }

    if (url.pathname.startsWith("/status")) {
      // A status read never creates or moves the bucket: compute on a copy, as before change 8.
      const b = this.bucket ? { ...this.bucket } : fresh();
      if (url.searchParams.has("rate") && rateOk) b.rate = rate;
      if (url.searchParams.has("burst") && burstOk) b.burst = burst;
      b.tokens = Math.min(b.burst, b.tokens + ((now - b.updated) / 1000) * b.rate);
      b.updated = now;
      return Response.json({ key: url.pathname.split("/")[2] ?? "", ...b, persistence: this.persist.report() });
    }

    // ---- synchronous mutation of the in-memory bucket ----
    const b = this.bucket ?? (this.bucket = fresh());
    // Re-parameterise only when the caller explicitly passes limits. The hook sends the same values
    // on every call, so only an actual change is written through immediately.
    let reparam = false;
    if (url.searchParams.has("rate") && rateOk && b.rate !== rate) {
      b.rate = rate;
      reparam = true;
    }
    if (url.searchParams.has("burst") && burstOk && b.burst !== burst) {
      b.burst = burst;
      reparam = true;
    }
    const elapsed = (now - b.updated) / 1000;
    b.tokens = Math.min(b.burst, b.tokens + elapsed * b.rate);
    b.updated = now;

    let res: Response;
    if (b.tokens >= cost) {
      b.tokens -= cost;
      b.allowed++;
      res = Response.json({ allowed: true, remaining: Math.floor(b.tokens), burst: b.burst, rate: b.rate });
    } else {
      b.denied++;
      const retry_after_ms = Math.ceil(((cost - b.tokens) / b.rate) * 1000);
      res = Response.json(
        { allowed: false, retry_after_ms, remaining: 0, burst: b.burst, rate: b.rate },
        { status: 429, headers: { "Retry-After": String(Math.ceil(retry_after_ms / 1000)) } },
      );
    }
    const snapshot = reparam ? { ...b } : null;
    // ---- end of the synchronous mutation ----

    await this.persist.markDirty(Date.now());
    if (snapshot) await this.state.storage.put("bucket", snapshot);
    return res;
  }

  /** Flush the bucket when dirty. Re-arms only if it went dirty again or anything failed. */
  async alarm(): Promise<void> {
    this.persist.alarmFired();
    try {
      await this.loader.run();
      const flushing = this.persist.dirty && this.bucket !== null;
      this.persist.dirty = false;
      if (!flushing || !this.bucket) return;
      const at = Date.now();
      try {
        await this.state.storage.put("bucket", { ...this.bucket });
        this.persist.lastFlush = at;
      } catch (err) {
        this.persist.dirty = true;
        throw err;
      }
    } catch (err) {
      console.error("gate: limiter alarm failed, retrying next interval", String(err));
      this.persist.dirty = true;
    } finally {
      if (this.persist.dirty) await this.persist.ensureAlarm(Date.now() + FLUSH_INTERVAL_MS);
    }
  }
}

export interface UpstreamConfig {
  target: string;
  rate?: number;
  burst?: number;
  /** Tokens/s for each caller's own cell. Present => the upstream is per-caller metered. */
  caller_rate?: number;
  /** Capacity of each caller's cell. Absent => DEFAULT_BURST. */
  caller_burst?: number;
  dedupe: boolean;
  store_bodies: boolean;
}

/** Headers whose presence gives a request a caller identity (same set callerIdentity digests). */
const IDENTITY_HEADER_NAMES = ["authorization", "x-api-key", "cookie"] as const;
const CALLER_HASH = /^[0-9a-f]{16}$/;

/**
 * Meter cell name. `identity` is the full callerIdentity() hex; only its first 16 hex characters
 * reach the name, so the credential (or even the full digest) never appears in a cell name,
 * an admin path or a response header.
 */
export function cellName(upstream: string, identity: string | null): string {
  return identity === null ? upstream : `${upstream}@${identity.slice(0, 16)}`;
}

/** Split `<upstream>` or `<upstream>@<hash16>`. Null when the caller part is malformed. */
export function parseCellName(raw: string): { upstream: string; caller: string | null } | null {
  const at = raw.indexOf("@");
  if (at === -1) return raw.length > 0 ? { upstream: raw, caller: null } : null;
  const upstream = raw.slice(0, at);
  const caller = raw.slice(at + 1);
  if (upstream.length === 0 || !CALLER_HASH.test(caller)) return null;
  return { upstream, caller };
}

function hasIdentity(headers: Headers): boolean {
  return IDENTITY_HEADER_NAMES.some((name) => headers.has(name));
}

function positive(value: unknown): number | undefined {
  return typeof value === "number" && value > 0 ? value : undefined;
}

export interface Env {
  LIMITER: DurableObjectNamespace;
  METER: DurableObjectNamespace;
  UPSTREAMS?: string;
}

// vars.UPSTREAMS is a JSON string. Parse once per distinct value (a deploy makes a new isolate).
let cachedRaw: string | null = null;
let cachedTable: Record<string, UpstreamConfig> = {};

export function parseUpstreams(raw: string | undefined): Record<string, UpstreamConfig> {
  const table: Record<string, UpstreamConfig> = {};
  if (!raw) return table;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    console.error("gate: UPSTREAMS is not valid JSON, no upstream is routable", String(err));
    return table;
  }
  if (!parsed || typeof parsed !== "object") return table;
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (!value || typeof value !== "object") continue;
    const row = value as Record<string, unknown>;
    if (typeof row.target !== "string") continue;
    let target: URL;
    try {
      target = new URL(row.target);
    } catch {
      console.error(`gate: upstream ${key} has an unparseable target, skipped`);
      continue;
    }
    // Only plain HTTP(S) upstreams. Never let a config typo turn into a file:// or data:// fetch.
    if (target.protocol !== "http:" && target.protocol !== "https:") {
      console.error(`gate: upstream ${key} has a non-http target, skipped`);
      continue;
    }
    const entry: UpstreamConfig = {
      target: target.toString(),
      rate: typeof row.rate === "number" ? row.rate : undefined,
      burst: typeof row.burst === "number" ? row.burst : undefined,
      dedupe: row.dedupe !== false,
      store_bodies: row.store_bodies === true,
    };
    // Per-caller fields are added only when valid, so an upstream without them (secrets) parses
    // to exactly the object it did before change 6.
    const callerRate = positive(row.caller_rate);
    const callerBurst = positive(row.caller_burst);
    if (callerRate !== undefined) entry.caller_rate = callerRate;
    if (callerBurst !== undefined) entry.caller_burst = callerBurst;
    table[key] = entry;
  }
  return table;
}

function upstreams(env: Env): Record<string, UpstreamConfig> {
  const raw = typeof env.UPSTREAMS === "string" ? env.UPSTREAMS : "";
  if (raw === cachedRaw) return cachedTable;
  cachedTable = parseUpstreams(raw);
  cachedRaw = raw;
  return cachedTable;
}

/**
 * Upstream URL for a /via request. The host always comes from config; only the path and query
 * come from the caller. Assigning `pathname` (rather than resolving a relative URL) means a
 * protocol-relative path like `/via/tracker//evil.example/x` cannot move the request off the
 * configured host.
 */
export function targetUrl(config: UpstreamConfig, rest: string, search: string): URL | null {
  const url = new URL(config.target);
  const basePath = url.pathname.replace(/\/+$/, "");
  url.pathname = `${basePath}${rest.startsWith("/") ? rest : `/${rest}`}`;
  url.search = search;
  url.hash = "";
  // Setting `pathname` cannot change the host, but the URL parser collapses dot segments across
  // the whole path, so a caller path full of `../` could climb out of a configured base path.
  // No upstream uses one today; assert it now so change 5 cannot introduce the hole quietly.
  if (basePath && !url.pathname.startsWith(`${basePath}/`) && url.pathname !== basePath) return null;
  return url;
}

/** Release an unread upstream body. `body` may be null and `cancel()` may reject; neither is fatal. */
async function cancelBody(res: Response): Promise<void> {
  try {
    await res.body?.cancel();
  } catch {
    /* already released */
  }
}

function gateJson(body: unknown, init: { status?: number; headers?: Record<string, string> } = {}): Response {
  return Response.json(body, {
    status: init.status ?? 200,
    headers: { "X-Gate": "gate", ...(init.headers ?? {}) },
  });
}

async function meterCall(stub: DurableObjectStub, path: string, body: unknown): Promise<Record<string, unknown>> {
  const res = await stub.fetch(
    new Request(`http://meter${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
  const parsed = await res.json().catch(() => null);
  return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
}

/** Name the cell that made the caller-level decision. Headers of every Response built here are mutable. */
function tagCell(res: Response, cell: string): Response {
  res.headers.set("X-Gate-Cell", cell);
  return res;
}

function replay(dedupe: Record<string, unknown>): Response {
  const status = Number(dedupe.status) || 200;
  const headers = buildResponseHeaders(new Headers((dedupe.headers as Record<string, string>) ?? {}));
  headers.set("X-Gate", "gate");
  headers.set("X-Gate-Dedupe", "hit");
  const b64 = typeof dedupe.body_b64 === "string" ? dedupe.body_b64 : "";
  const body = BODYLESS_STATUSES.has(status) || b64.length === 0 ? null : fromBase64(b64);
  return new Response(body, { status, headers });
}

async function handleVia(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  url: URL,
  key: string,
  rest: string,
): Promise<Response> {
  const config = upstreams(env)[key];
  if (!config) {
    return gateJson({ error: "unknown upstream", upstream: key }, { status: 404 });
  }

  const method = request.method.toUpperCase();
  const isWrite = WRITE_METHODS.has(method);

  // The body must be buffered to be fingerprinted and to survive the single upstream retry.
  // The cap is on bytes actually read, never on the caller's Content-Length: a chunked or
  // under-declared body would otherwise buffer without limit and exhaust the isolate.
  // Past the cap the body still forwards, it just streams: no dedupe, no retry.
  let bodyBuf: ArrayBuffer | null = null;
  let bodyStream: ReadableStream<Uint8Array> | null = null;
  if (isWrite) {
    const read = await readBody(request.body, MAX_BUFFER);
    bodyBuf = read.buffered;
    bodyStream = read.stream;
  }
  const streamBody = bodyStream !== null;

  let fp: string | null = null;
  if (config.dedupe && isWrite && bodyBuf !== null && bodyBuf.byteLength <= DEDUPE_MAX_REQUEST) {
    // The fingerprint is bound to the caller's credentials so a stored response can only be
    // replayed to the identity that produced it.
    fp = await fingerprint(method, rest, url.search, bodyBuf, await callerIdentity(request.headers));
  }

  // Per-caller cell: only for upstreams with caller_rate, and only when the request carries a
  // credential header. Anonymous requests are metered by the upstream cell alone.
  const perCaller = config.caller_rate !== undefined && hasIdentity(request.headers);
  const callerCell = perCaller ? cellName(key, await callerIdentity(request.headers)) : null;
  const callerStub = callerCell !== null ? env.METER.get(env.METER.idFromName(callerCell)) : null;
  // The cell whose answer ended or last gated the request. Until the caller cell has been
  // consulted (and whenever the upstream cell itself decides), that is the upstream cell.
  let decidingCell = key;

  const stub = env.METER.get(env.METER.idFromName(key));
  const startedAt = Date.now();
  const signal = request.signal ?? null;
  const recordCaller = (payload: Record<string, unknown>) => {
    if (!callerStub) return Promise.resolve();
    // The caller cell paces and counts only: no fingerprint, no dedupe entry, never a body.
    return recordWithRetry(callerStub, {
      status: payload.status,
      waited_ms: payload.waited_ms,
      retried: payload.retried,
      upstream_error: payload.upstream_error,
      aborted: payload.aborted,
      store_bodies: false,
      caller: true, // change 12: marks the cell prunable after 24 h idle
    });
  };
  const record = (payload: Record<string, unknown>) =>
    Promise.all([recordWithRetry(stub, { store_bodies: config.store_bodies, ...payload }), recordCaller(payload)]).then(
      () => undefined,
    );

  // Caller-first admission (change 6 amendment 2026-10-26). The shared cell is touched only after
  // the caller cell has admitted the request AND its caller-level wait is over, so a flooding key
  // can feed the shared cell no faster than its own caller_rate and never fills the shared queue.
  if (callerStub && callerCell !== null) {
    // (A) A completed, replayable duplicate is answered before any token is spent anywhere. The
    // Meter counts it as deduped inside /peek; nothing else is sent for this request.
    if (fp) {
      let peeked: Record<string, unknown>;
      try {
        peeked = await meterCall(stub, "/peek", { fp, now: startedAt });
      } catch (err) {
        return tagCell(await bypassMeter(request, config, key, rest, url, bodyBuf, bodyStream, err), decidingCell);
      }
      if (peeked.dedupe && typeof peeked.dedupe === "object") {
        return tagCell(replay(peeked.dedupe as Record<string, unknown>), decidingCell);
      }
      // `pending` is not acted on here: the upstream /admit below re-reads the entry and reports
      // dedupe_wait, which is authoritative because the original may land in between.
    }

    // (B) The caller cell decides first. A refusal here never reaches the shared cell: no shared
    // token, no dedupe placeholder, no upstream counter.
    decidingCell = callerCell;
    let callerAdmit: Record<string, unknown>;
    try {
      callerAdmit = await meterCall(callerStub, "/admit", {
        method,
        path: rest,
        query: url.search,
        fp: null,
        now: startedAt,
        default_rate: config.caller_rate,
        default_burst: config.caller_burst ?? DEFAULT_BURST,
        caller: true, // change 12: only caller cells are ever pruned; shared cells never get this flag
      });
    } catch (err) {
      return tagCell(await bypassMeter(request, config, callerCell, rest, url, bodyBuf, bodyStream, err), decidingCell);
    }
    if (callerAdmit.reject === true) {
      return tagCell(rateLimited(key, callerAdmit.retry_after_s), decidingCell);
    }
    await sleep((Number(callerAdmit.release_at) || Date.now()) - Date.now(), signal);
    if (signal?.aborted) {
      ctx.waitUntil(recordCaller({ status: 0, aborted: true, waited_ms: Date.now() - startedAt }));
      return tagCell(gateJson({ aborted: true, upstream: key }, { status: 503 }), decidingCell);
    }
  }

  // (C) The shared cell. On the per-caller path this runs at the caller's release time, so the
  // release below is max(caller, upstream), and the shared cell enforces only what is left of the
  // one 30 s budget (max_wait_ms). The single-cell flow sends exactly what it always has.
  const admittedAt = Date.now();
  const upstreamAdmit: Record<string, unknown> = {
    method,
    path: rest,
    query: url.search,
    fp,
    now: callerStub ? admittedAt : startedAt,
    default_rate: config.rate,
    default_burst: config.burst,
  };
  if (callerStub) upstreamAdmit.max_wait_ms = Math.max(0, WAIT_CAP_MS - (admittedAt - startedAt));

  let admit: Record<string, unknown>;
  try {
    admit = await meterCall(stub, "/admit", upstreamAdmit);
  } catch (err) {
    const bypassed = await bypassMeter(request, config, key, rest, url, bodyBuf, bodyStream, err);
    // On the per-caller path the caller cell admitted this request and it was forwarded: count it
    // there and name it, as for any other forward that passed the caller cell.
    if (callerStub) ctx.waitUntil(recordCaller({ status: bypassed.status, waited_ms: Date.now() - startedAt }));
    return tagCell(bypassed, decidingCell);
  }

  if (admit.reject === true) {
    // The caller token spent above is not refunded: it charges only this caller.
    return tagCell(rateLimited(key, admit.retry_after_s), key);
  }

  if (admit.dedupe && typeof admit.dedupe === "object") {
    return tagCell(replay(admit.dedupe as Record<string, unknown>), key);
  }

  // Only the request that created the dedupe placeholder may finalise or delete it. A duplicate
  // admitted alongside an in-flight original shares the same key and must never clobber its slot.
  const ownedFp = admit.dedupe_wait === true ? null : fp;
  const ddAt = typeof admit.dd_at === "number" ? admit.dd_at : undefined;

  const releaseAt = Number(admit.release_at) || Date.now();
  await sleep(releaseAt - Date.now(), signal);

  const abandoned = () => {
    ctx.waitUntil(record({ fp: ownedFp, dd_at: ddAt, status: 0, aborted: true, waited_ms: Date.now() - startedAt }));
    return tagCell(gateJson({ aborted: true, upstream: key }, { status: 503 }), decidingCell);
  };

  // the actor runtime: signal.onabort is inert; `.aborted` and addEventListener are the supported forms.
  if (signal?.aborted) return abandoned();

  // An identical write from this same caller was already in flight when we were admitted. Wait it
  // out and answer from its response: forwarding now would hit the upstream twice with the same
  // write, which is the whole thing the fingerprint exists to prevent.
  if (admit.dedupe_wait === true && fp) {
    const replayed = await awaitOriginal(stub, fp, startedAt + WAIT_CAP_MS, signal);
    if (replayed) return tagCell(replayed, decidingCell);
    if (signal?.aborted) return abandoned();
  }

  const waited = Date.now() - startedAt;
  let upstreamRes = await forwardOrError(request, config, rest, url, bodyBuf, bodyStream);
  let retried = false;

  const retryable = isWrite ? WRITE_RETRY_STATUSES : RETRY_STATUSES;
  if (retryable.has(upstreamRes.status) && !streamBody) {
    // No fp here: this is not the final answer, so it must not become the dedupe entry.
    ctx.waitUntil(record({ status: upstreamRes.status, upstream_error: true, waited_ms: waited }));
    await cancelBody(upstreamRes);
    await sleep(RETRY_DELAY_MS, signal);
    upstreamRes = await forwardOrError(request, config, rest, url, bodyBuf, bodyStream);
    retried = true;
  }

  const headers = buildResponseHeaders(upstreamRes.headers);
  headers.set("X-Gate", "gate");
  headers.set("X-Gate-Cell", decidingCell);

  const wantsBody = ownedFp !== null && config.store_bodies && !BODYLESS_STATUSES.has(upstreamRes.status);
  let outBody: BodyInit | null = null;
  let body_b64: string | undefined;

  if (BODYLESS_STATUSES.has(upstreamRes.status)) {
    await cancelBody(upstreamRes);
  } else if (wantsBody) {
    const bytes = new Uint8Array(await upstreamRes.arrayBuffer());
    outBody = bytes;
    if (bytes.byteLength <= DEDUPE_BODY_CAP) body_b64 = toBase64(bytes);
  } else {
    outBody = upstreamRes.body;
  }

  ctx.waitUntil(
    record({
      fp: ownedFp,
      dd_at: ddAt,
      status: upstreamRes.status,
      headers: body_b64 !== undefined ? headersToObject(headers) : undefined,
      body_b64,
      waited_ms: waited,
      retried,
      upstream_error: RETRY_STATUSES.has(upstreamRes.status),
    }),
  );

  return new Response(outBody, { status: upstreamRes.status, statusText: upstreamRes.statusText, headers });
}

function rateLimited(key: string, retryAfterS: unknown): Response {
  const retryAfter = Math.max(1, Number(retryAfterS) || Math.ceil(WAIT_CAP_MS / 1000));
  return gateJson(
    { error: "rate_limited", upstream: key, retry_after_s: retryAfter, wait_cap_ms: WAIT_CAP_MS },
    { status: 503, headers: { "Retry-After": String(retryAfter) } },
  );
}

/**
 * Metering failing must never take an upstream offline (AI.md hard rule 8). Forward unmetered
 * and say so in a header so the bypass is visible in the proxy/agent logs.
 */
async function bypassMeter(
  request: Request,
  config: UpstreamConfig,
  cell: string,
  rest: string,
  url: URL,
  bodyBuf: ArrayBuffer | null,
  bodyStream: ReadableStream<Uint8Array> | null,
  err: unknown,
): Promise<Response> {
  console.error(`gate: meter unavailable for ${cell}, forwarding unmetered`, String(err));
  const bypass = await forwardOrError(request, config, rest, url, bodyBuf, bodyStream);
  const bypassHeaders = buildResponseHeaders(bypass.headers);
  bypassHeaders.set("X-Gate", "gate");
  bypassHeaders.set("X-Gate-Meter", "bypass");
  return new Response(bypass.body, { status: bypass.status, statusText: bypass.statusText, headers: bypassHeaders });
}

/**
 * The dedupe state machine depends on /record landing: a placeholder left at done:false blocks
 * duplicates from being replayed for the rest of the window. Retry a few times before giving up.
 */
async function recordWithRetry(stub: DurableObjectStub, payload: Record<string, unknown>, attempts = 3): Promise<void> {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await meterCall(stub, "/record", payload);
      return;
    } catch (err) {
      if (attempt === attempts) {
        console.error(`gate: /record lost after ${attempts} attempts, dedupe slot may stay open`, String(err));
        return;
      }
      await sleep(PEEK_POLL_START_MS * attempt);
    }
  }
}

/**
 * Wait out an identical write that was already in flight when this one was admitted, and answer
 * from its response. Returns null when there is nothing left to wait for (the original finished
 * without a replayable entry, vanished, or the caller went away) or when the deadline passes -
 * at which point the caller is served by forwarding rather than left hanging.
 */
async function awaitOriginal(
  stub: DurableObjectStub,
  fp: string,
  deadline: number,
  signal: AbortSignal | null,
): Promise<Response | null> {
  let delay = PEEK_POLL_START_MS;
  for (;;) {
    const peeked = await meterCall(stub, "/peek", { fp }).catch(() => ({}) as Record<string, unknown>);
    if (peeked.dedupe && typeof peeked.dedupe === "object") {
      return replay(peeked.dedupe as Record<string, unknown>);
    }
    if (peeked.pending !== true) return null;
    if (signal?.aborted) return null;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      console.error("gate: gave up waiting on an in-flight duplicate write, forwarding it");
      return null;
    }
    await sleep(Math.min(delay, remaining), signal);
    delay = Math.min(delay * 2, PEEK_POLL_MAX_MS);
  }
}

async function forward(
  request: Request,
  config: UpstreamConfig,
  rest: string,
  url: URL,
  bodyBuf: ArrayBuffer | null,
  bodyStream: ReadableStream<Uint8Array> | null,
): Promise<Response> {
  const target = targetUrl(config, rest, url.search);
  if (!target) return Response.json({ error: "path_outside_upstream" }, { status: 404 });
  const headers = buildForwardHeaders(request.headers, url);
  const method = request.method.toUpperCase();
  const hasBody = method !== "GET" && method !== "HEAD";
  const init: RequestInit = {
    method,
    headers,
    body: hasBody ? (bodyStream ?? bodyBuf) : null,
  };
  if (bodyStream && hasBody) (init as { duplex?: string }).duplex = "half";
  const res = await fetch(target.toString(), init);
  return new Response(res.body, {
    status: res.status,
    statusText: res.statusText,
    headers: buildResponseHeaders(res.headers),
  });
}

/** Forward, turning a transport failure into a 502 so the meter still records it. */
async function forwardOrError(
  request: Request,
  config: UpstreamConfig,
  rest: string,
  url: URL,
  bodyBuf: ArrayBuffer | null,
  bodyStream: ReadableStream<Uint8Array> | null,
): Promise<Response> {
  try {
    return await forward(request, config, rest, url, bodyBuf, bodyStream);
  } catch (err) {
    console.error("gate: upstream fetch failed", String(err));
    return Response.json({ error: "upstream_unreachable" }, { status: 502 });
  }
}

async function handleMeterAdmin(request: Request, env: Env, url: URL, key: string): Promise<Response> {
  const parsed = parseCellName(key);
  if (!parsed) return gateJson({ error: "unknown cell" }, { status: 404 });
  // Only configured upstreams get a Meter instance: an arbitrary key would mint unbounded DOs.
  const config = upstreams(env)[parsed.upstream];
  if (!config) {
    return gateJson({ error: "unknown upstream", upstream: parsed.upstream }, { status: 404 });
  }
  // Caller cells exist only for per-caller upstreams; on any other upstream the form is unknown.
  if (parsed.caller !== null && config.caller_rate === undefined) {
    return gateJson({ error: "unknown cell" }, { status: 404 });
  }
  const stub = env.METER.get(env.METER.idFromName(key));
  const res = await stub.fetch(new Request(`http://meter/${url.search}`, { method: request.method }));
  const headers = new Headers(res.headers);
  headers.set("X-Gate", "gate");
  return new Response(res.body, { status: res.status, headers });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health" || url.pathname === "/") return new Response("ok\n");

    const via = url.pathname.match(/^\/via\/([^/]+)(\/.*)?$/);
    if (via) {
      return handleVia(request, env, ctx, url, decodeURIComponent(via[1]), via[2] ?? "/");
    }

    const meter = url.pathname.match(/^\/meter\/([^/]+)\/?$/);
    if (meter && (request.method === "GET" || request.method === "DELETE")) {
      return handleMeterAdmin(request, env, url, decodeURIComponent(meter[1]));
    }

    const limit = url.pathname.match(/^\/(limit|status)\/([^/]+)$/);
    if (limit) {
      const id = env.LIMITER.idFromName(decodeURIComponent(limit[2]));
      return env.LIMITER.get(id).fetch(request);
    }

    // Transparent mode (change 3): the proxy forwards the caller's ORIGINAL path and names the upstream
    // in a header, so the same reverse_proxy block can fall through to the real upstream when the
    // gate is down without a path rewrite that only one of the two targets understands.
    const designated = request.headers.get("X-Gate-Upstream");
    if (designated) {
      return handleVia(request, env, ctx, url, designated.trim().toLowerCase(), url.pathname);
    }

    return new Response(
      "usage: /via/<upstream>/<path> | /meter/<upstream>[@<hash16>]?rate=&burst= | /limit/<key>?cost=1&rate=5&burst=10 | /status/<key>\n",
      { status: 404, headers: { "X-Gate": "gate" } },
    );
  },
};
