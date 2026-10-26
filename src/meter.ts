// gate: Meter actor - one instance per upstream key ("tracker", "bridge", "secrets", ...).
//
// Doctrine: hold, never drop. The DO does NOT perform the upstream call inside its serialized turn.
// It hands out a release timestamp (reserve-ahead token bucket, negative balance allowed) and the
// Worker sleeps until then. That gives FIFO order per upstream, one DO turn per request, and many
// forwards in flight at once.
//
// Internal endpoints (Worker -> stub only; never routed from the public listener):
//   POST /admit   {method, path, fp?, now?, default_rate?, default_burst?, max_wait_ms?, caller?}
//                 -> {release_at, wait_ms, dedupe_wait?} | {reject:true, retry_after_s} | {dedupe:{...}}
//   POST /record  {fp?, status, headers?, body_b64?, waited_ms?, aborted?, upstream_error?, retried?, store_bodies?, caller?}
//                 caller (change 12): true only from the Worker's caller-cell calls (`<upstream>@<hash16>`).
//                 Sticky once seen; it makes the cell prunable after IDLE_PRUNE_MS without /admit or /record.
//   POST /peek    {fp, now?} -> {dedupe:{...}} | {pending:true} | {miss:true}
//                 A dedupe hit is counted (`deduped`) here; the Worker replays it and sends nothing
//                 else for that request, so a replay is never counted twice. /peek spends no token.
//                 max_wait_ms (change 6): remaining wait budget; reject beyond it. Absent => WAIT_CAP_MS.
// Public (via the Worker's /meter/<upstream>):
//   GET  /?rate=&burst=  -> bucket + counters (re-parameterises and persists when params are present)
//   DELETE /             -> reset bucket, counters and the dedupe table
//
// Persistence (change 8): bucket and counters live in actor memory and are flushed at most once per
// FLUSH_INTERVAL_MS by the object's one alarm, which also purges expired dedupe entries. Only the
// dedupe table (`dd:<fp>`) and a re-parameterising GET write synchronously. Storage keys:
// "bucket", "counters", "dd:<fp>", and (change 12) "cell" = {caller, last_touched}, written with the flush;
// a fresh instance reloads them on first touch.
//
// Idle pruning (change 12, owner ruling 2026-10-26): a caller cell untouched by /admit and /record for
// IDLE_PRUNE_MS is deleted by its own alarm (storage, memory, no re-arm) so the object can evict. A clean
// caller cell holds one alarm at last_touched + IDLE_PRUNE_MS, never a 10 s loop. Shared cells never prune.

import { base64Bytes } from "./util.ts";
import { FLUSH_INTERVAL_MS, MIN_ALARM_LEAD_MS, Once, Persistence, commitWrites, exclusive, startLoad } from "./persist.ts";

export const WAIT_CAP_MS = 30_000; // owner ruling 2026-09-03: beyond this, 503 + Retry-After
export const DEDUPE_TTL_MS = 60_000; // owner ruling: write-fingerprint dedupe window
export const BACKOFF_MS = 10_000; // halve the refill rate for this long after an upstream error
export const DEDUPE_BODY_CAP = 256 * 1024; // larger writes forward but are not replayable
export const HIST_BUCKETS: readonly number[] = [0, 100, 500, 1000, 5000, 10000, 30000];
export const DEFAULT_RATE = 5;
export const DEFAULT_BURST = 10;
export const BACKOFF_STATUSES: ReadonlySet<number> = new Set([429, 502, 503, 504]);
/** change 12 owner ruling 2026-10-26: a caller cell idle (no /admit, no /record) this long is pruned. */
export const IDLE_PRUNE_MS = 24 * 60 * 60 * 1000;

/** Persisted cell identity + activity (storage key "cell"), flushed with bucket and counters. */
export interface CellMeta {
  caller: boolean;
  last_touched: number | null;
}

const MIN_RATE = 0.01; // never divide by zero when scheduling a release
const MAX_RATE = 10_000;
const MAX_BURST = 100_000;
const MAX_STORED_HEADERS = 64;
const MAX_STORED_HEADER_LEN = 2048;

export interface Bucket {
  tokens: number;
  updated: number;
  rate: number;
  burst: number;
  backoff_until: number;
}

export interface Counters {
  admitted: number;
  forwarded: number;
  deduped: number;
  dropped_abort: number;
  rejected_cap: number;
  upstream_errors: number;
  retries: number;
  waited_ms: number[]; // one count per HIST_BUCKETS entry
}

export interface DedupeEntry {
  at: number; // first attempt, anchors the 60 s window
  release_at: number;
  done: boolean;
  replayable: boolean;
  status?: number;
  headers?: Record<string, string>;
  body_b64?: string;
}

export function emptyCounters(): Counters {
  return {
    admitted: 0,
    forwarded: 0,
    deduped: 0,
    dropped_abort: 0,
    rejected_cap: 0,
    upstream_errors: 0,
    retries: 0,
    waited_ms: HIST_BUCKETS.map(() => 0),
  };
}

/** Positive finite rate, or the fallback. Guards against `?rate=abc` poisoning the bucket. */
export function sanitiseRate(value: unknown, fallback: number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.max(n, MIN_RATE), MAX_RATE);
}

export function sanitiseBurst(value: unknown, fallback: number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(n, MAX_BURST);
}

/**
 * Longest reservation an /admit may hand out. `max_wait_ms` is the caller's remaining budget when
 * the request already waited in another cell (change 6 per-caller flow: the caller cell first, then
 * this one), so the two waits together never exceed the owner-ruled cap. Absent => the full cap.
 */
export function waitBudget(value: unknown): number {
  if (value === undefined || value === null) return WAIT_CAP_MS;
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return WAIT_CAP_MS;
  return Math.min(Math.max(n, 0), WAIT_CAP_MS);
}

/** Refill rate in effect right now: halved while an upstream backoff is running. */
export function effectiveRate(bucket: Bucket, now: number): number {
  const rate = bucket.backoff_until > now ? bucket.rate / 2 : bucket.rate;
  return Math.max(rate, MIN_RATE);
}

/**
 * Advance the bucket to `now` and return the clock the caller should schedule against.
 *
 * The bucket clock is monotonic. `now` is the Worker's timestamp, taken before the DO round trip,
 * so two concurrent admits can reach this serialized turn in an order that does not match their
 * timestamps. Writing an older `now` into `updated` would rewind the clock, and the next refill
 * would then measure `elapsed` against a stale baseline and over-credit tokens - quietly serving
 * faster than the owner-ruled rate. Clamping forward makes out-of-order arrival harmless.
 */
export function refill(bucket: Bucket, now: number): number {
  const clock = Math.max(bucket.updated, now);
  const elapsed = Math.max(0, (clock - bucket.updated) / 1000);
  bucket.tokens = Math.min(bucket.burst, bucket.tokens + elapsed * effectiveRate(bucket, clock));
  bucket.updated = clock;
  return clock;
}

export function histIndex(waitMs: number): number {
  let i = 0;
  for (let k = 0; k < HIST_BUCKETS.length; k++) {
    if (waitMs >= HIST_BUCKETS[k]) i = k;
  }
  return i;
}

function ddKey(fp: string): string {
  return `dd:${fp}`;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function sanitiseHeaders(value: unknown): Record<string, string> {
  const src = asRecord(value);
  const out: Record<string, string> = {};
  let count = 0;
  for (const [k, v] of Object.entries(src)) {
    if (count >= MAX_STORED_HEADERS) break;
    if (typeof v !== "string") continue;
    out[k] = v.length > MAX_STORED_HEADER_LEN ? v.slice(0, MAX_STORED_HEADER_LEN) : v;
    count++;
  }
  return out;
}

function replayPayload(entry: DedupeEntry) {
  return { status: entry.status ?? 200, headers: entry.headers ?? {}, body_b64: entry.body_b64 ?? "" };
}

export function newBucket(now: number, defaultRate?: unknown, defaultBurst?: unknown): Bucket {
  const burst = sanitiseBurst(defaultBurst, DEFAULT_BURST);
  return { tokens: burst, updated: now, rate: sanitiseRate(defaultRate, DEFAULT_RATE), burst, backoff_until: 0 };
}

function normaliseCounters(stored: Counters | undefined): Counters {
  const counters = stored ?? emptyCounters();
  if (!Array.isArray(counters.waited_ms) || counters.waited_ms.length !== HIST_BUCKETS.length) {
    counters.waited_ms = HIST_BUCKETS.map(() => 0);
  }
  return counters;
}

export class Meter {
  state: DurableObjectState;
  /** In-memory bucket; null until the first admit seeds it (or storage held one). */
  bucket: Bucket | null = null;
  counters: Counters = emptyCounters();
  readonly persist: Persistence;
  private readonly loader: Once;
  /** dd key -> `at` of the entry this instance last wrote; guards the purge against a re-mint. */
  private readonly ddWritten = new Map<string, number>();
  /** change 12: a per-caller cell (learned from `caller: true` on /admit or /record; sticky). */
  caller = false;
  /** change 12: Date.now() of the last /admit or /record; null = never touched by this cell's state. */
  lastTouched: number | null = null;

  constructor(state: DurableObjectState, _env?: unknown) {
    this.state = state;
    this.persist = new Persistence(state.storage);
    this.loader = new Once(() => this.load());
    startLoad(state, this.loader);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    await this.loader.run();

    if (request.method === "POST") {
      const body = asRecord(await request.json().catch(() => null));
      if (path === "/admit") return this.admit(body);
      if (path === "/record") return this.record(body);
      if (path === "/peek") return this.peek(body);
      return Response.json({ error: "unknown meter endpoint" }, { status: 404 });
    }
    if (request.method === "DELETE") return this.reset();
    if (request.method === "GET") return this.report(url);
    return Response.json({ error: "method not allowed" }, { status: 405 });
  }

  /**
   * The object's one alarm: flush bucket + counters when dirty and purge expired dedupe entries in
   * one storage transaction. Re-arms only while there is something left to do, so an idle cell
   * holds no alarm and writes nothing. Whatever throws, dirty state is never left without an alarm.
   */
  async alarm(): Promise<void> {
    this.persist.alarmFired();
    let nextExpiry = Infinity;
    let failed = false;
    let flushed = false;
    try {
      await this.loader.run();
      if (this.prunable(Date.now())) {
        await this.prune();
        return;
      }
      const storage = this.state.storage;
      const entries = await storage.list<DedupeEntry>({ prefix: "dd:" });

      // ---- synchronous segment: snapshot and choose deletes against current memory ----
      const now = Date.now();
      const flushing = this.persist.dirty;
      const puts: [string, unknown][] = [];
      if (flushing) {
        if (this.bucket) puts.push(["bucket", { ...this.bucket }]);
        puts.push(["counters", { ...this.counters, waited_ms: [...this.counters.waited_ms] }]);
        puts.push(["cell", this.meta()]);
      }
      const deletes: string[] = [];
      for (const [key, entry] of entries) {
        // An admit may have re-minted this key during the list() await: never delete a newer entry.
        const written = this.ddWritten.get(key);
        if (written !== undefined && entry && written !== entry.at) {
          nextExpiry = Math.min(nextExpiry, written + DEDUPE_TTL_MS);
          continue;
        }
        if (!entry || now - entry.at >= DEDUPE_TTL_MS) deletes.push(key);
        else nextExpiry = Math.min(nextExpiry, entry.at + DEDUPE_TTL_MS);
      }
      this.persist.dirty = false;
      // ---- end of synchronous segment ----

      try {
        await commitWrites(storage, puts, deletes);
        if (flushing) {
          this.persist.lastFlush = now;
          flushed = true;
        }
        for (const key of deletes) {
          const entry = entries.get(key);
          if (entry && this.ddWritten.get(key) === entry.at) this.ddWritten.delete(key);
        }
      } catch (err) {
        if (flushing) this.persist.dirty = true;
        throw err;
      }
    } catch (err) {
      failed = true;
      console.error("gate: meter alarm failed, retrying next interval", String(err));
    } finally {
      const after = Date.now();
      let target = Infinity;
      if (this.persist.dirty || failed) target = after + FLUSH_INTERVAL_MS;
      if (Number.isFinite(nextExpiry)) target = Math.min(target, Math.max(nextExpiry, after + MIN_ALARM_LEAD_MS));
      // change 12 idle prune: a caller cell keeps exactly one alarm. Right after a flush it is armed one
      // interval out (a busy cell then costs one setAlarm per interval, as before: touches inside the
      // interval find the alarm already due sooner); an alarm that finds the cell clean arms the prune
      // deadline instead, so an idle caller cell sleeps until last_touched + IDLE_PRUNE_MS.
      if (!failed && this.caller && this.lastTouched !== null) {
        const idle = flushed ? after + FLUSH_INTERVAL_MS : this.lastTouched + IDLE_PRUNE_MS;
        target = Math.min(target, Math.max(idle, after + MIN_ALARM_LEAD_MS));
      }
      if (Number.isFinite(target)) await this.persist.ensureAlarm(target);
    }
  }

  private async load(): Promise<void> {
    const storage = this.state.storage;
    const [bucket, counters, meta] = await Promise.all([
      storage.get<Bucket>("bucket"),
      storage.get<Counters>("counters"),
      storage.get<CellMeta>("cell"),
    ]);
    await this.persist.loadAlarm();
    // Assign only after every read resolved: a half-loaded instance never serves.
    this.bucket = bucket ?? null;
    this.counters = normaliseCounters(counters);
    // Sticky: an event that landed before the load finished (no input gate) may already have set it.
    this.caller = this.caller || meta?.caller === true;
    const stored = typeof meta?.last_touched === "number" && Number.isFinite(meta.last_touched) ? meta.last_touched : null;
    if (stored !== null && (this.lastTouched === null || stored > this.lastTouched)) this.lastTouched = stored;
  }

  private meta(): CellMeta {
    return { caller: this.caller, last_touched: this.lastTouched };
  }

  /** change 12: an /admit or /record touched this cell. Synchronous; the caller flag is sticky. */
  private touch(body: Record<string, unknown>): void {
    if (body.caller === true) this.caller = true;
    this.lastTouched = Date.now();
  }

  /** Only a caller cell, clean, untouched for IDLE_PRUNE_MS. Shared cells (caller false) never prune. */
  private prunable(now: number): boolean {
    return this.caller && this.lastTouched !== null && !this.persist.dirty && now - this.lastTouched >= IDLE_PRUNE_MS;
  }

  /**
   * Delete everything this idle caller cell holds (bucket, counters, cell meta, any dd entry) and its
   * alarm, then forget it in memory; no alarm is re-armed, so the object can evict. Re-checked with
   * the input gate closed: an /admit that slipped in first keeps the cell. The next request re-seeds
   * the cell from its /admit defaults (caller 4/10), exactly like a fresh cell.
   */
  private async prune(): Promise<void> {
    await exclusive(this.state, async () => {
      if (!this.prunable(Date.now())) return;
      const kept = { bucket: this.bucket, counters: this.counters, lastTouched: this.lastTouched };
      this.bucket = null;
      this.counters = emptyCounters();
      this.ddWritten.clear();
      this.caller = false;
      this.lastTouched = null;
      this.persist.reset();
      const storage = this.state.storage;
      try {
        await storage.deleteAll();
        if (typeof storage.deleteAlarm === "function") await storage.deleteAlarm();
      } catch (err) {
        // Storage may still hold the cell: keep it prunable in memory too (unless an admit already
        // re-touched it), and let alarm() re-arm on the failure so the prune is retried.
        if (this.lastTouched === null) {
          this.caller = true;
          this.lastTouched = kept.lastTouched;
          if (this.bucket === null) this.bucket = kept.bucket;
          if (!this.persist.dirty) this.counters = kept.counters;
        } else {
          this.caller = true;
        }
        throw err;
      }
    });
    // Without an input gate an admit may have landed during the awaits; alarm() re-arms while dirty.
    if (this.persist.dirty) this.persist.alarmAt = null;
  }

  /** The in-memory bucket, seeded on first touch. A stored bucket always keeps its own rate/burst. */
  private touchBucket(now: number, defaultRate?: unknown, defaultBurst?: unknown): Bucket {
    if (!this.bucket) this.bucket = newBucket(now, defaultRate, defaultBurst);
    return this.bucket;
  }

  private async liveEntry(fp: string | null, now: number): Promise<DedupeEntry | undefined> {
    if (!fp) return undefined;
    const entry = await this.state.storage.get<DedupeEntry>(ddKey(fp));
    if (entry && now - entry.at < DEDUPE_TTL_MS) return entry;
    return undefined;
  }

  /** A dedupe entry needs the purge alarm by the end of its window (merged with the flush alarm). */
  private ensurePurge(at: number): Promise<void> {
    return this.persist.ensureAlarm(at + DEDUPE_TTL_MS);
  }

  private async admit(body: Record<string, unknown>): Promise<Response> {
    const storage = this.state.storage;
    const now = Number.isFinite(Number(body.now)) ? Number(body.now) : Date.now();
    const fp = typeof body.fp === "string" && body.fp.length > 0 ? body.fp : null;
    // The only read is the dedupe entry; every bucket/counter change below runs without an await.
    const live = await this.liveEntry(fp, now);

    this.touch(body);
    const bucket = this.touchBucket(now, body.default_rate, body.default_burst);
    const counters = this.counters;
    const clock = refill(bucket, now);

    // A completed, replayable write inside the window never reaches the upstream and never
    // spends a token.
    if (live && live.done && live.replayable) {
      counters.deduped++;
      await this.persist.markDirty(Date.now());
      return Response.json({ dedupe: replayPayload(live) });
    }

    // Reserve ahead: allow the balance to go negative and schedule the release that far out.
    const rate = effectiveRate(bucket, clock);
    const tentative = bucket.tokens - 1;
    const wait_ms = (Math.max(0, -tentative) / rate) * 1000;

    if (wait_ms > waitBudget(body.max_wait_ms)) {
      // Rejected requests never hold a reservation - a burst of rejects must not push the queue out
      // for the callers that are actually waiting.
      counters.rejected_cap++;
      await this.persist.markDirty(Date.now());
      return Response.json({
        reject: true,
        retry_after_s: Math.max(1, Math.ceil(wait_ms / 1000)),
        wait_ms,
      });
    }

    bucket.tokens = tentative;
    counters.admitted++;
    counters.waited_ms[histIndex(wait_ms)]++;
    const release_at = clock + wait_ms;
    const ownsPlaceholder = fp !== null && !live;
    // ---- end of the synchronous mutation ----

    await this.persist.markDirty(Date.now());
    let dd_at: number | undefined;
    if (ownsPlaceholder && fp) {
      // Dedupe placeholders are the one per-request write that must be durable before we answer.
      this.ddWritten.set(ddKey(fp), clock);
      await storage.put(ddKey(fp), { at: clock, release_at, done: false, replayable: false });
      await this.ensurePurge(clock);
      dd_at = clock;
    }
    // dedupe_wait: an identical write is still in flight. The caller re-checks with /peek at
    // release time, so the second write is answered from the first response when it has landed.
    // dd_at: generation tag of the placeholder this admission owns; /record must echo it so a
    // stale record (delayed past the window) can never finalise a newer placeholder for the same fp.
    return Response.json({ release_at, wait_ms, dedupe_wait: Boolean(live && !live.done), dd_at });
  }

  private async record(body: Record<string, unknown>): Promise<Response> {
    const storage = this.state.storage;
    const now = Number.isFinite(Number(body.now)) ? Number(body.now) : Date.now();
    const fp = typeof body.fp === "string" && body.fp.length > 0 ? body.fp : null;
    const status = Number(body.status) || 0;
    const ddAt = typeof body.dd_at === "number" ? body.dd_at : null;
    const previous = await this.liveEntry(fp, now);

    // ---- synchronous mutation of bucket + counters ----
    // /record never seeds a bucket: it carries no default_rate/default_burst, and a bucket created
    // here at the Meter defaults (5/10) would be flushed and then win over the cell's owner-ruled
    // numbers forever (e.g. a restart between a caller cell's first /admit and its /record). Without
    // a bucket only the counters move; the next /admit seeds the bucket from its own defaults.
    this.touch(body);
    const bucket = this.bucket;
    const counters = this.counters;
    const clock = bucket ? refill(bucket, now) : now;
    let ddWrite: (() => Promise<unknown>) | null = null;

    if (body.aborted === true) {
      counters.dropped_abort++;
      // Drop the in-flight placeholder: nothing was sent upstream, so nothing is deduplicable.
      if (fp && previous && (ddAt === null || previous.at === ddAt)) {
        ddWrite = () => {
          this.ddWritten.delete(ddKey(fp));
          return storage.delete(ddKey(fp));
        };
      }
    } else {
      counters.forwarded++;
      if (body.retried === true) counters.retries++;
      if (body.upstream_error === true || BACKOFF_STATUSES.has(status)) {
        counters.upstream_errors++;
        if (bucket) bucket.backoff_until = clock + BACKOFF_MS;
      }
      // Only the admission that created the live placeholder may finalise it. If the placeholder
      // aged out and a later admission minted a new one for the same fp, this record is stale.
      const owns = fp !== null && (ddAt === null ? previous !== undefined : previous?.at === ddAt);
      if (fp && owns) {
        const entry: DedupeEntry = {
          at: previous?.at ?? now,
          release_at: previous?.release_at ?? now,
          done: true,
          replayable: false,
          status,
        };
        // Owner rule: bodies and headers are stored ONLY when the upstream opts in.
        // Upstreams holding secrets (secrets) keep status and nothing else, ever.
        if (body.store_bodies === true) {
          const b64 = typeof body.body_b64 === "string" ? body.body_b64 : null;
          if (b64 !== null && base64Bytes(b64) <= DEDUPE_BODY_CAP) {
            entry.headers = sanitiseHeaders(body.headers);
            entry.body_b64 = b64;
            entry.replayable = true;
          }
        }
        ddWrite = async () => {
          this.ddWritten.set(ddKey(fp), entry.at);
          await storage.put(ddKey(fp), entry);
          await this.ensurePurge(entry.at);
        };
      }
    }
    // ---- end of the synchronous mutation ----

    await this.persist.markDirty(Date.now());
    if (ddWrite) await ddWrite();
    return Response.json({ recorded: true });
  }

  private async peek(body: Record<string, unknown>): Promise<Response> {
    const now = Number.isFinite(Number(body.now)) ? Number(body.now) : Date.now();
    const fp = typeof body.fp === "string" && body.fp.length > 0 ? body.fp : null;
    const live = await this.liveEntry(fp, now);
    if (live && live.done && live.replayable) {
      this.counters.deduped++;
      await this.persist.markDirty(Date.now());
      return Response.json({ dedupe: replayPayload(live) });
    }
    // `pending` means the first attempt is still out at the upstream, so a duplicate must keep
    // waiting rather than send the same write a second time. `miss` means there is nothing to
    // wait for - the entry never existed, aged out, or completed without a replayable body.
    if (live && !live.done) return Response.json({ pending: true });
    return Response.json({ miss: true });
  }

  /** DELETE: clear memory and storage, with the input gate closed where the runtime allows it. */
  private async reset(): Promise<Response> {
    await exclusive(this.state, async () => {
      this.bucket = null;
      this.counters = emptyCounters();
      this.ddWritten.clear();
      this.caller = false;
      this.lastTouched = null;
      this.persist.reset();
      const storage = this.state.storage;
      await storage.deleteAll();
      if (typeof storage.deleteAlarm === "function") await storage.deleteAlarm();
    });
    // Without an input gate an admit may have landed during the awaits above. Its alarm may have
    // been removed by deleteAlarm(), so forget the known slot and arm again: do not strand it.
    if (this.persist.dirty) {
      this.persist.alarmAt = null;
      await this.persist.markDirty(Date.now());
    }
    return Response.json({ reset: true });
  }

  private async report(url: URL): Promise<Response> {
    const storage = this.state.storage;
    // `Number(null)` is 0, which Number.isFinite accepts: a missing param must mean "now", not epoch.
    const nowParam = url.searchParams.get("now");
    const now = nowParam !== null && Number.isFinite(Number(nowParam)) ? Number(nowParam) : Date.now();
    const reparam = url.searchParams.has("rate") || url.searchParams.has("burst");

    let bucket: Bucket;
    if (reparam) {
      // Re-parameterisation is rare and must survive a restart: apply to memory, persist now.
      bucket = this.touchBucket(now);
      if (url.searchParams.has("rate")) bucket.rate = sanitiseRate(url.searchParams.get("rate"), bucket.rate);
      if (url.searchParams.has("burst")) bucket.burst = sanitiseBurst(url.searchParams.get("burst"), bucket.burst);
      refill(bucket, now);
      await storage.put("bucket", { ...bucket });
    } else {
      // A plain read never seeds a bucket or moves the clock: compute on a copy.
      bucket = this.bucket ? { ...this.bucket } : newBucket(now);
      refill(bucket, now);
    }

    const dedupe = await storage.list<DedupeEntry>({ prefix: "dd:" });
    return Response.json({
      bucket: {
        tokens: Math.round(bucket.tokens * 1000) / 1000,
        updated: bucket.updated,
        rate: bucket.rate,
        burst: bucket.burst,
        backoff_until: bucket.backoff_until,
        effective_rate: effectiveRate(bucket, now),
        backoff_active: bucket.backoff_until > now,
      },
      counters: this.counters,
      waited_ms_buckets: HIST_BUCKETS,
      dedupe_entries: dedupe.size,
      persistence: this.persist.report(),
      idle: {
        caller: this.caller,
        last_touched: this.lastTouched,
        prune_at: this.caller && this.lastTouched !== null ? this.lastTouched + IDLE_PRUNE_MS : null,
      },
      now,
    });
  }
}
