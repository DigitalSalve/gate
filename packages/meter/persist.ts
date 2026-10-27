// gate: interval persistence shared by the Meter and RateLimiter actors (change 8).
//
// the actor runtime writes one level-0 LTX object to the fleet bucket per storage transaction and never
// deletes covered L0 objects, so a put per request exhausted inodes on the gate host (2026-10-04 outage).
// Bucket and counters therefore live in actor memory; this helper tracks the dirty flag and the
// single per-object alarm that flushes them at most once per FLUSH_INTERVAL_MS. Losing up to one
// interval of counters on a crash is accepted by the owner brief.

/**
 * 10 s, the bottom of the owner's 10-30 s range: a busy cell costs one flush plus one alarm wake
 * entry per interval regardless of traffic (a >99% cut from ~6.5 objects per request), and a crash
 * loses at most 10 s of counters - a third of the 30 s reservation horizon. See the paper on in-memory state.
 */
export const FLUSH_INTERVAL_MS = 10_000;
/** Never re-arm closer than this, so an alarm can not spin on an entry that expires "now". */
export const MIN_ALARM_LEAD_MS = 1_000;

export interface PersistenceReport {
  interval_ms: number;
  dirty: boolean;
  last_flush: number | null;
}

export class Persistence {
  private readonly storage: DurableObjectStorage;
  /** Unpersisted in-memory change since the last successful flush. */
  dirty = false;
  /** Alarm this instance knows is scheduled; null = none. Set synchronously before setAlarm. */
  alarmAt: number | null = null;
  lastFlush: number | null = null;

  constructor(storage: DurableObjectStorage) {
    this.storage = storage;
  }

  /** Read the stored alarm so a reloaded instance does not schedule a second one. */
  async loadAlarm(): Promise<void> {
    if (typeof this.storage.getAlarm !== "function") return;
    const at = await this.storage.getAlarm();
    this.alarmAt = typeof at === "number" ? at : null;
  }

  /** Record an in-memory change and make sure a flush is due within one interval. */
  markDirty(now: number): Promise<void> {
    this.dirty = true;
    return this.ensureAlarm(now + FLUSH_INTERVAL_MS);
  }

  /**
   * Schedule the one alarm no later than `target`. The known alarm is updated synchronously, so
   * concurrent events in the same interval never issue a second setAlarm (each one is a durable
   * wake entry in the bucket). A failed setAlarm forgets the slot so the next mutation retries.
   */
  async ensureAlarm(target: number): Promise<void> {
    if (this.alarmAt !== null && this.alarmAt <= target) return;
    if (typeof this.storage.setAlarm !== "function") return;
    this.alarmAt = target;
    try {
      await this.storage.setAlarm(target);
    } catch (err) {
      if (this.alarmAt === target) this.alarmAt = null;
      console.error("gate: setAlarm failed, flush re-armed on the next change", String(err));
    }
  }

  /** Called first thing in alarm(): the alarm that fired is no longer scheduled. */
  alarmFired(): void {
    this.alarmAt = null;
  }

  /** Forget everything after a DELETE. */
  reset(): void {
    this.dirty = false;
    this.alarmAt = null;
  }

  report(): PersistenceReport {
    return { interval_ms: FLUSH_INTERVAL_MS, dirty: this.dirty, last_flush: this.lastFlush };
  }
}

/**
 * Once-promise loader. The first event (or the constructor, through blockConcurrencyWhile) starts
 * the load; every later event awaits the same promise. A rejected load is forgotten so the next
 * event retries instead of the instance failing forever.
 */
export class Once {
  private pending: Promise<void> | null = null;
  private done = false;
  private readonly fn: () => Promise<void>;

  constructor(fn: () => Promise<void>) {
    this.fn = fn;
  }

  get loaded(): boolean {
    return this.done;
  }

  run(): Promise<void> {
    if (this.done) return Promise.resolve();
    if (!this.pending) {
      this.pending = this.fn().then(
        () => {
          this.done = true;
        },
        (err) => {
          this.pending = null;
          throw err;
        },
      );
    }
    return this.pending;
  }
}

/**
 * Start the load inside blockConcurrencyWhile when the runtime has it (the actor runtime closes the input gate,
 * as the hosted platform does), so no event runs before the state is in memory. Handlers still await
 * `once.run()`, which covers a runtime or test double without it: lazy load on first touch.
 */
export function startLoad(state: DurableObjectState, once: Once): void {
  if (typeof state.blockConcurrencyWhile !== "function") return;
  state.blockConcurrencyWhile(() => once.run()).catch((err) => {
    console.error("gate: initial state load failed, retrying on the next event", String(err));
  });
}

/**
 * Run `fn` with the input gate closed when possible (DELETE: memory + storage reset must not
 * interleave with an admit). Without blockConcurrencyWhile it simply runs.
 */
export function exclusive<T>(state: DurableObjectState, fn: () => Promise<T>): Promise<T> {
  return typeof state.blockConcurrencyWhile === "function" ? state.blockConcurrencyWhile(fn) : fn();
}

/**
 * Commit a flush as ONE storage transaction (one LTX object on the actor runtime) when the runtime offers
 * `transaction()`; the actor runtime only promises grouping for explicit transactions. Fallback: issue every
 * write in one synchronous segment and await them together (implicit coalescing where it exists).
 * All-or-nothing on the transaction path, so bucket and counters never commit out of step.
 */
export async function commitWrites(
  storage: DurableObjectStorage,
  puts: [string, unknown][],
  deletes: string[],
): Promise<void> {
  if (puts.length === 0 && deletes.length === 0) return;
  if (typeof storage.transaction === "function") {
    await storage.transaction(async (txn) => {
      const ops: Promise<unknown>[] = [];
      for (const [k, v] of puts) ops.push(txn.put(k, v));
      for (const k of deletes) ops.push(txn.delete(k));
      await Promise.all(ops);
    });
    return;
  }
  const ops: Promise<unknown>[] = [];
  for (const [k, v] of puts) ops.push(storage.put(k, v));
  for (const k of deletes) ops.push(storage.delete(k));
  await Promise.all(ops);
}
