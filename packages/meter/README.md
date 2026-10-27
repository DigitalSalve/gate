# gate-meter

A reserve-ahead token bucket for a single-threaded durable actor: callers get a release time instead of a rejection, and the service behind it sees a steady rate.

## What it does

`gate-meter` is the pacing core of the gate, packaged so it can be used without the rest of the gate. One actor instance is one cell. A cell answers four questions for the code that fronts a shared service:

- **When may this request go?** `/admit` runs a token bucket whose balance may go negative. A request that finds no token is not refused; it is given a release time that far in the future, up to a hold cap (30 s by default). Beyond the cap the answer is a reject with a `retry_after_s` the caller can turn into a 503 and Retry-After. Rejected requests hold no reservation, so a burst of rejects never pushes the queue out for the callers that are waiting. The caller may pass a smaller `max_wait_ms` when part of the budget was already spent elsewhere.
- **Has this exact write been seen in the last minute?** When the caller passes a write fingerprint, `/admit` stores a placeholder and `/record` finalises it with the upstream's status and, only when the cell is told to, the headers and body, so a resend of the same write inside the window (60 s by default) is answered from the first response instead of reaching the service twice. A second write that arrives while the first is still out gets `dedupe_wait`, and `/peek` tells it when the first has landed. Bodies over 256 KiB are forwarded but never stored.
- **How is the service doing?** `/record` counts forwarded requests, retries, aborts, rejects and upstream errors, keeps a wait-time histogram, and on an upstream error halves the refill rate for a short backoff (10 s by default).
- **What is the cell's state?** `GET /` reports the bucket, the counters, the histogram, the number of dedupe entries, the persistence state and the idle state, and re-parameterises the bucket when `rate` or `burst` are in the query. `DELETE /` resets everything.

State lives in actor memory. One alarm per cell flushes the bucket, the counters and the cell metadata at most once per interval (10 s by default) in a single storage transaction, and purges expired dedupe entries on the same alarm. Only dedupe entries and a re-parameterisation write synchronously. A crash loses at most one interval of counters; the rate guarantee is unchanged because admission is decided in memory, one request at a time. A cell marked as a per-caller cell that sees no `/admit` or `/record` for the idle window (24 h by default) deletes its own storage and alarm from that same alarm, so the actor can be evicted; a shared cell never prunes itself.

## Install

The package is a folder of TypeScript source with no dependencies and no build step. It is consumed by a bundler, the same way the gate consumes it.

```bash
# from a checkout of the gate repo
npm install ./packages/meter

# or copy the folder into your project and import it by path
```

The actor class must be bound in your runtime's configuration like any other durable actor class (the gate binds it as `Meter`), and the runtime must offer the storage API the package expects: `get`, `put`, `delete`, `deleteAll`, `list`, `setAlarm`, `getAlarm`, `deleteAlarm` and, when it has one, `transaction`. The ambient types the package needs are in its own `types.d.ts`.

Tests run with the built-in test runner on node 22.6 or later, against an in-memory storage double. No network, no runtime.

```bash
cd packages/meter && npm test
```

## Usage

```ts
// The actor class, exported so the runtime can bind it.
import { Meter } from "gate-meter";
export { Meter };

// In the request handler that fronts the service.
export default {
  async fetch(request: Request, env: { METER: DurableObjectNamespace }): Promise<Response> {
    const cell = env.METER.get(env.METER.idFromName("tracker"));
    const admit = await cell.fetch("http://meter/admit", {
      method: "POST",
      body: JSON.stringify({ default_rate: 5, default_burst: 10 }),
    });
    const decision = (await admit.json()) as { release_at?: number; reject?: true; retry_after_s?: number };
    if (decision.reject) {
      return new Response("busy", { status: 503, headers: { "Retry-After": String(decision.retry_after_s) } });
    }
    await new Promise((r) => setTimeout(r, Math.max(0, decision.release_at! - Date.now())));
    const upstream = await fetch("http://service.internal" + new URL(request.url).pathname, request);
    await cell.fetch("http://meter/record", { method: "POST", body: JSON.stringify({ status: upstream.status }) });
    return upstream;
  },
};
```

The first ten requests go at once; the rest are released five per second, in arrival order, each one after the sleep it was told to take. Pass `fp` (a hash of caller identity, method, path and body) to `/admit` and `/record` to turn on write dedupe for that request; pass `store_bodies: true` and `body_b64` to `/record` when the service's responses may be replayed, and never for a service that holds secrets.

## API

All endpoints are internal to the actor: the host calls them through the actor stub, never from a public listener. Bodies are JSON.

| Call | Body | Answer |
|---|---|---|
| `POST /admit` | `default_rate`, `default_burst` (seed a new bucket), `now`, `fp`, `max_wait_ms`, `caller` | `{release_at, wait_ms, dedupe_wait, dd_at}`, or `{reject: true, retry_after_s, wait_ms}`, or `{dedupe: {status, headers, body_b64}}` |
| `POST /record` | `status`, `fp`, `dd_at`, `now`, `aborted`, `retried`, `upstream_error`, `store_bodies`, `headers`, `body_b64`, `caller` | `{recorded: true}` |
| `POST /peek` | `fp`, `now` | `{dedupe: {...}}`, `{pending: true}` or `{miss: true}` |
| `GET /?rate=&burst=&now=` | query only | bucket, counters, `waited_ms_buckets`, `dedupe_entries`, `persistence {interval_ms, dirty, last_flush}`, `idle {caller, last_touched, prune_at}` |
| `DELETE /` | none | `{reset: true}` |

`dd_at` is the generation tag of the dedupe placeholder an admission owns; `/record` echoes it so a record delayed past the window can never finalise a newer placeholder for the same fingerprint. `caller: true` marks the cell as a per-caller cell, which makes it prunable when idle; the flag is sticky until a prune or a reset.

Exports beyond the class: the constants `WAIT_CAP_MS`, `DEDUPE_TTL_MS`, `BACKOFF_MS`, `DEDUPE_BODY_CAP`, `IDLE_PRUNE_MS`, `DEFAULT_RATE`, `DEFAULT_BURST`, `HIST_BUCKETS`, `BACKOFF_STATUSES`; the pure functions `refill`, `effectiveRate`, `waitBudget`, `sanitiseRate`, `sanitiseBurst`, `newBucket`, `emptyCounters`, `histIndex`; and from `persist.ts` the `Persistence` helper (dirty flag and the one alarm), `Once` (a retrying once-promise loader), `startLoad`, `exclusive` and `commitWrites`, for a host that wants the same interval-flush discipline in another actor class. The gate uses them for its advisory rate limiter.

## Scope

What the package deliberately does not do:

- It never calls the service. Admission is decided inside the actor's serialised turn; the host sleeps until the release time and makes the call outside it, so many requests are in flight at once while order per cell stays first in, first out.
- It does not know who the caller is. Caller identity, the write fingerprint and the per-caller cell naming are the host's job; the package only binds a stored response to the fingerprint it was given.
- It does not route or front anything. Transparent proxying, upstream tables, retries and fall-through live in the host.
- It does not share fairly across many callers on its own. One cell is one bucket; the gate gets per-caller fairness by consulting a caller cell before the shared cell, with the remaining wait budget. Several moderate callers can still fill a shared cell together.
- Numbers are defaults, not policy. A stored bucket keeps its rate and burst until re-parameterised through `GET /?rate=&burst=`, so a changed default never silently rewrites a running cell.

## Status

Built and running inside the gate: the bucket, the hold cap, dedupe with replay and `dd_at` ownership, counters and backoff, the in-memory state with the one-alarm flush (measured on the host: 6.08 durable writes per request down to 0.20). Partly built: the idle prune is tested against the storage double, and its first live firing on a 24 h idle cell is still being observed.

## License

MIT. See `LICENSE`.
