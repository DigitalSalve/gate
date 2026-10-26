# Hold, never drop: pacing agent traffic to shared services

## Summary

Autonomous agents burst requests at shared internal services far faster than those services tolerate. The usual answer, a rate limiter that drops, just moves the burst to the retry. The gate takes the other path: it holds. A reserve-ahead token bucket in a single-threaded actor hands each request a release time instead of a verdict, callers wait on the gate, and the service sees a steady rate. Per-caller cells ahead of the shared cell keep one flooder from starving the rest, a caller-bound write fingerprint answers duplicate writes once, state lives in memory with one periodic flush, and the proxy's active health check makes the whole layer fail open. It runs today on self-hosted edge functions and durable actors, in front of an issue tracker, a secrets manager and a CI farm.

## The problem

A fleet of agents does not behave like a fleet of users. One agent retries a timed-out write immediately. Ten agents working the same backlog hit the tracker in the same second. A planner fans out fifty reads before it decides anything. The services behind them, mostly small self-hosted tools with hard per-key limits, answered with 429s or fell over, and every agent grew its own backoff logic, each slightly different and none aware of the others.

Three things were wrong at once. There was no single point that knew the service's real capacity. Rejection taught agents to retry, which recreated the burst a second later. And a single busy caller could consume the whole budget while quiet callers waited behind it.

## The idea

- A token bucket that returns a release time instead of rejecting lets callers wait while the upstream sees a steady rate.
- Consulting a per-caller cell before spending a shared token stops one flooding caller from starving the rest.
- Binding a write fingerprint to the caller identity makes a stored first response safe to replay, and only to the caller that produced it.
- Keeping bucket state in actor memory with a periodic flush removes the per-request durable write without weakening the guarantee.
- An active proxy health check against the node, not the meter, makes a metering layer fail open without disabling it under load.

The reasoning behind the first claim carries the rest. A classic bucket refuses when empty. A reserve-ahead bucket lets the balance go negative and computes how far in the future the balance would be non-negative again; that instant is the release time. Because one single-threaded actor owns each bucket, admissions are serialised and release times are monotonic per cell. The arrival pattern, however bursty, is converted into a schedule at the configured rate. A cap of 30 s bounds the schedule: past it the gate answers 503 with Retry-After, so the agent gets a real signal instead of a stall, and the queue depth per cell is bounded by rate times 30 s.

## How it works

1. The reverse proxy lists the gate first and the service second for the metered paths, and names the upstream in a request header. The proxy also runs an active health check against the runtime's own health endpoint, and its per-request header timeout is set above the hold cap.
2. The gate's edge function buffers the body for writes, hashes the credential header into a caller identity, and fingerprints writes with method, path, body and caller.
3. For a per-caller service it asks the caller's own actor for a release time first, then the service's shared actor with whatever remains of the 30 s budget. For a shared-only service it asks the shared actor once. Each actor runs the reserve-ahead bucket with a monotonic clock, so out-of-order arrivals cannot over-credit tokens.
4. The edge function sleeps until the release time, checks that the caller is still connected, forwards the original path to the configured target, retries once on a transient upstream error while halving the refill rate for a short backoff, and records the outcome. Non-idempotent writes are never replayed on a 502 or 504.
5. Identical writes from the same caller inside 60 s are answered from the stored first response with a dedupe header. For a secrets service only the status is stored, never the body.
6. Bucket and counter state live in actor memory. One alarm per actor flushes dirty state every 10 s in a single transaction. A clean per-caller actor parks the same alarm at last-touched plus 24 h and deletes its own storage when it fires. Shared actors are never pruned.
7. From the caller's side nothing changes: same hostname, same credentials, no client library. Their contract is three lines: keep the client timeout above 30 s, obey Retry-After on a 503, and expect an identical write resent inside a minute to be answered rather than sent again.

Admission happens inside the actor's turn; the upstream call happens outside it. Many forwards are in flight at once while order per cell stays first-in first-out, and the actor is never blocked on a slow service.

## Examples

A caller with one API key sends 40 requests at once to the tracker. The first 10 are forwarded immediately, the remaining 30 are released at 4 per second, and each response names the cell that paced it. The tracker never sees more than its own per-key limit. A second caller's requests pass its own cell at once and take shared tokens with the full budget instead of waiting behind the flood.

The same POST sent twice within a minute by one key returns the first response with a dedupe header; the tracker holds one record. The same body from a different key is forwarded as a new request.

With the gate process stopped entirely, 40 of 40 requests still returned 200 directly from the services, worst case 0.155 s. With the gate hung rather than dead, the proxy's active check failed it within 8 s and traffic flowed unmetered until it recovered.

Persistence, before and after the in-memory change: 1000 metered requests cost 6.08 durable writes per request, then 0.20. Counters were intact across a process restart and a node reboot.

## Trade-offs

Holding costs latency and connections. A held request occupies a proxy connection and a caller's thread for up to 30 s. Clients whose timeout is shorter than the cap waste their place in line; the gate checks for an abandoned caller at release and does not forward on its behalf, but the caller still gets nothing.

Fail-open means that when the gate is down, traffic is unmetered. That is the deliberate choice: the worst case is a busy service, never an unreachable one.

In-memory state means a crash loses up to 10 s of counters. The bucket refills anyway, so the rate guarantee survives; only the numbers in the report are approximate after a crash.

It is not a rate limiter that drops. It is not an API gateway or auth layer: it never inspects credentials beyond hashing them. It is not a cache: dedupe replays only an identical write from the same caller inside a short window. It is not a message bus or claims system; pacing and claims are deliberately separate pieces. It is not deployed on a hosted edge platform: same programming model, own hardware.

## Status

Built and running: the metering proxy with transparent mode, the reserve-ahead bucket with hold cap and Retry-After, caller-bound write dedupe with replay, single retry with refill-halving backoff, two-level admission, in-memory state with the 10 s flush, operational counters with a persistence and idle report, three upstreams, proxy active health fall-through, and alerts on shedding, node health and disk inodes.

Partly built: the per-caller cells stop one flooder, but several moderate callers can together fill the shared cell; fair sharing across callers needs a weighted or round-robin draw that does not exist yet. Caller identity is a hash of the credential header, so a caller that rotates an unrelated header earns a fresh cell. The 24 h self-prune is tested; its first live firing is still being observed.

Ideas: an advisory pre-call hook, so an agent can ask for a slot and do other work until it comes; and caller identity from a per-lane token instead of a hash.

## Credits

Built on an open-source, self-hosted runtime that implements the edge-function and durable-actor programming model. The reserve-ahead bucket is a token bucket allowed to go negative; the idea is old, the application to agents is ours.
