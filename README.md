# gate

A hold-never-drop traffic governor for shared internal services, run as edge functions and durable actors on your own hardware.

## What it does

Agents and batch jobs burst requests at shared services faster than those services tolerate. The gate sits behind your reverse proxy, in front of each service, and paces the traffic: callers wait for a slot instead of being rejected, one caller cannot starve the rest, an identical write resent inside a minute is answered once, and if the gate itself dies the proxy sends traffic straight to the service. Clients do not change. They call the same hostname with the same credentials and see one extra response header naming the cell that paced them.

What a caller gets:

- a response, delayed up to 30 s, when the service is busy;
- a 503 with Retry-After when the wait would exceed 30 s;
- the first response again, with a dedupe header, when it resends the same write inside 60 s.

## Quick start

```bash
# install and run
npm install            # one dev dependency, the bundler
npm test               # 92 tests, node >= 22.6, no framework; packages/meter has its own npm test

# edit wrangler.jsonc: one entry per upstream in vars.UPSTREAMS
#   target        where the service lives
#   rate, burst   shared cell: tokens per second and bucket size
#   caller_rate, caller_burst   optional per-caller cells (one per credential)
#   dedupe, store_bodies        write dedupe; never store bodies for a secrets service

# deploy to your runtime from this directory with its deploy command, then:
curl -s http://<gate>:8080/health
curl -s 'http://<gate>:8080/meter/<cell>'        # bucket, counters, persistence, idle
```

The reverse proxy lists the gate first and the service second for the metered paths, adds a header naming the upstream (`X-Gate-Upstream: <key>`), and runs an active health check against the runtime's own health endpoint with a per-request header timeout longer than 30 s. The gate forwards the caller's original path to the configured target.

## How it works

1. The edge function reads the upstream header, buffers the body for writes, and derives a caller identity from a hash of the credential header.
2. It asks the caller's own actor for a release time, then the service's shared actor, with whatever is left of the 30 s budget. Each actor runs a reserve-ahead token bucket: the balance may go negative, and the request is scheduled that far out.
3. It sleeps until the release time, forwards, retries once on a transient upstream error while halving the refill rate for a short backoff, and records the outcome.
4. Identical writes from the same caller inside 60 s are answered from the stored first response.
5. Bucket and counter state live in actor memory; one alarm per actor flushes dirty state every 10 s in a single transaction. Idle per-caller actors prune themselves after 24 h.

Source layout: `src/index.ts` (entry, routing, upstream table, two-level admission), `src/headers.ts`, `src/util.ts`, and `test/` for each of them. The pacing core is its own package, `packages/meter` (the bucket, counters, dedupe, the memory state with its flush alarm, and the idle prune, with its own tests and README), and the gate imports it from there; it can be installed on its own.

## Status

Built and running: everything in How it works, three upstreams configured, proxy fall-through measured (gate stopped: 40 of 40 requests still answered directly, worst case 0.155 s; gate hung: failover within 8 s), persistence measured (6.08 durable writes per request down to 0.20).

Partly built: the per-caller cells stop one flooding caller, but several moderate callers can together fill the shared cell; fair sharing across callers is not implemented. Caller identity is a hash of the credential header, so rotating an unrelated header earns a fresh caller cell. The 24 h prune is tested but its first live firing is still being observed.

Ideas: an advisory pre-call hook so an agent can ask for a slot before it calls, and caller identity from a per-lane token instead of a hash.

## The thinking behind this

- [Hold, never drop: pacing agent traffic to shared services](https://gist.github.com/dustintaylored/45a636576e99de46493b4d13a37ad60e), the whitepaper.
- [One alarm, not one write per request](https://gist.github.com/dustintaylored/6147b5ecf0c696326ce169b7ed9d685b), the short version of the persistence lesson.

## About this repository

This public repository is a generated export of a private source: the code and the tests are copied out by a declared rule set with private material stripped, and nothing here is edited by hand. A pull request is answered by a change in the source and a fresh export, so its diff is welcome and its commits are not kept. The papers live only on their gists, linked below; the repository keeps no copies.

## Credits

Built on an open-source, self-hosted runtime that implements the edge-function and durable-actor programming model, and a bundler. Both are named in `package.json`.

## License

MIT. See `LICENSE`.

---

Root: agent-infrastructure · Type: build
