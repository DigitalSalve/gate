# One alarm, not one write per request: actor state that survives load

## The idea

A rate limiter that lives in a single-threaded actor does not need to write its bucket to durable storage on every request. Keep the bucket and the counters in memory, mark them dirty, and let one alarm per actor flush the dirty state every 10 s in a single transaction. Admission is decided in memory and the actor processes one request at a time, so the rate guarantee is unchanged. What changes is the write rate to storage: from one durable write per request to one every 10 s, however many requests arrive.

## Example

The gate paces agent traffic to shared services. Its first version did the obvious thing: every admission updated the bucket and wrote it to actor storage. On a runtime whose storage appends one object per transaction to an object store and compacts lazily, that was a slow disaster.

Before: 1000 metered requests consumed 6.08 inodes each on the node's disk. At a few thousand requests an hour the node ran out of inodes in days, the runtime's own health check went red, and the proxy fell through to unmetered traffic. The gate had taken itself offline by being used.

The change, in one actor:

- state loads once, inside the runtime's startup gate, so no request runs before the bucket is in memory;
- every mutation sets a dirty flag and schedules the alarm if none is parked;
- the alarm writes every dirty key in one transaction and clears the flag;
- the dedupe table, which must be durable before the response leaves, keeps its synchronous write.

After: the same 1000 requests consumed 0.20 inodes each, a thirty-fold drop. Counters were intact across a process restart and across a full reboot of the node. A `GET /meter/<cell>` report shows the flush interval, whether state is dirty, and the last flush time, so the trade is visible to the operator.

## Why it matters

Anyone running counters, buckets or leases inside durable actors pays for every storage write, in latency and often in storage objects or log entries that someone has to compact. The lesson is that durability has a grain. Decide which state must be durable before the caller hears back, and write only that synchronously. Everything else can ride one periodic flush, and the cost of a crash is bounded by the interval: here, at most 10 s of counters, which the design accepts because the bucket refills anyway.

It also makes idle cleanup cheap. The same alarm slot, when an actor is clean and belongs to a single caller, is parked at last-touched plus 24 h; when it fires, the actor deletes its own storage. No sweeper, no idle loop, no list of actors to walk.

## Status

Built and running since the measurement above. The 24 h self-prune is tested; its first firing on the live runtime is still being observed.
