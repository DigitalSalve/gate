import { test } from "node:test";
import assert from "node:assert/strict";

import { callerIdentity, fingerprint, readBody, toBase64, fromBase64, base64Bytes, hex } from "../src/util.ts";

const enc = new TextEncoder();
const body = (text: string) => enc.encode(text).buffer as ArrayBuffer;

function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  const queue = chunks.map((c) => enc.encode(c));
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (queue.length === 0) controller.close();
      else controller.enqueue(queue.shift() as Uint8Array);
    },
  });
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) parts.push(value);
  }
  return parts.map((p) => new TextDecoder().decode(p)).join("");
}

// --- caller identity binding (the dedupe boundary) -------------------------------------------

test("callerIdentity is a one-way digest that never carries the credential", async () => {
  const id = await callerIdentity(new Headers({ "x-api-key": "tracker-secret-key" }));
  assert.match(id, /^[0-9a-f]{64}$/);
  assert.equal(id.includes("tracker-secret-key"), false);
});

test("callerIdentity separates every credential header and treats absence as its own identity", async () => {
  const a = await callerIdentity(new Headers({ "x-api-key": "key-a" }));
  const b = await callerIdentity(new Headers({ "x-api-key": "key-b" }));
  const bearer = await callerIdentity(new Headers({ authorization: "Bearer key-a" }));
  const cookie = await callerIdentity(new Headers({ cookie: "session=key-a" }));
  const none = await callerIdentity(new Headers());

  assert.notEqual(a, b);
  assert.notEqual(a, bearer, "the same value in a different header is a different identity");
  assert.notEqual(a, cookie);
  assert.notEqual(a, none);
  assert.equal(a, await callerIdentity(new Headers({ "x-api-key": "key-a" })), "stable for the same caller");
});

test("two callers with different credentials never share a write fingerprint", async () => {
  const a = await callerIdentity(new Headers({ "x-api-key": "key-a" }));
  const b = await callerIdentity(new Headers({ "x-api-key": "key-b" }));

  const fpA = await fingerprint("POST", "/api/v1/issues/", "", body('{"name":"Task"}'), a);
  const fpB = await fingerprint("POST", "/api/v1/issues/", "", body('{"name":"Task"}'), b);
  assert.notEqual(fpA, fpB, "identical payloads from different credentials must not share a dedupe slot");

  const again = await fingerprint("POST", "/api/v1/issues/", "", body('{"name":"Task"}'), a);
  assert.equal(fpA, again, "the same caller retrying the same write must dedupe");
});

test("the fingerprint covers method, path, query and body", async () => {
  const id = await callerIdentity(new Headers({ "x-api-key": "k" }));
  const base = await fingerprint("POST", "/api/v1/issues/", "?a=1", body("x"), id);

  assert.notEqual(base, await fingerprint("PUT", "/api/v1/issues/", "?a=1", body("x"), id));
  assert.notEqual(base, await fingerprint("POST", "/api/v1/other/", "?a=1", body("x"), id));
  assert.notEqual(base, await fingerprint("POST", "/api/v1/issues/", "?a=2", body("x"), id));
  assert.notEqual(base, await fingerprint("POST", "/api/v1/issues/", "?a=1", body("y"), id));
  assert.match(base, /^[0-9a-f]{64}$/);
});

// --- byte-accurate body cap -------------------------------------------------------------------

test("a body inside the cap is buffered whole", async () => {
  const read = await readBody(streamOf(["hello ", "world"]), 1024);
  assert.equal(read.stream, null);
  assert.equal(new TextDecoder().decode(read.buffered as ArrayBuffer), "hello world");
});

test("a missing body buffers as empty rather than throwing", async () => {
  const read = await readBody(null, 1024);
  assert.equal(read.stream, null);
  assert.equal((read.buffered as ArrayBuffer).byteLength, 0);
});

test("the cap counts bytes actually read, and an oversized body still forwards intact", async () => {
  // Ten 10-byte chunks, cap 25: the cap trips on the third chunk, mid-stream.
  const chunks = Array.from({ length: 10 }, (_, i) => String(i).repeat(10));
  const read = await readBody(streamOf(chunks), 25);

  assert.equal(read.buffered, null, "past the cap nothing is held in memory for dedupe or retry");
  assert.ok(read.stream, "an oversized body must still forward");
  assert.equal(await drain(read.stream as ReadableStream<Uint8Array>), chunks.join(""), "no bytes lost at the spill point");
});

test("an under-declared Content-Length cannot defeat the cap", async () => {
  // The header lies; readBody never reads it, so the cap still trips on real bytes.
  const request = new Request("http://gate/via/tracker/x", {
    method: "POST",
    headers: { "content-length": "5" },
    body: "x".repeat(500),
  });
  const read = await readBody(request.body, 100);
  assert.equal(read.buffered, null);
  assert.ok(read.stream);
  assert.equal((await drain(read.stream as ReadableStream<Uint8Array>)).length, 500);
});

// --- base64 helpers ---------------------------------------------------------------------------

test("base64 round-trips arbitrary bytes and reports its decoded size", () => {
  const bytes = new Uint8Array([0, 1, 127, 128, 255, 65, 66]);
  const b64 = toBase64(bytes);
  assert.deepEqual([...fromBase64(b64)], [...bytes]);
  assert.equal(base64Bytes(b64), bytes.length);
  assert.equal(base64Bytes(""), 0);
  assert.equal(base64Bytes(toBase64(new Uint8Array(300 * 1024))), 300 * 1024);
});

test("hex pads every byte", () => {
  assert.equal(hex(new Uint8Array([0, 15, 16, 255])), "000f10ff");
});
