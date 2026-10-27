// Small runtime helpers shared by the Worker and the Meter DO.
// No node built-ins: this bundles for the actor runtime, where only Web APIs exist.

/** Headers that identify who is calling. A dedupe entry must never cross one of these. */
const IDENTITY_HEADERS = ["authorization", "x-api-key", "cookie"] as const;

/**
 * One-way digest of the caller's credentials. Feeds the write fingerprint so a stored response
 * can only ever be replayed to the identity that produced it. The credential itself is never
 * stored: only this digest reaches the fingerprint, and only the fingerprint reaches storage.
 */
export async function callerIdentity(headers: Headers): Promise<string> {
  const material = IDENTITY_HEADERS.map((name) => headers.get(name) ?? "").join("\u0000");
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(material));
  return hex(new Uint8Array(digest));
}

/**
 * Write fingerprint: sha256 hex over caller identity, method, path, query and body.
 * Two writes with the same fingerprint inside the dedupe window are the same write retried by the
 * same caller, so the second is answered from the stored response instead of hitting the upstream
 * twice. Identity is part of the input on purpose: without it, two agents holding different
 * credentials that happen to send the same payload to the same path would share a dedupe slot,
 * and one would be handed the other's response while its own write was silently dropped.
 */
export async function fingerprint(
  method: string,
  pathname: string,
  search: string,
  body: ArrayBuffer | null,
  identity: string,
): Promise<string> {
  const prefix = new TextEncoder().encode(`${identity}\n${method.toUpperCase()}\n${pathname}\n${search}\n`);
  const bodyBytes = body ? new Uint8Array(body) : new Uint8Array(0);
  const buf = new Uint8Array(prefix.length + bodyBytes.length);
  buf.set(prefix, 0);
  buf.set(bodyBytes, prefix.length);
  const digest = await crypto.subtle.digest("SHA-256", buf);
  return hex(new Uint8Array(digest));
}

export function hex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

/** base64 of arbitrary bytes, chunked so a large body cannot blow the argument limit. */
export function toBase64(bytes: Uint8Array): string {
  let binary = "";
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

export function fromBase64(b64: string): Uint8Array {
  const binary = atob(b64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

export { base64Bytes } from "../packages/meter/util.ts";

export interface ReadBodyResult {
  /** Whole body, when it fitted inside the cap. Dedupe and the single retry need this. */
  buffered: ArrayBuffer | null;
  /** Re-assembled stream, when the body was larger than the cap. No dedupe, no retry. */
  stream: ReadableStream<Uint8Array> | null;
}

/**
 * Read a request body into memory, bounded by `cap` actual bytes.
 *
 * The cap is enforced against bytes really read, never against the caller's Content-Length:
 * a chunked or under-declared body would otherwise sail past a header-based check and buffer
 * without limit. Past the cap the already-read chunks are pushed back in front of the remaining
 * reader, so an oversized body still forwards - it just streams, and gives up dedupe and retry.
 */
export async function readBody(body: ReadableStream<Uint8Array> | null, cap: number): Promise<ReadBodyResult> {
  if (!body) return { buffered: new ArrayBuffer(0), stream: null };
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;

  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      const out = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        out.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return { buffered: out.buffer as ArrayBuffer, stream: null };
    }
    if (!value) continue;
    chunks.push(value);
    total += value.byteLength;
    if (total > cap) return { buffered: null, stream: rejoin(chunks, reader) };
  }
}

/** Emit the chunks already read, then the rest of the reader. the actor runtime lacks ReadableStream.from(). */
function rejoin(chunks: Uint8Array[], reader: ReadableStreamDefaultReader<Uint8Array>): ReadableStream<Uint8Array> {
  const pending = chunks.slice();
  chunks.length = 0;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (pending.length > 0) {
        controller.enqueue(pending.shift() as Uint8Array);
        return;
      }
      const { done, value } = await reader.read();
      if (done) controller.close();
      else if (value) controller.enqueue(value);
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
}

/** Resolve after `ms`, or immediately when `signal` aborts. Never rejects. */
export function sleep(ms: number, signal?: AbortSignal | null): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise<void>((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      // the actor runtime: signal.onabort has no effect; addEventListener is the supported form.
      try {
        signal?.removeEventListener("abort", finish);
      } catch {
        /* signal may not support removal */
      }
      resolve();
    };
    const timer = setTimeout(finish, ms);
    try {
      signal?.addEventListener("abort", finish, { once: true });
    } catch {
      /* no signal support: fall back to the timer alone */
    }
  });
}
