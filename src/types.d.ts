// Minimal ambient types for the the actor runtime / edge functions runtime.
// the hosted platform's type package is deliberately NOT a dependency: the node bundles with esbuild 0.28
// (types are stripped, never checked) and the test runner uses node's native type stripping.
// These declarations exist so editors and `tsc --noEmit` have something to bind to.

/// <reference path="../packages/meter/types.d.ts" />

declare interface DurableObjectId {
  toString(): string;
}

declare interface DurableObjectStub {
  fetch(input: Request | string, init?: RequestInit): Promise<Response>;
}

declare interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  get(id: DurableObjectId): DurableObjectStub;
}

declare interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
  passThroughOnException?(): void;
}
