// Minimal ambient types for the the actor runtime / edge functions runtime.
// the hosted platform's type package is deliberately NOT a dependency: the node bundles with esbuild 0.28
// (types are stripped, never checked) and the test runner uses node's native type stripping.
// These declarations exist so editors and `tsc --noEmit` have something to bind to.

declare interface DurableObjectStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T = unknown>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  deleteAll(): Promise<void>;
  list<T = unknown>(options?: { prefix?: string; limit?: number; reverse?: boolean }): Promise<Map<string, T>>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  getAlarm(): Promise<number | null>;
  deleteAlarm(): Promise<void>;
  transaction?<T>(fn: (txn: DurableObjectTransaction) => Promise<T>): Promise<T>;
}

declare interface DurableObjectTransaction {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T = unknown>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
}

declare interface DurableObjectState {
  storage: DurableObjectStorage;
  blockConcurrencyWhile?<T>(fn: () => Promise<T>): Promise<T>;
}

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
