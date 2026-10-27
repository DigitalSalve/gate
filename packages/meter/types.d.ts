// Ambient types for the durable actor storage API the meter uses. Types are stripped at
// test and bundle time, never checked at runtime; these exist for editors and `tsc --noEmit`.

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
