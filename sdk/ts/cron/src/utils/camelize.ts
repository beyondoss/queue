type CamelKey<S extends string> = S extends `${infer P}_${infer Q}${infer R}`
  ? `${P}${Uppercase<Q>}${CamelKey<R>}`
  : S;

export type Camelize<T> = T extends null | undefined ? T
  : T extends (infer U)[] ? Camelize<U>[]
  : T extends object
    ? { [K in keyof T as CamelKey<K & string>]: Camelize<T[K]> }
  : T;

export function camelize<T>(obj: T): Camelize<T> {
  if (obj === null || obj === undefined) return obj as Camelize<T>;
  if (Array.isArray(obj)) return obj.map(camelize) as Camelize<T>;
  if (typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj as Record<string, unknown>)) {
      const camel = key.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
      result[camel] = camelize(value);
    }
    return result as Camelize<T>;
  }
  return obj as Camelize<T>;
}

export function snakenize<T>(obj: T): unknown {
  if (obj === null || obj === undefined) return obj;
  if (Array.isArray(obj)) return obj.map(snakenize);
  if (typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj as Record<string, unknown>)) {
      const snake = key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
      result[snake] = snakenize(value);
    }
    return result;
  }
  return obj;
}
