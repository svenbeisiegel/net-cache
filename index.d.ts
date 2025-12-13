import { Socket, Server } from 'net';

export interface NetCacheServerOptions {
  /** Maximum lifetime of each cache entry in milliseconds (default: 60000) */
  maxLifetimeMs?: number;
  /** TCP port to listen on (default: 11211) */
  port?: number;
}

export interface NetCacheClientOptions {
  /** Server host (default: '127.0.0.1') */
  host?: string;
  /** Server port (default: 11211) */
  port?: number;
  /** Delay before reconnect attempt in milliseconds (default: 1000) */
  reconnectDelayMs?: number;
}

export class NetCacheServer {
  readonly maxLifetimeMs: number;
  readonly port: number;
  readonly cache: Map<string, { value: Buffer; timer: NodeJS.Timeout }>;
  readonly server: Server;

  constructor(options?: NetCacheServerOptions);

  /** Start listening on the configured port */
  start(): Promise<void>;

  /** Stop the server and clear the cache */
  stop(): Promise<void>;
}

export class NetCacheClient {
  readonly host: string;
  readonly port: number;
  readonly reconnectDelayMs: number;
  readonly socket: Socket | null;
  readonly connected: boolean;

  constructor(options?: NetCacheClientOptions);

  /** Connect to the server */
  connect(): Promise<void>;

  /** Disconnect from the server */
  disconnect(): void;

  /**
   * Write a key-value pair to the cache
   * @returns true on success
   * @throws Error on failure
   */
  write(key: string, value: Buffer | string): Promise<boolean>;

  /**
   * Read a value from the cache
   * @returns Buffer if found, null if key not found
   * @throws Error on other failures
   */
  read(key: string): Promise<Buffer | null>;

  /**
   * Read and delete a value from the cache
   * @returns Buffer if found, null if key not found
   * @throws Error on other failures
   */
  readAndDelete(key: string): Promise<Buffer | null>;
}
