import createDebug from 'debug'
import { isPromise } from 'node:util/types'
import {
  Connection,
  ConnectionStatus,
  type QueryHook,
  type Result,
  type Row,
  type SQLTemplate,
} from 'pg-native'
import { sleep } from 'radashi'
import { Query, type QueryOptions } from './query'

const debug = /** @__PURE__ */ createDebug('pg-nano')

export interface ClientOptions {
  /**
   * The minimum number of connections to maintain in the pool.
   * @default 1
   */
  minConnections: number

  /**
   * The maximum number of connections allowed in the pool.
   * @default 100
   */
  maxConnections: number

  /**
   * The initial delay (in milliseconds) before retrying a failed connection.
   * @default 250
   */
  initialRetryDelay: number

  /**
   * The maximum delay (in milliseconds) between connection retry attempts.
   * @default 10000
   */
  maxRetryDelay: number

  /**
   * The maximum number of times to retry connecting before giving up.
   * @default Number.POSITIVE_INFINITY
   */
  maxRetries: number

  /**
   * The time (in milliseconds) after which an idle connection is closed.
   * @default 30000
   */
  idleTimeout: number
}

/**
 * A minimal connection pool for Postgres.
 *
 * Queries are both promises and async iterables.
 *
 * Note that `maxConnections` defaults to 100, which assumes you only have one
 * application server. If you have multiple application servers, you probably
 * want to lower this value by dividing it by the number of application servers.
 */
export class Client {
  protected pool: (Connection | Promise<Connection>)[] = []
  protected backlog: ((err?: Error) => void)[] = []

  readonly dsn: string | null = null
  readonly config: Readonly<ClientOptions>

  constructor({
    minConnections = 1,
    maxConnections = 100,
    initialRetryDelay = 250,
    maxRetryDelay = 10e3,
    maxRetries = Number.POSITIVE_INFINITY,
    idleTimeout = 30e3,
  }: Partial<ClientOptions> = {}) {
    this.config = {
      minConnections,
      maxConnections,
      initialRetryDelay,
      maxRetryDelay,
      maxRetries,
      idleTimeout,
    }
  }

  protected async connectWithRetry(
    connection: Connection,
    signal?: AbortSignal,
    retries = Math.max(this.config.maxRetries, 0),
    delay = Math.max(this.config.initialRetryDelay, 0),
  ): Promise<void> {
    if (!this.dsn) {
      throw new Error('Postgres is not connected')
    }
    signal?.throwIfAborted()
    try {
      await connection.connect(this.dsn)
    } catch (error) {
      if (retries > 0) {
        signal?.throwIfAborted()

        if (delay > 0) {
          await sleep(delay)
        }
        return this.connectWithRetry(
          connection,
          signal,
          retries - 1,
          Math.min(delay * 2, this.config.maxRetryDelay),
        )
      }
      throw error
    }
  }

  protected addConnection(
    signal?: AbortSignal,
    idleTimeout = this.config.idleTimeout,
  ): Promise<Connection> {
    const connection = new Connection(idleTimeout)

    const connecting = this.connectWithRetry(connection, signal).then(
      () => {
        const index = this.pool.indexOf(connecting)
        this.pool[index] = connection

        connection.on('close', () => {
          this.removeConnection(connection)
        })

        return connection
      },
      error => {
        this.removeConnection(connecting)
        throw error
      },
    )

    this.pool.push(connecting)

    if (process.env.NODE_ENV !== 'production' && debug.enabled) {
      const index = this.pool.indexOf(connecting)
      connecting.then(() => {
        if (index === this.pool.length - 1) {
          debug(
            `open connections: ${this.pool.length} of ${this.config.maxConnections}`,
          )
        }
      })
    }

    return connecting
  }

  protected removeConnection(connection: Connection | Promise<Connection>) {
    const index = this.pool.indexOf(connection)
    if (index !== -1) {
      this.pool.splice(index, 1)

      if (process.env.NODE_ENV !== 'production' && debug.enabled) {
        const poolSize = this.pool.length
        setImmediate(() => {
          if (poolSize === this.pool.length) {
            debug(
              `open connections: ${poolSize} of ${this.config.maxConnections}`,
            )
          }
        })
      }
    }
  }

  protected getConnection(
    signal?: AbortSignal,
  ): Connection | Promise<Connection> {
    const idleConnection = this.pool.find(
      conn => !isPromise(conn) && conn.status === ConnectionStatus.IDLE,
    )
    if (idleConnection) {
      return idleConnection
    }
    if (this.pool.length < this.config.maxConnections) {
      return this.addConnection(signal)
    }
    return new Promise((resolve, reject) => {
      signal?.throwIfAborted()
      this.backlog.push(err => {
        if (err) {
          reject(err)
        } else {
          resolve(this.getConnection(signal))
        }
      })
    })
  }

  /**
   * Connects to the database and initializes the connection pool.
   */
  async connect(dsn: string, signal?: AbortSignal) {
    if (this.dsn != null) {
      throw new Error('Postgres is already connected')
    }
    this.setDSN(dsn)
    if (this.config.minConnections > 0) {
      const firstConnection = this.addConnection(
        signal,
        Number.POSITIVE_INFINITY,
      )
      for (let i = 0; i < this.config.minConnections - 1; i++) {
        this.addConnection(signal, Number.POSITIVE_INFINITY)
      }
      await firstConnection
    }
    return this
  }

  /**
   * Execute one or more commands.
   */
  query<TRow extends Row = Row, TIteratorResult = Result<TRow>>(
    sql: SQLTemplate,
    transform?: (result: Result<TRow>) => TIteratorResult | TIteratorResult[],
  ) {
    return new Query<Result<TRow>[], TIteratorResult>(this, sql, transform)
  }

  protected async dispatchQuery<
    TRow extends Row = Row,
    TResult = Result<TRow>[],
  >(
    connection: Connection | Promise<Connection>,
    sql: SQLTemplate | QueryHook<TResult>,
    signal?: AbortSignal,
    singleRowMode?: boolean,
  ): Promise<TResult> {
    signal?.throwIfAborted()

    if (isPromise(connection)) {
      // Only await the connection if necessary, so the connection status can
      // change to QUERY_WRITING as soon as possible.
      connection = await connection
    }

    try {
      signal?.throwIfAborted()

      const queryPromise = connection.query(sql, singleRowMode)

      if (signal) {
        const cancel = () => connection.cancel()
        signal.addEventListener('abort', cancel)
        queryPromise.finally(() => {
          signal.removeEventListener('abort', cancel)
        })
      }

      return await queryPromise
    } finally {
      this.backlog.shift()?.()
    }
  }

  /**
   * Executes a query and returns an array of rows. This assumes only one
   * command exists in the given query.
   *
   * You may define the row type using generics.
   */
  many<TRow extends Row>(
    sql: SQLTemplate,
    options?: QueryOptions,
  ): Query<TRow[]> {
    const query = this.query<TRow, TRow>(sql, result => result.rows)
    return query.withOptions({
      ...options,
      resolve: ([result]) => result.rows,
    }) as any
  }

  /**
   * Executes a query and returns a single row. This assumes only one command
   * exists in the given query. If you don't limit the results, the promise will
   * be rejected when more than one row is received.
   *
   * You may define the row type using generics.
   */
  async one<TRow extends Row>(
    sql: SQLTemplate,
    options?: QueryOptions,
  ): Promise<TRow | undefined> {
    const [result] = await this.query<TRow>(sql).withOptions(options)
    if (result.rows.length > 1) {
      throw new Error('Expected 1 row, got ' + result.rows.length)
    }
    return result.rows[0]
  }

  /**
   * Execute a single command that returns a single row with a single value.
   */
  async scalar<T>(sql: SQLTemplate, options?: QueryOptions): Promise<T> {
    const results = await this.query(sql).withOptions(options)
    const row = results[0].rows[0]
    for (const key in row) {
      return row[key] as T
    }
    // Should be unreachable.
    return undefined!
  }

  proxy<API extends object>(api: API): ClientProxy<API> {
    return new Proxy(this, {
      get(client, key) {
        if (key in api) {
          // biome-ignore lint/complexity/noBannedTypes:
          return (api[key as keyof API] as Function).bind(null, client)
        }
        return client[key as keyof Client]
      },
    }) as any
  }

  /**
   * Closes all connections in the pool.
   */
  async close() {
    if (this.dsn == null) {
      return
    }
    this.setDSN(null)
    const closing = Promise.all(
      this.pool.map(connection =>
        isPromise(connection)
          ? connection.then(c => c.close())
          : connection.close(),
      ),
    )
    this.pool = []
    if (this.backlog.length > 0) {
      const error = new Error('Postgres client was closed')
      this.backlog.forEach(fn => fn(error))
      this.backlog = []
    }
    await closing
  }

  private setDSN(dsn: string | null) {
    ;(this as { dsn: string | null }).dsn = dsn
  }
}

export type ClientProxy<API extends object> = {
  [K in keyof API]: API[K] extends (
    client: Client,
    ...args: infer TArgs
  ) => infer TResult
    ? (...args: TArgs) => TResult
    : never
} & {
  readonly dsn: string | null
  readonly config: Readonly<ClientOptions>
  query: Client['query']
  many: Client['many']
  one: Client['one']
  scalar: Client['scalar']
  close: Client['close']
}
