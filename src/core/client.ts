import {
  baseTypeParsers,
  Connection,
  ConnectionStatus,
  createTextParser,
  hashSessionParameters,
  parseConnectionString,
  QueryType,
  renderTemplateValue,
  stringifyConnectOptions,
  type CommandResult,
  type ConnectOptions,
  type QueryHook,
  type Row,
  type SessionParameters,
  type SQLTemplate,
  type SQLTemplateValue,
  type TextParser,
} from 'pg-native'
import { isString, noop, shake, sleep } from 'radashi'
import { FieldCase } from './casing.js'
import { importCustomTypeParsers } from './data/composite.js'
import { debug } from './debug.js'
import { ConnectionError } from './error.js'
import { Query, QueryResultCount } from './query.js'

export interface ClientConfig {
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

  /**
   * Fixes the casing of field names in generated types.
   *
   * - `camel` will convert snake case field names to camel case.
   * - `preserve` will leave field names as is.
   *
   * This should match the `generate.fieldCase` option in your pg-nano config.
   *
   * @default FieldCase.camel
   */
  fieldCase: FieldCase

  /**
   * Sets session parameters for each connection, immediately after it is
   * established, before any queries are run.
   */
  sessionParams: SessionParameters

  /**
   * Text parsers for custom types. This can be used to override or extend the
   * default text parsers. Note that pg-nano will automatically generate type
   * parsers for certain custom types discovered through introspection, such as
   * user-defined composite types.
   */
  textParsers: Record<number, TextParser> | null

  /**
   * Pre-allocate an `Error` for each query, thereby capturing a stack trace
   * from where the query was constructed. This is useful when an error isn't
   * providing an actionable stack trace, but it's not recommended for
   * production due to performance impact.
   *
   * @default false
   */
  debug: boolean
}

/**
 * A connection pool for Postgres, powered by `libpq`.
 *
 * Queries are both promises and async iterables.
 *
 * Note that `maxConnections` defaults to 100, which assumes you only have one
 * application server. If you have multiple application servers, you probably
 * want to lower this value by dividing it by the number of application servers.
 */
export class Client {
  /** All connections that are being established. */
  protected connecting: Promise<Connection>[] = []
  /** Up to `config.maxConnections` connections are maintained in the pool. */
  protected connected: Connection[] = []
  /** A queue of queries waiting for a connection. */
  protected backlog: ((err?: Error) => void)[] = []
  /** Any initialization work that needs to be done before queries can be run. */
  protected initPromise: Promise<void> | null = null
  /**
   * Parse the text representation of a Postgres value. This function is
   * generated at runtime according to the target database, whose composite type
   * OIDs cannot be known at compile-time.
   */
  protected parseText: ((value: string, dataTypeID: number) => unknown) | null =
    null

  dsn: string | null = null
  readonly config: Readonly<ClientConfig>
  readonly sessionHash: string

  /** The total number of connections, both connected and connecting. */
  get numConnections() {
    return this.connected.length + this.connecting.length
  }

  constructor({
    minConnections = 1,
    maxConnections = 100,
    initialRetryDelay = 250,
    maxRetryDelay = 10e3,
    maxRetries = Number.POSITIVE_INFINITY,
    idleTimeout = 30e3,
    fieldCase = FieldCase.camel,
    sessionParams,
    textParsers = null,
    debug = false,
  }: Partial<ClientConfig> = {}) {
    this.config = {
      minConnections,
      maxConnections,
      initialRetryDelay,
      maxRetryDelay,
      maxRetries,
      idleTimeout,
      fieldCase,
      sessionParams: sessionParams ? shake(sessionParams) : {},
      textParsers,
      debug,
    }
    this.sessionHash = hashSessionParameters(this.config.sessionParams)
  }

  /**
   * Derive a new `Client` instance by merging the given session parameters with
   * the current instance's session parameters. You can “unset” session parameters
   * by setting them to `undefined`.
   *
   * The new client reuses the connection pool of the current instance.
   */
  extend(sessionParams: SessionParameters) {
    const client = Object.create(this) as Client as {
      config: ClientConfig
      sessionHash: string
    }
    client.config = {
      ...this.config,
      sessionParams: shake({
        ...this.config.sessionParams,
        ...sessionParams,
      }),
    }
    client.sessionHash = hashSessionParameters(client.config.sessionParams)
    return client as Client
  }

  /**
   * Create a proxy object that allows you to call routines from the given
   * schema object as methods on the client instance. The original methods and
   * properties of the client are preserved, but routines of the same name take
   * precedence over them.
   *
   * The easiest way to use `withSchema` is by importing your `sql/schema.ts`
   * file as a namespace and passing it to this method.
   *
   * @example
   * ```ts
   * import * as schema from './sql/schema.js'
   * const client = new Client().withSchema(schema)
   * await client.myPostgresFunc(1, 2, 3)
   * ```
   */
  withSchema<TSchema extends object>(schema: TSchema): ClientProxy<TSchema> {
    return new Proxy(this, {
      get(client, key) {
        if (key in schema) {
          return (schema[key as keyof TSchema] as Function).bind(null, client)
        }
        return client[key as keyof Client]
      },
    }) as any
  }

  protected async connectWithRetry(
    connection: Connection,
    triesRemaining: number,
    signal?: AbortSignal,
    delay = Math.max(this.config.initialRetryDelay, 0),
  ): Promise<string> {
    const { dsn } = this
    if (dsn == null) {
      throw new ConnectionError('Postgres client was closed')
    }
    signal?.throwIfAborted()
    try {
      await connection.connect(dsn, this.config.sessionParams)
      return dsn
    } catch (error) {
      if (triesRemaining > 0) {
        signal?.throwIfAborted()

        if (delay > 0) {
          await sleep(delay)
        }
        if (this.numConnections >= this.config.maxConnections) {
          throw error
        }
        return this.connectWithRetry(
          connection,
          triesRemaining - 1,
          signal,
          Math.min(delay * 2, this.config.maxRetryDelay),
        )
      }
      throw error
    }
  }

  /**
   * Perform any initialization work specific to the target database before
   * queries can be executed. Currently, this involves generating type parsers
   * for custom types discovered by introspection, whose type OIDs can't be
   * known at compile-time (i.e. they are non-deterministic).
   */
  protected async init(dsn: string, connection: Connection) {
    const {
      host = process.env.PGHOST ?? 'localhost',
      port = process.env.PGPORT ?? 5432,
      dbname = process.env.PGDATABASE ?? 'postgres',
    } = parseConnectionString(dsn)

    let customTypeParsers: Record<number, TextParser> | null = null

    // Assume no custom types have been created in the `postgres` database.
    if (dbname !== 'postgres') {
      customTypeParsers = (
        await importCustomTypeParsers(connection, host, port, dbname)
      ).default

      if (dsn !== this.dsn) {
        return // Bail if the connection string has changed.
      }
    }

    this.parseText = createTextParser({
      ...baseTypeParsers,
      ...customTypeParsers,
      ...this.config.textParsers,
    })
  }

  protected addConnection(
    signal?: AbortSignal,
    idleTimeout = this.config.idleTimeout,
    maxRetries = Math.max(this.config.maxRetries, 0),
  ): Promise<Connection> {
    const connection = new Connection(idleTimeout)

    const connecting = this.connectWithRetry(connection, maxRetries, signal)
      .then(async dsn => {
        if (!this.parseText) {
          await (this.initPromise ??= this.init(dsn, connection).finally(() => {
            this.initPromise = null
          }))
        }

        const index = this.connecting.indexOf(connecting)
        this.connecting.splice(index, 1)

        connection.on('close', () => {
          this.removeConnection(connection)
        })

        return connection
      })
      .catch(error => {
        const index = this.connecting.indexOf(connecting)
        this.connecting.splice(index, 1)

        throw error
      })

    this.connecting.push(connecting)

    // Once the connection is established, log the number of open connections if
    // debug logs are enabled.
    if (process.env.NODE_ENV !== 'production' && debug.enabled) {
      const count = this.connected.length
      connecting.then(
        () =>
          count === this.connected.length &&
          debug(
            `open connections: ${this.connected.length} of ${this.config.maxConnections}`,
          ),
      )
    }

    return connecting
  }

  protected removeConnection(connection: Connection) {
    const index = this.connected.indexOf(connection)
    if (index !== -1) {
      this.connected.splice(index, 1)

      // Log the number of open connections if debug logs are enabled.
      if (process.env.NODE_ENV !== 'production' && debug.enabled) {
        const count = this.connected.length
        setImmediate(
          () =>
            count === this.connected.length &&
            debug(
              `open connections: ${this.connected.length} of ${this.config.maxConnections}`,
            ),
        )
      }
    }
  }

  protected async getConnection(signal?: AbortSignal): Promise<Connection> {
    signal?.throwIfAborted()

    let conn = this.connected.find(
      conn =>
        conn.status === ConnectionStatus.IDLE &&
        conn.sessionHash === this.sessionHash,
    )
    if (conn) {
      conn.status = ConnectionStatus.RESERVED
      return conn
    }

    if (this.numConnections < this.config.maxConnections) {
      conn = await this.addConnection(signal)
      conn.status = ConnectionStatus.RESERVED
      return conn
    }

    return new Promise((resolve, reject) => {
      this.backlog.push(err => {
        if (err) {
          reject(err)
        } else {
          resolve(this.getConnection(signal))
        }
      })
    })
  }

  // Signals an idle connection.
  protected onQueryFinished() {
    this.backlog.shift()?.()
  }

  /**
   * Connects to the database and initializes the connection pool.
   */
  async connect(target: string | ConnectOptions, signal?: AbortSignal) {
    if (this.dsn != null) {
      throw new ConnectionError('Postgres client is already connected')
    }
    this.dsn = isString(target) ? target : stringifyConnectOptions(target)
    if (this.config.minConnections > 0) {
      // Wait for the first connection to be established before adding more.
      const connection = await this.addConnection(
        signal,
        Number.POSITIVE_INFINITY,
        Number.POSITIVE_INFINITY,
      )
      this.connected.push(connection)
      for (let i = 0; i < this.config.minConnections - 1; i++) {
        this.addConnection(
          signal,
          Number.POSITIVE_INFINITY,
          Number.POSITIVE_INFINITY,
        ).then(connection => {
          this.connected.push(connection)
        }, noop)
      }
    }
    return this
  }

  /**
   * Closes all connections in the pool.
   */
  async close() {
    if (this.dsn == null) {
      return
    }

    this.dsn = null
    this.initPromise = null

    const closing = Promise.all(
      this.connecting.map(promise => promise.then(conn => conn.close())),
    )
    this.connecting.length = 0

    this.connected.forEach(conn => conn.close())
    this.connected.length = 0

    // Clear the backlog by rejecting all promises.
    if (this.backlog.length > 0) {
      const error = new ConnectionError('Postgres client was closed')
      this.backlog.forEach(fn => fn(error))
      this.backlog = []
    }

    await closing.catch(noop)

    // This can't be unset until after the connections have been closed.
    this.parseText = null
  }

  /**
   * Returns a stringified version of the template. It's async because it uses
   * libpq's escaping functions.
   */
  stringify(input: SQLTemplateValue, options: { reindent?: boolean } = {}) {
    // Since we're not sending anything to the server, it's perfectly fine to
    // use a non-idle connection.
    const connection = this.connected[0]
    if (!connection) {
      throw new ConnectionError('Postgres client is not connected')
    }
    // biome-ignore lint/complexity/useLiteralKeys: Protected access
    return renderTemplateValue(input, connection['pq'], options)
  }

  /**
   * Execute one or more commands.
   */
  query<TRow extends Row>(
    input: SQLTemplate | QueryHook<CommandResult<TRow>[]>,
    options?: Query.Options | null,
  ): Query<CommandResult<TRow>[]> {
    return new Query(this, QueryType.full, input, options)
  }

  /**
   * Create a query that resolves with an array of rows (or stream one row at a
   * time, when used as an async iterable).
   *
   * - You may define the row type via this method's type parameter.
   * - Your SQL template may contain multiple commands, but they run
   *   sequentially. The result sets are concatenated.
   */
  queryRowList<TRow extends Row>(
    input: SQLTemplate | QueryHook<TRow[]>,
    options?: Query.Options | null,
  ): Query<TRow[]> {
    return new Query(this, QueryType.row, input, options)
  }

  /**
   * Create a query that resolves with an array of values, where each value is
   * derived from the only column of each row in the result set.
   *
   * - You may define the column type via this method's type parameter.
   * - Your SQL template may contain multiple commands, but they run
   *   sequentially. The result sets are concatenated.
   */
  queryValueList<T>(
    input: SQLTemplate | QueryHook<T[]>,
    options?: Query.Options | null,
  ): Query<T[]> {
    return new Query(this, QueryType.value, input, options)
  }

  /**
   * Create a query that resolves with a single row or null. This assumes only
   * one command exists in the given query. If you don't limit the results, the
   * promise will be rejected when more than one row is received.
   *
   * You may define the row type using generics.
   */
  queryRowOrNull<TRow extends Row>(
    input: SQLTemplate | QueryHook<TRow[]>,
    options?: Query.Options | null,
  ): Query<TRow | null, TRow> {
    return new Query(
      this,
      QueryType.row,
      input,
      options,
      QueryResultCount.zeroOrOne,
    )
  }

  /**
   * Like `queryRowOrNull`, but throws an error if the result is null.
   */
  queryRow<TRow extends Row>(
    input: SQLTemplate | QueryHook<TRow[]>,
    options?: Query.Options | null,
  ): Query<TRow> {
    return new Query(
      this,
      QueryType.row,
      input,
      options,
      QueryResultCount.exactlyOne,
    )
  }

  /**
   * Create a query that resolves with a single value, derived from the single
   * column of the single row of the result set.
   *
   */
  queryValueOrNull<T>(
    input: SQLTemplate | QueryHook<T[]>,
    options?: Query.Options | null,
  ): Query<T | null, T> {
    return new Query(
      this,
      QueryType.value,
      input,
      options,
      QueryResultCount.zeroOrOne,
    )
  }

  /**
   * Like `queryValueOrNull`, but throws an error if the result is null.
   */
  queryValue<T extends {}>(
    input: SQLTemplate | QueryHook<T[]>,
    options?: Query.Options | null,
  ): Query<T, T> {
    return new Query(
      this,
      QueryType.value,
      input,
      options,
      QueryResultCount.exactlyOne,
    )
  }
}

export type ClientProxy<TSchema extends object> = Omit<
  Client,
  keyof TSchema
> & {
  [K in keyof TSchema]: TSchema[K] extends (
    client: Client,
    ...args: infer TArgs
  ) => infer TResult
    ? (...args: TArgs) => TResult
    : never
}
