import Libpq from '@pg-nano/libpq'
import { EventEmitter } from 'node:events'
import util from 'node:util'
import { isFunction, noop, shake, uid } from 'radashi'
import { debugConnection, debugQuery } from './debug'
import { PgNativeError } from './error'
import { baseTypeParsers, createTextParser } from './pg-types.js'
import {
  type CommandResult,
  type QueryDescriptor,
  type QueryHook,
  type QueryOptions,
  type QueryPromise,
  QueryType,
} from './query.js'
import { streamResults } from './result-stream.js'
import {
  hashSessionParameters,
  renderSessionParameters,
  type SessionParameters,
} from './session.js'
import type { SQLTemplate } from './template'
import { renderTemplate } from './template/render'

interface ConnectionEvents {
  result: [result: unknown]
  end: []
  close: []
}

/**
 * The `pg-native` connection represents a single socket, connected to a
 * PostgreSQL server. It can only process one query at a time. There is no
 * protection against concurrent queries, and the connection won't work
 * correctly if more than one query is executed at the same time.
 *
 * You must call `connect` before you can execute queries. If you later call
 * `close`, you will have to call `connect` again before you can execute further
 * queries.
 *
 * Row streaming is possible through the `"result"` event. Results won't be
 * buffered if a `"result"` listener is added.
 *
 * The `"notify"` event is emitted when a NOTIFY message is received from the
 * database. This is useful for push-based data updates.
 */
export class Connection extends EventEmitter<ConnectionEvents> {
  protected currentQuery: QueryDescriptor | null = null
  protected idleTimeoutId: any = null
  protected declare pq: Libpq

  readonly id = uid(8)
  status: ConnectionStatus = ConnectionStatus.CLOSED
  /**
   * A hash of the session parameters used to connect to the database.
   */
  sessionHash = ''

  constructor(readonly idleTimeout: number = Number.POSITIVE_INFINITY) {
    super()
  }

  async connect(dsn: string, sessionParams?: SessionParameters) {
    this.pq = new Libpq()
    await this.pq.connect(dsn)
    if (sessionParams) {
      sessionParams = shake(sessionParams)
    }
    if (sessionParams && Object.keys(sessionParams).length) {
      await this.query(QueryType.void, renderSessionParameters(sessionParams))
      this.sessionHash = hashSessionParameters(sessionParams)
    } else {
      setStatus(unprotect(this), ConnectionStatus.IDLE)
    }
  }

  setIdleTimeout() {
    if (Number.isFinite(this.idleTimeout)) {
      clearTimeout(this.idleTimeoutId)
      this.idleTimeoutId = setTimeout(() => this.close(), this.idleTimeout)
    }
  }

  /**
   * Execute a dynamic query which may contain multiple statements.
   */
  query<TResult = CommandResult[]>(
    type: QueryType,
    input: SQLTemplate | QueryHook<TResult>,
    parseText?: ((value: string, dataTypeID: number) => unknown) | null,
    options?: QueryOptions | null,
  ): QueryPromise<TResult> {
    const conn = unprotect(this)
    if (!conn.pq) {
      throw new PgNativeError('Connection closed')
    }

    const query: QueryDescriptor = {
      id: uid(8),
      type,
      input,
      parseText: parseText || createTextParser(baseTypeParsers),
      ctrl: new AbortController(),
      error: null,
      ...options,
    }

    const promise = sendQuery(conn, query).finally(() => {
      reset(conn, ConnectionStatus.IDLE)
      conn.setIdleTimeout()
    }) as QueryPromise<TResult>

    promise.cancel = () => {
      promise.cancel = noop
      if (this.currentQuery === query) {
        query.ctrl.abort()
        const result = this.pq.cancel()
        if (result !== true) {
          throw new Error(result)
        }
      }
    }

    if (process.env.NODE_ENV !== 'production' && debugQuery.enabled) {
      promise.then(
        results => {
          debugQuery(
            `query:${query.id} results\n  ${util.inspect(results, { depth: null }).replace(/\n/g, '\n  ')}`,
          )
        },
        error => {
          debugQuery(
            `query:${query.id} error\n  ${util.inspect(error, { depth: null }).replace(/\n/g, '\n  ')}`,
          )
        },
      )
    }

    return promise
  }

  /**
   * Stream each result from the query as it comes in. Query errors are not
   * propagated through this method.
   */
  async *stream<TResult = CommandResult>() {
    let current = Promise.withResolvers<boolean>()

    const buffer: TResult[] = []
    const onResult = (result: unknown) => {
      buffer.push(result as TResult)
      current.resolve(false)
      current = Promise.withResolvers()
    }

    this.on('result', onResult)
    this.once('end', () => {
      this.off('result', onResult)
      current.resolve(true)
    })

    while (true) {
      const done = await current.promise
      if (buffer.length > 0) {
        const results = [...buffer]
        buffer.length = 0
        yield* results
      }
      if (done) {
        return
      }
    }
  }

  /**
   * Close the database connection.
   */
  close() {
    if (this.pq) {
      this.currentQuery?.ctrl.abort(new PgNativeError('Connection closed'))
      reset(unprotect(this), ConnectionStatus.CLOSED)
      this.pq.finish()
      this.pq = null!
      this.emit('close')
    }
  }
}

export enum ConnectionStatus {
  /** This connection is not connected to a server. */
  CLOSED = 0,
  /** This connection is ready to process queries. */
  IDLE = 1,
  /** This connection is reserved for a query but is not actively processing it. */
  RESERVED = 2,
  /** This connection is actively writing a query to the socket. */
  QUERY_WRITING = 3,
  /** This connection is actively reading the result of a query from the socket. */
  QUERY_READING = 4,
  /** This connection will be idle once custom types are loaded. */
  AWAITING_TYPES = 5,
}

interface IConnection extends EventEmitter<ConnectionEvents> {
  pq: Libpq
  id: string
  status: ConnectionStatus
  currentQuery: QueryDescriptor | null
  setIdleTimeout(): void
}

function unprotect(conn: Connection): IConnection {
  return conn as any
}

function setStatus(conn: IConnection, newStatus: ConnectionStatus): void {
  if (conn.status !== newStatus) {
    conn.status = newStatus
    if (process.env.NODE_ENV !== 'production') {
      debugConnection(
        `connection:${conn.id} status changed to ${ConnectionStatus[newStatus]}`,
      )
    }
  }
}

function reset(conn: IConnection, newStatus: ConnectionStatus): void {
  stopReading(conn, newStatus)
  if (conn.currentQuery) {
    conn.currentQuery = null
    conn.emit('end')
  }
}

/**
 * Sends a query to libpq and waits for it to finish writing query text to the
 * socket.
 */
async function sendQuery(
  conn: IConnection,
  query: QueryDescriptor,
): Promise<any> {
  stopReading(conn, ConnectionStatus.QUERY_WRITING)
  conn.currentQuery = query

  if (!conn.pq.setNonBlocking(true)) {
    throw new PgNativeError('Unable to set non-blocking to true')
  }

  const { input } = query

  let sent: boolean | (() => Promise<any>)

  if (isFunction(input)) {
    sent = input(conn.pq, query)
  } else {
    if (!input.params || !input.command) {
      input.params = []
      input.command = renderTemplate(input, conn.pq, {
        reindent: process.env.NODE_ENV !== 'production',
      })
    }

    if (process.env.NODE_ENV !== 'production' && debugQuery.enabled) {
      debugQuery(
        `query:${query.id} writing\n${input.command.replace(/^|\n/g, '$&  ')}`,
      )
    }

    sent = input.params.length
      ? conn.pq.sendQueryParams(input.command, input.params)
      : conn.pq.sendQuery(input.command)
  }

  if (!sent) {
    debugConnection(`connection:${conn.id} failed to send query`)
    throw new PgNativeError(conn.pq.getLastErrorMessage())
  }

  if (
    query.singleRowMode &&
    query.type !== QueryType.full &&
    !conn.pq.setSingleRowMode()
  ) {
    throw new PgNativeError('Unable to set single row mode')
  }

  await waitForDrain(conn, conn.pq)

  if (isFunction(sent)) {
    return sent()
  }

  setStatus(conn, ConnectionStatus.QUERY_READING)

  const results = query.singleRowMode ? null : ([] as unknown[])

  // Each `result` is an array of results, except when `query.singleRowMode` is
  // true, in which case, individual results are yielded immediately upon being
  // received and parsed.
  for await (const result of streamResults(conn.pq, query)) {
    if (results) {
      results.push(result)
    } else {
      conn.emit('result', result)
    }
  }

  if (query.error) {
    throw query.error
  }

  return results && query.type !== QueryType.full
    ? results.length > 1
      ? results.flat()
      : results[0]
    : results
}

function stopReading(conn: IConnection, newStatus: ConnectionStatus) {
  if (conn.pq && conn.status === ConnectionStatus.QUERY_READING) {
    conn.pq.stopRead()
  }
  setStatus(conn, newStatus)
}

function waitForDrain(conn: IConnection, pq: Libpq) {
  return new Promise<void>(function check(resolve, reject) {
    switch (pq.flush()) {
      case 0:
        resolve()
        break
      case -1:
        debugConnection(`connection:${conn.id} failed to flush`)
        reject(new PgNativeError(pq.getLastErrorMessage()))
        break
      default:
        // You cannot read & write on a socket at the same time.
        pq.writable(() => {
          check(resolve, reject)
        })
    }
  })
}
