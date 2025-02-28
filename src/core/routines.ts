import type { Options } from 'option-types'
import {
  sql,
  type Interval,
  type QueryOptions,
  type QueryType,
  type Range,
  type Row,
  type SQLTemplate,
  type SQLToken,
} from 'pg-native'
import { isArray, isObject } from 'radashi'
import type { Client } from './client.js'
import type { FieldMapper } from './data/fieldMapper.js'
import type { Input } from './data/types.js'
import type { ListQuery, RowQuery, ValueQuery, VoidQuery } from './query.js'

/**
 * Allow a single value to be passed instead of named parameters, unless the
 * value is a plain object.
 */
type UnwrapSingleKey<T> = keyof T extends infer TKey
  ? TKey extends keyof T
    ? { [K in TKey]: T[K] } extends T
      ? T[TKey] extends
          | readonly any[]
          | Interval
          | Range<any>
          | Date
          | RegExp
          | NodeJS.TypedArray
          | Buffer
          | BigInt
        ? T[TKey]
        : Exclude<T[TKey], object>
      : never
    : never
  : never

export type Routine<TArgs extends object, TResult> = TArgs extends any[]
  ? (client: Client, ...args: Input<TArgs>) => TResult
  : object extends TArgs
    ? (client: Client, args?: Input<TArgs | UnwrapSingleKey<TArgs>>) => TResult
    : (client: Client, args: Input<TArgs | UnwrapSingleKey<TArgs>>) => TResult

/**
 * Create a dedicated query function for a Postgres routine that returns a
 * result set of any number of rows. The result set may be empty.
 */
export function bindQueryRowList<TArgs extends object, TRow extends Row>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, ListQuery<TRow, QueryType.row>> {
  return buildRoutine('queryRowList', name, build) as any
}

/**
 * Create a dedicated query function for a Postgres routine that returns a
 * result set where each row has a single column. The result set may be empty.
 */
export function bindQueryValueList<TArgs extends object, TResult>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, ListQuery<TResult, QueryType.value>> {
  return buildRoutine('queryValueList', name, build) as any
}

/**
 * Create a dedicated query function for a Postgres routine that returns a
 * single row or throws an error.
 */
export function bindQueryRow<TArgs extends object, TRow extends Row>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, RowQuery<TRow>> {
  return buildRoutine('queryRow', name, build) as any
}

/**
 * Create a dedicated query function for a Postgres routine that returns a
 * single value (i.e. one row with one column) or throws an error.
 */
export function bindQueryValue<TArgs extends object, TResult>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, ValueQuery<TResult>> {
  return buildRoutine('queryValue', name, build) as any
}

/**
 * Create a dedicated query function for a Postgres routine that returns a
 * single row or nothing.
 */
export function bindQueryRowOrNull<TArgs extends object, TRow extends Row>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, RowQuery<TRow | null>> {
  return buildRoutine('queryRowOrNull', name, build) as any
}

/**
 * Create a dedicated query function for a Postgres routine that returns a
 * single value (i.e. one row with one column) or nothing.
 */
export function bindQueryValueOrNull<TArgs extends object, TResult>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, ValueQuery<TResult | null>> {
  return buildRoutine('queryValueOrNull', name, build) as any
}

export function bindProcedure<TArgs extends object>(
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<TArgs, VoidQuery> {
  return buildRoutine('query', name, build) as any
}

function buildRoutine(
  method: keyof Client,
  name: string | string[],
  build: (builder: RoutineBuilder) => typeof builder,
): Routine<any, any> {
  const { config } = build(new RoutineBuilder())

  const id = isArray(name) ? sql.id(...name) : sql.id(name)
  const query = method === 'query' ? queryProcedureCall : queryFunctionCall

  // When a function's return type is a record type without SETOF before it, the
  // use of `RETURN NULL` leads to a record with all nulls. The where clause
  // below is a workaround to treat such a record as just NULL.
  const endClause = config.returnsRecord
    ? sql`WHERE NOT res IS NULL`
    : undefined

  if (config.argNames) {
    return (client: Client, args?: unknown) => {
      return client[method as 'query'](
        query(id, prepareInput(client, args, config), endClause),
        getQueryOptions(client, config),
      )
    }
  }

  return (client: Client, ...args: any[]) => {
    return client[method as 'query'](
      query(id, prepareInput(client, args, config), endClause),
      getQueryOptions(client, config),
    )
  }
}

type RoutineConfig = Options<{
  minArgCount?: number
  maxArgCount?: number
  argNames?: string[]
  returnsRecord?: boolean
}> & {
  inputMappers?: Record<string, FieldMapper> | undefined
  outputMappers?: Record<string, FieldMapper> | undefined
}

class RoutineBuilder {
  config: RoutineConfig = {}

  arity(minArgCount: number, maxArgCount?: number) {
    this.config.minArgCount = minArgCount
    this.config.maxArgCount = maxArgCount
    return this
  }
  namedArgs(argNames: string[]) {
    this.config.argNames = argNames
    return this
  }
  returnsRecord() {
    this.config.returnsRecord = true
    return this
  }
  mapInput(index: number, type: FieldMapper) {
    this.config.inputMappers ||= {}
    this.config.inputMappers[index] = type
    return this
  }
  mapOutput(key: string, type: FieldMapper) {
    this.config.outputMappers ||= {}
    this.config.outputMappers[key] = type
    return this
  }
}

function queryProcedureCall(id: SQLToken, values: any[]) {
  return sql`CALL ${id}(${sql.join(',', values.map(sql.param))})`
}

function queryFunctionCall(
  id: SQLToken,
  values: any[],
  endClause?: SQLTemplate,
) {
  return sql`
    SELECT * FROM ${id}(${sql.join(',', values.map(sql.param))}) res
    ${endClause}
  `
}

function getQueryOptions(client: Client, { outputMappers }: RoutineConfig) {
  let options: QueryOptions | undefined

  if (outputMappers) {
    options ||= {}
    options.mapFieldValue = (value, name) => {
      const type = outputMappers[name]
      if (type?.mapOutput) {
        return type.mapOutput(value, client)
      }
      return value
    }
  }

  return options
}

function prepareInput(client: Client, input: unknown, config: RoutineConfig) {
  // When all named parameters could be optional, passing `undefined` is
  // perfectly valid.
  if (input === undefined) {
    return []
  }

  let args: unknown[]
  let maxArgCount = config.maxArgCount
  let namedArgs: Record<string, unknown> | undefined

  if (config.argNames) {
    namedArgs = isObject(input)
      ? (input as Record<string, unknown>)
      : // When the input is not a plain object, there can only be one named
        // parameter.
        { [config.argNames[0]]: input }

    args = []
    maxArgCount ??= config.argNames.length
  } else {
    args = input as unknown[]
    maxArgCount ??= args.length
  }

  for (let i = maxArgCount; --i >= 0; ) {
    const name = config.argNames?.[i]
    const type = config.inputMappers?.[i]

    let value = namedArgs
      ? Object.prototype.hasOwnProperty.call(namedArgs, name!)
        ? namedArgs[name!]
        : undefined
      : i < args.length
        ? args[i]
        : undefined

    if (type?.mapInput && value != null) {
      value = type.mapInput(value, client)
    }

    if (value !== undefined) {
      args[i] = value
    } else if (i < args.length) {
      args[i] = null
    }
  }

  return args
}
