import {
  buildResult,
  sql,
  type Client,
  type QueryHook,
  type Result,
} from 'pg-nano'

export type PgObject = PgFunction | PgEnumType | PgCompositeType

export type PgNamespace = {
  name: string
  functions: PgFunction[]
  compositeTypes: PgCompositeType[]
  enumTypes: PgEnumType[]
  /**
   * The names of every object in this namespace.
   */
  names: string[]
}

export async function introspectNamespaces(
  client: Client,
  signal?: AbortSignal,
) {
  const [functions, compositeTypes, enumTypes] = await Promise.all([
    introspectFunctions(client, signal),
    introspectCompositeTypes(client, signal),
    introspectEnumTypes(client, signal),
  ])

  const namespaces: Record<string, PgNamespace> = {}
  const getNamespace = (nspname: string) =>
    (namespaces[nspname] ??= {
      name: nspname,
      functions: [],
      compositeTypes: [],
      enumTypes: [],
      names: [],
    })

  for (const fn of functions) {
    const nsp = getNamespace(fn.nspname)
    nsp.functions.push(fn)
    nsp.names.push(fn.proname)
  }

  for (const t of compositeTypes) {
    const nsp = getNamespace(t.nspname)
    nsp.compositeTypes.push(t)
    nsp.names.push(t.typname)
  }

  for (const t of enumTypes) {
    const nsp = getNamespace(t.nspname)
    nsp.enumTypes.push(t)
    nsp.names.push(t.typname)
  }

  return namespaces
}

export type PgFunction = {
  nspname: string
  proname: string
  proargnames: string[] | null
  /** Space-separated list of argument types */
  proargtypes: number[]
  pronargdefaults: number
  prorettype: number
  proretset: boolean
  provariadic: boolean
}

export function introspectFunctions(client: Client, signal?: AbortSignal) {
  /**
   * Find the procs that are:
   *   - not built-in
   *   - not added by extensions
   *   - not related to a trigger
   */
  const query = sql`
    SELECT n.nspname, p.proname, p.proargnames, 
           p.proargtypes::int[] AS proargtypes, 
           p.pronargdefaults, p.prorettype, p.proretset, p.provariadic
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON (n.oid = p.pronamespace)
    LEFT JOIN pg_catalog.pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
    LEFT JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
    WHERE p.prokind = 'f' 
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND e.oid IS NULL
      AND p.prorettype != 2279 -- trigger
  `

  return client.queryRows<PgFunction>(query, { signal })
}

export async function introspectResultSet(
  client: Client,
  fn: PgFunction,
  signal?: AbortSignal,
) {
  const stmtName = 'pg_nano_' + fn.proname

  await sendCommand(
    client,
    pq =>
      pq.sendPrepare(
        stmtName,
        `SELECT * FROM ${fn.nspname}.${fn.proname}(${fn.proargtypes.map((_, i) => `$${i + 1}`).join(', ')})`,
        fn.proargtypes.length,
      ),
    signal,
  )

  const description = await sendCommand(
    client,
    pq => {
      pq.describePrepared(stmtName)
      return async () => buildResult(pq)
    },
    signal,
  )

  const query = sql`DEALLOCATE ${sql.id(stmtName)}`
  await client.query(query).withOptions({ signal })

  return description.fields
}

export type PgArrayType = {
  oid: number
  typelem: number
}

export function introspectArrayTypes(client: Client, signal?: AbortSignal) {
  const query = sql`
    SELECT oid, typelem
    FROM pg_catalog.pg_type
    WHERE typlen = -1 AND typelem != 0 AND typarray = 0
  `

  return client.queryRows<PgArrayType>(query, { signal })
}

export type PgEnumType = {
  oid: number
  typname: string
  nspname: string
  typarray: number
  labels: string[]
}

export function introspectEnumTypes(client: Client, signal?: AbortSignal) {
  const query = sql`
    SELECT
      t.oid,
      t.typname,
      n.nspname,
      t.typarray::oid,
      array(
        SELECT enumlabel
        FROM pg_catalog.pg_enum e
        WHERE e.enumtypid = t.oid
        ORDER BY e.enumsortorder
      )::text[] AS labels
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typtype = 'e'
  `

  return client.queryRows<PgEnumType>(query, { signal })
}

export type PgCompositeType = {
  oid: number
  typname: string
  nspname: string
  typarray: number
  fields: Array<{
    attname: string
    atttypid: number
    attnotnull: boolean
  }>
}

export function introspectCompositeTypes(client: Client, signal?: AbortSignal) {
  const attributesQuery = sql`
    SELECT array_agg(
      json_build_object(
        'attname', a.attname,
        'atttypid', a.atttypid::int,
        'attnotnull', a.attnotnull
      )
      ORDER BY a.attnum
    )
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = t.typrelid
      AND a.attnum > 0
      AND NOT a.attisdropped
  `

  const query = sql`
    SELECT
      t.oid,
      t.typname,
      n.nspname,
      t.typarray::oid,
      (${attributesQuery}) AS fields
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typrelid != 0
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  `

  return client.queryRows<PgCompositeType>(query, { signal })
}

async function sendCommand<TResult = Result[]>(
  client: Client,
  hook: QueryHook<TResult>,
  signal?: AbortSignal,
) {
  // biome-ignore lint/complexity/useLiteralKeys: Protected access
  const connection = client['getConnection'](signal)
  // biome-ignore lint/complexity/useLiteralKeys: Protected access
  return client['dispatchQuery'](connection, hook, signal)
}
