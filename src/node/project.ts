import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import type { ShallowOptions } from 'option-types'
import type { Client } from 'pg-nano'
import type { UserConfig } from 'pg-nano/config'
import { parseConnectionString } from 'pg-native'
import { map } from 'radashi'
import { globSync } from 'tinyglobby'
import type { PgBaseType, PgObjectStmt } from './config/plugin.js'
import { debug, traceDepends, traceParser } from './debug.js'
import { getEnv, type Env } from './env.js'
import { events } from './events.js'
import { generate } from './generator/generate.js'
import { prepareObjects } from './generator/prepareObjects.js'
import { prepareSchemaDir } from './generator/prepareSchemaDir.js'
import { runGeneratorPlugins } from './generator/runGeneratorPlugins.js'
import { inspectBaseTypes } from './inspector/inspect.js'
import { createNameResolver } from './inspector/name.js'
import { linkStatements } from './linker/link.js'
import { migrateSchema } from './migrate/migrateSchema.js'
import { migrateStaticRows } from './migrate/migrateStaticRows.js'
import type { SQLIdentifier } from './parser/identifier.js'
import { parseSchemaFile, type PgSchema } from './parser/parse.js'
import { resolveImport } from './util/resolveImport.js'

export class Project {
  #snapshot: ProjectSnapshot | null = null

  constructor(readonly options: Readonly<Project.Options>) {}

  /**
   * Introspect the project's database schema, skipping any migration or emit
   * steps.
   */
  async refresh(options: Project.RefreshOptions = {}) {
    let env = this.#snapshot?.env
    if (!env || options.reloadEnv) {
      env = await getEnv(this.options.root, {
        overrides: this.options.config,
        noConfigBundling: this.options.noConfigBundling,
        findConfigFile: this.options.findConfigFile,
      })
    }

    const findSchemaFiles =
      this.options.findSchemaFiles ??
      ((cwd, include, ignore) =>
        globSync(include, {
          cwd,
          ignore,
          absolute: true,
        }))

    let filePaths = findSchemaFiles(env.root, env.config.schema.include, [
      ...env.config.schema.exclude,
      env.config.generate.pluginSqlDir,
      '**/.pg-nano/**',
    ])

    // Include our internal SQL files in the migration plan.
    filePaths = [...filePaths, resolveImport('pg-nano/sql/nano.sql')]

    const client = await env.client
    const readFile = this.options.readFile ?? fs.readFileSync

    const baseTypes = await inspectBaseTypes(client)
    const schema = flatMapProperties(
      await map(filePaths, async file => {
        traceParser('parsing schema file:', file)
        return await parseSchemaFile(readFile(file, 'utf8'), file, baseTypes)
      }),
    )

    this.#snapshot = {
      env,
      filePaths,
      client,
      schema,
      baseTypes,
    }

    return {
      configFilePath: env.configFilePath,
      configDependencies: env.configDependencies,
    }
  }

  /**
   * Migrate the database schema and any static rows according to the pre-parsed
   * SQL files, then generate TypeScript bindings for any user-defined Postgres
   * routines.
   *
   * If you're calling this method, you shouldn't also call `refresh`.
   */
  async update(options: Project.UpdateOptions = {}) {
    if (!this.#snapshot || !options.skipRefresh) {
      await this.refresh()
    }

    const { env, schema, baseTypes } = this.#snapshot!
    const pg = await env.client

    const names = createNameResolver(pg)

    // Plugins may add to the object list, so run them before linking the object
    // dependencies together.
    const pluginsByStatementId = await runGeneratorPlugins(
      env,
      schema,
      baseTypes,
    )

    debug('plugin statements prepared')

    // Since pg-schema-diff is still somewhat limited, we have to create our own
    // dependency graph, so we can ensure all objects (and their dependencies)
    // exist before pg-schema-diff works its magic.
    const sortedObjectStmts = linkStatements(schema)

    if (traceDepends.enabled) {
      for (const stmt of sortedObjectStmts) {
        const name = `${stmt.kind} ${stmt.id.toQualifiedName()}`
        if (stmt.dependencies.size > 0) {
          traceDepends(
            `${name}\n${Array.from(
              stmt.dependencies,
              (dep, index) =>
                `${index === stmt.dependencies.size - 1 ? '└─' : '├─'} ${dep.id.toQualifiedName()}`,
            ).join('\n')}`,
          )
        } else {
          traceDepends('\x1b[2m%s (none)\x1b[0m', name)
        }
      }
    }

    events.emit('prepare:start')

    const droppedTables = new Set<SQLIdentifier>()

    await prepareObjects(pg, schema, names, droppedTables)
    prepareSchemaDir(env, sortedObjectStmts)

    await migrateSchema(env, droppedTables)
    await migrateStaticRows(pg, schema, droppedTables, names)

    // TODO: only purge the cache if composite types have changed
    purgeClientCache(pg)

    if (!options.noEmit) {
      await generate(
        env,
        schema,
        baseTypes,
        sortedObjectStmts,
        pluginsByStatementId,
        options.signal,
      )
    }
  }

  /**
   * Find a statement by its schema and name. You can set the `schema` to an
   * empty string to allow any schema.
   */
  async findObjectStatement(
    schema: string,
    name: string,
  ): Promise<PgObjectStmt | undefined> {
    const snapshot = await this.#assertSnapshot()
    return snapshot.schema.objects.find(
      schema === ''
        ? stmt => stmt.id.name === name
        : stmt =>
            name === stmt.id.name && schema === (stmt.id.schema ?? 'public'),
    )
  }

  async #assertSnapshot() {
    if (!this.#snapshot) {
      await this.refresh()
    }
    return this.#snapshot!
  }
}

export declare namespace Project {
  type Options = ShallowOptions<{
    /**
     * The directory in which your `pg-nano.config.ts` file is located.
     */
    root: string
    /**
     * Partially override the config file.
     */
    config?: Partial<UserConfig>
    /**
     * When true, dynamic `import()` is used to load the config file, instead of
     * the `bundle-require` npm package.
     */
    noConfigBundling?: boolean
    /**
     * Override the default file-reading API.
     *
     * @default import('node:fs').readFileSync
     */
    readFile?: (filePath: string, encoding: BufferEncoding) => string
    /**
     * Override the default logic for finding the config file.
     *
     * The returned path must be absolute.
     */
    findConfigFile?: (cwd: string) => string | null
    /**
     * Override the default logic for finding schema files.
     *
     * The returned paths must be absolute.
     */
    findSchemaFiles?: (
      cwd: string,
      include: string[],
      exclude: string[],
    ) => string[]
  }>
  type RefreshOptions = ShallowOptions<{
    reloadEnv?: boolean
  }>
  type UpdateOptions = ShallowOptions<{
    /**
     * When true, the project is not refreshed before updating.
     */
    skipRefresh?: boolean
    /**
     * When true, no files are emitted, but the database is still migrated.
     */
    noEmit?: boolean
    /**
     * Use an abort signal to conditionally halt the update process as soon as
     * possible. Most useful for file-watching updates.
     */
    signal?: AbortSignal
  }>
}

interface ProjectSnapshot {
  readonly env: Env
  readonly filePaths: string[]
  readonly client: Client
  readonly schema: PgSchema
  readonly baseTypes: PgBaseType[]
}

function flatMapProperties<T extends Record<string, any[]>>(objects: T[]): T {
  const merged = {} as T
  for (const key in objects[0]) {
    merged[key] = objects.flatMap(obj => obj[key]) as any
  }
  return merged
}

/**
 * The “client cache” is a directory in the user's home directory that stores
 * custom type parsers for a given database connection.
 *
 * Removing it ensures that new instances of `Client` for the configured
 * database will re-introspect the database and generate new custom type
 * parsers. For long-running processes, the `Client` instance may have its
 * `reloadCustomTypes` method called, which will also re-generate the custom
 * type parsers.
 */
function purgeClientCache(pg: Client) {
  // TODO: use `PQconninfo` instead of making assumptions about default values
  // @see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PQCONNINFO
  const {
    host = process.env.PGHOST ?? 'localhost',
    port = process.env.PGPORT ?? 5432,
    dbname = process.env.PGDATABASE ?? 'postgres',
  } = parseConnectionString(pg.dsn!)

  const cacheDir = path.join(
    os.homedir(),
    `.pg-nano/${dbname}+${md5Hex(`${host}:${port}`)}`,
  )

  fs.rmSync(cacheDir, { recursive: true, force: true })
}

function md5Hex(value: string) {
  return crypto.createHash('md5').update(value).digest('hex')
}
