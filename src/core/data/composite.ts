import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { QueryType, sql, type Connection, type TextParser } from 'pg-native'
import { noop, pascal } from 'radashi'
import { debug } from '../debug.js'

export async function importCustomTypeParsers(
  conn: Connection,
  host: string,
  port: string | number,
  dbname: string,
  force?: boolean,
): Promise<Record<number, TextParser>> {
  // Runtime-generated modules are stored in the user's home directory, so that
  // pg-nano's "dev" command can purge them when the schema changes.
  const cacheDir = path.join(
    os.homedir(),
    `.pg-nano/${dbname}+${md5Hex(`${host}:${port}`)}`,
  )

  const moduleName = 'customTypeParsers'

  let cachedModuleFile = await fs
    .readdir(cacheDir)
    .then(files => files.findLast(file => file.startsWith(moduleName)), noop)

  if (force || !cachedModuleFile) {
    debug('building custom type parsers')

    const code = await buildCustomTypeParsers(conn)

    cachedModuleFile = `${moduleName}.${Date.now()}.mjs`

    await fs.mkdir(cacheDir, { recursive: true })
    await fs.writeFile(path.join(cacheDir, cachedModuleFile), code)
  }

  const exports = await import(path.join(cacheDir, cachedModuleFile))
  return exports.default(await import('pg-native'))
}

async function buildCustomTypeParsers(conn: Connection) {
  let code = ''
  let textParsersByOid = ''

  const imports = new Set<string>()

  for (const type of await queryCustomTypes(conn)) {
    const parserId = `parse${pascal(type.name)}`

    if (type.kind === 'c') {
      code += `const ${parserId} = parseComposite({ ${type.attributes
        .map(attr => `${JSON.stringify(attr.name)}: ${attr.dataTypeID}`)
        .join(', ')} })\n`

      imports.add('parseComposite')
      textParsersByOid += `  ${type.id}: ${parserId},\n`
    } else {
      continue
    }

    if (type.arrayTypeID) {
      imports.add('parseArray')
      textParsersByOid += `  ${type.arrayTypeID}: parseArray(${parserId}),\n`
    }
  }

  code += `\nreturn {\n${textParsersByOid}}\n`

  return `export default ({ ${[...imports].join(', ')} }) => {\n  ${code.replace(/\n+/g, '$&  ').trimEnd()}\n}`
}

type CustomType = {
  id: number
  name: string
  kind: string
  arrayTypeID: number
  attributes: {
    name: string
    dataTypeID: number
  }[]
}

function queryCustomTypes(conn: Connection) {
  return conn.query<CustomType[]>(
    QueryType.row,
    sql`
      SELECT
        oid AS "id",
        typname AS "name",
        typtype AS "kind",
        typarray AS "arrayTypeID",
        (SELECT
          json_agg(json_build_object(
            'name', attname,
            'dataTypeID', atttypid::int
          ) ORDER BY attnum)
          FROM pg_attribute
          WHERE attrelid = t.typrelid
            AND attnum > 0
            AND NOT attisdropped
        ) AS attributes
      FROM
        pg_type t
      WHERE
        typtype = 'c'
        AND typnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
    `,
  )
}

function md5Hex(value: string) {
  return crypto.createHash('md5').update(value).digest('hex')
}
