import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { type Client, sql } from 'pg-nano'
import { group, memo, sift, tryit } from 'radashi'
import type { Env } from './env'
import { log } from './log'
import { parseIdentifier, type SQLIdentifier } from './parseIdentifier'
import { dedent } from './util/dedent'

type SQLObject = {
  id: SQLIdentifier
  type: string
  stmtIndex: number
}

export async function prepareForMigration(filePaths: string[], env: Env) {
  const client = await env.client

  fs.rmSync(env.schemaDir, { recursive: true, force: true })
  fs.mkdirSync(env.schemaDir, { recursive: true })

  const { pre: prePlanFiles, rest: schemaFiles = [] } = group(
    filePaths,
    file => {
      const name = path.basename(file)
      return name[0] === '!' ? 'pre' : 'rest'
    },
  )

  let prePlanDDL = dedent`
    SET check_function_bodies = off;\n\n
  `

  if (prePlanFiles) {
    prePlanDDL +=
      prePlanFiles.map(file => fs.readFileSync(file, 'utf8')).join('\n\n') +
      '\n\n'
  }

  const parsedSchemaFiles = schemaFiles.map(file => {
    const stmts = splitStatements(fs.readFileSync(file, 'utf8'))
    const objects = stmts.map((stmt, stmtIndex) => {
      const match = stmt.match(
        /(?:^|\n)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP|TEMPORARY\s+)?(?:RECURSIVE\s+)?(\w+)\s+(?:IF\s+NOT\s+EXISTS\s+)?(.+?)(?:\s+\w+|\s*[;(])/i,
      )
      if (match) {
        let [, type, id] = match
        type = type.toLowerCase()

        return {
          id: parseIdentifier(id),
          type,
          stmtIndex,
        }
      }
      return null
    })
    return { file, stmts, objects }
  })

  const doesObjectExist = memo(async (object: SQLObject) => {
    if (object.type === 'table') {
      return await client.scalar<boolean>(sql`
        SELECT EXISTS (
          SELECT 1
          FROM pg_tables
          WHERE schemaname = ${object.id.schemaVal}
            AND tablename = ${object.id.nameVal}
        );
      `)
    }
    if (object.type === 'type') {
      return await client.scalar<boolean>(sql`
        SELECT EXISTS (
          SELECT 1
          FROM pg_type
          WHERE typname = ${object.id.nameVal}
            AND typnamespace = ${object.id.schemaVal}::regnamespace
        );
      `)
    }
    return false
  })

  const schemaObjects = parsedSchemaFiles.flatMap(({ objects }) =>
    sift(objects),
  )

  type SQLFuncWithSetof = {
    id: SQLIdentifier
    referencedId: SQLIdentifier
  }

  const funcsWithSetof: SQLFuncWithSetof[] = []
  const stubbedTables = new Set<SQLObject>()

  for (const { file, stmts, objects } of parsedSchemaFiles) {
    const outFile = path.join(
      env.schemaDir,
      path.basename(file, path.extname(file)) +
        '.' +
        md5Hash(file).slice(0, 8) +
        '.sql',
    )

    const unhandledStmts: string[] = []

    for (let i = 0; i < stmts.length; i++) {
      const stmt = stmts[i]
      const object = objects[i]

      // Skip views, which are not *yet* supported by pg-schema-diff.
      // https://github.com/stripe/pg-schema-diff/issues/135
      if (!object || object.type === 'view') {
        unhandledStmts.push(stmt)
        continue
      }

      if (object.type === 'function') {
        const matchedSetof = /\bRETURNS\s+SETOF\s+(.+?)\s+AS\b/i.exec(stmt)

        // When a function uses SETOF with a table identifier, that table may
        // not exist before pg-schema-diff creates the function. This issue is
        // tracked by https://github.com/stripe/pg-schema-diff/issues/129.
        //
        // Therefore, we need to ensure the table exists before applying the
        // migration plan generated by pg-schema-diff.
        if (matchedSetof) {
          const referencedId = parseIdentifier(matchedSetof[1])
          const referencedTable = schemaObjects.find(
            obj => obj.type === 'table' && obj.id.compare(referencedId),
          )

          if (
            referencedTable &&
            !stubbedTables.has(referencedTable) &&
            !(await doesObjectExist(referencedTable))
          ) {
            // Use an empty table as a placeholder to avoid reference errors.
            await client.query(sql`
              CREATE TABLE ${referencedId.toSQL()} (tmp int);
              ALTER TABLE ${referencedId.toSQL()} DROP COLUMN tmp;
            `)
            stubbedTables.add(referencedTable)
          }

          funcsWithSetof.push({
            id: object.id,
            referencedId,
          })
        }
      }
      // Non-enum types are not supported by pg-schema-diff, so we need to
      // diff them manually.
      else if (object.type === 'type' && !stmt.match(/\bAS\s+ENUM\b/i)) {
        const typeExists = await doesObjectExist(object)
        if (!typeExists) {
          await client.query(sql.unsafe(stmt))
          stmts[i] = ''
        } else if (await hasTypeChanged(client, object, stmt)) {
          await client.query(sql`
            DROP TYPE ${object.id.toSQL()} CASCADE;
            ${sql.unsafe(stmt)}
          `)
        }
      }
    }

    if (unhandledStmts.length) {
      log.warn(`Unhandled statements in ${file}:`)
      log.warn(
        unhandledStmts
          .map(stmt => {
            stmt = stmt.replace(/(^|\n) *--[^\n]+/g, '').replace(/\s+/g, ' ')
            return '  ' + (stmt.length > 50 ? stmt.slice(0, 50) + '…' : stmt)
          })
          .join('\n\n'),
      )
    }

    tryit(fs.unlinkSync)(outFile)
    fs.writeFileSync(outFile, sift(stmts).join('\n\n'))
  }

  await client.query(sql`
    DROP SCHEMA IF EXISTS nano CASCADE;
  `)

  const prePlanFile = path.join(env.untrackedDir, 'pre-plan.sql')
  fs.writeFileSync(prePlanFile, prePlanDDL)

  return {
    funcsWithSetof,
  }
}

function md5Hash(input: string): string {
  return crypto.createHash('md5').update(input).digest('hex')
}

/**
 * Compare a type to the existing type in the database.
 *
 * @returns `true` if the type has changed, `false` otherwise.
 */
async function hasTypeChanged(client: Client, type: SQLObject, stmt: string) {
  const tmpId = type.id.withSchema('nano')
  const tmpStmt = type.id.schema
    ? stmt.replace(type.id.schema, 'nano')
    : stmt.replace(type.id.name, 'nano.' + type.id.name)

  // Add the current type to the database (but under the "nano" schema), so we
  // can compare it to the existing type.
  await client.query(sql`
    CREATE SCHEMA IF NOT EXISTS nano;
    DROP TYPE IF EXISTS ${tmpId.toSQL()} CASCADE;
    ${sql.unsafe(tmpStmt)}
  `)

  const selectTypeById = (id: SQLIdentifier) => sql`
    SELECT
      a.attname AS column_name,
      a.atttypid AS type_id,
      a.attnum AS column_number
    FROM
      pg_attribute a
    JOIN
      pg_type t ON t.oid = a.attrelid
    WHERE
      t.typname = ${id.nameVal}
      AND t.typnamespace = ${id.schemaVal}::regnamespace
    ORDER BY
      a.attnum
  `

  const hasChanges = await client.scalar<boolean>(
    sql`
      WITH type1 AS (
        ${selectTypeById(type.id)}
      ),
      type2 AS (
        ${selectTypeById(tmpId)}
      )
      SELECT 
        EXISTS (
          SELECT 1
          FROM (
            SELECT * FROM type1
            EXCEPT
            SELECT * FROM type2
          ) diff1
        ) OR
        EXISTS (
          SELECT 1
          FROM (
            SELECT * FROM type2
            EXCEPT
            SELECT * FROM type1
          ) diff2
        ) AS has_changes;
    `,
  )

  return hasChanges
}

/**
 * Split a string of SQL statements into individual statements. This assumes
 * your SQL is properly indented.
 */
function splitStatements(stmts: string): string[] {
  const regex = /;\s*\n(?=\S)/g
  const statements = stmts.split(regex)
  if (statements.length > 1) {
    const falsePositive = /^(BEGIN|END|\$\$)/i
    statements.forEach((stmt, i) => {
      if (falsePositive.test(stmt)) {
        // Find the previous non-empty statement and merge them.
        for (let j = i - 1; j >= 0; j--) {
          if (statements[j] === '') {
            continue
          }
          statements[j] += stmt
          break
        }
        statements[i] = ''
      }
    })
    return sift(statements).map(stmt => stmt.trim() + ';')
  }
  return statements
}
