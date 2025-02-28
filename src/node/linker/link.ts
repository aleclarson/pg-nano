import { SQLIdentifier } from '../parser/identifier.js'
import type { PgSchema } from '../parser/parse.js'
import type { PgCastStmt, PgObjectStmt } from '../parser/types.js'
import { TopologicalSet } from './topologicalSet.js'

/**
 * Populate the dependencies and dependents of each object statement. Returns a
 * topological set of the objects.
 */
export function linkStatements(schema: PgSchema) {
  const objectsByName = new Map<string, PgObjectStmt>()

  const idSortedObjects = schema.objects.toSorted((left, right) => {
    const cmp = (left.id.schema ?? 'public').localeCompare(
      right.id.schema ?? 'public',
    )
    if (cmp !== 0) {
      return cmp
    }
    return left.id.name.localeCompare(right.id.name)
  })

  for (const object of idSortedObjects) {
    objectsByName.set(object.id.toQualifiedName(), object)
  }

  const link = (stmt: PgCastStmt | PgObjectStmt, id: SQLIdentifier) => {
    const dep = objectsByName.get(id.toQualifiedName())
    if (dep && dep !== stmt) {
      stmt.dependencies.add(dep)
      dep.dependents.add(stmt)
    }
  }

  // Determine dependencies
  for (const stmt of idSortedObjects) {
    if (stmt.kind === 'schema') {
      continue
    }
    if (stmt.id.schema) {
      link(stmt, new SQLIdentifier('', stmt.id.schema))
    }
    if (stmt.kind === 'routine') {
      for (const param of stmt.params) {
        link(stmt, param.type.toIdentifier())
      }
      if (!stmt.returnType) {
        continue
      }
      if (stmt.returnType instanceof SQLIdentifier) {
        link(stmt, stmt.returnType.toIdentifier())
      } else {
        for (const columnDef of stmt.returnType) {
          link(stmt, columnDef.type.toIdentifier())
        }
      }
    } else {
      if ('columns' in stmt) {
        for (const column of stmt.columns) {
          link(stmt, column.type.toIdentifier())
        }
      }
      if ('refs' in stmt) {
        for (const ref of stmt.refs) {
          link(stmt, ref)
        }
      }
    }
  }

  for (const stmt of schema.casts) {
    link(stmt, stmt.sourceId)
    link(stmt, stmt.targetId)
  }

  return new TopologicalSet(idSortedObjects)
}
