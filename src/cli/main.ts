#!/usr/bin/env node
import mri from 'mri'
import { log } from 'pg-nano/node'

async function main() {
  const [command, ...argv] = process.argv.slice(2)
  if (!command || command[0] === '-') {
    log.error('No command provided')
    process.exit(1)
  }
  switch (command) {
    case 'dev': {
      log.enableTimestamps(true)
      const command = await import('./commands/dev')
      const args: any = mri(argv, {
        string: ['dsn'],
        boolean: ['verbose', 'refreshPluginRole', 'noConfigBundling', 'noEmit'],
        alias: {
          v: 'verbose',
        },
      })
      args.root = process.cwd()
      await command.default(args)
      break
    }
    case 'init': {
      const command = await import('./commands/init')
      await command.default(process.cwd())
      break
    }
    default:
      log.error(`Unknown command: ${command}`)
      process.exit(1)
  }
}

main()
