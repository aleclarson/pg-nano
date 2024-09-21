import copy from 'esbuild-plugin-copy'
import licenses from 'esbuild-plugin-license'
import { readFileSync } from 'node:fs'
import { defineConfig, type Options } from 'tsup'

const commonOptions = {
  format: ['esm'],
  external: ['pg-native', 'pg-nano', 'debug'],
  treeshake: 'smallest',
  minifySyntax: !process.env.DEV,
  dts: !process.env.DEV && {
    compilerOptions: JSON.parse(readFileSync('tsconfig.json', 'utf-8'))
      .compilerOptions,
  },
} satisfies Options

export default defineConfig([
  {
    ...commonOptions,
    entry: {
      'pg-nano': 'src/core/mod.ts',
    },
    esbuildPlugins: [
      licenses(),
      copy({
        assets: {
          from: 'packages/pg-native/package.json',
          to: 'node_modules/pg-native',
        },
      }),
    ],
  },
  {
    ...commonOptions,
    entry: {
      'pg-nano/config': 'src/config/config.ts',
      'pg-nano/plugin': 'src/plugin/plugin.ts',
    },
  },
  {
    ...commonOptions,
    entry: { main: 'src/cli/main.ts' },
    outDir: 'dist/pg-nano/cli',
    dts: false,
  },
  {
    ...commonOptions,
    entry: { index: 'packages/pg-native/src/index.ts' },
    outDir: 'dist/node_modules/pg-native',
    esbuildPlugins: getProductionEsbuildPlugins(),
  },
])

function getProductionEsbuildPlugins() {
  if (process.env.DEV) {
    return []
  }
  return [
    licenses(),
    copy({
      assets: [
        {
          from: 'packages/pg-native/LICENSE',
          to: '.',
        },
      ],
    }),
  ]
}
