// @ts-check
/* eslint-env node */

'use strict'

const { AppViewIndexer } = require('@futuristick/bsky-indexer')

const main = async () => {
  const env = getEnv()

  const dbOptions = {
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: env.poolSize ?? 100,
    idleTimeoutMillis: 1_000,
  }

  const idResolverOptions = { plcUrl: env.didPlcUrl, timeout: 10_000 }

  const sub = new AppViewIndexer({
    identityResolverOptions: idResolverOptions,
    databaseOptions: dbOptions,
    service: env.repoProvider,
    unauthenticatedHandles: true,
    unauthenticatedCommits: true,
  })

  sub.start()

  process.on('SIGTERM', sub.destroy)
  process.on('disconnect', sub.destroy)
}

const getEnv = () => ({
  dbPostgresUrl: process.env.BSKY_DB_POSTGRES_URL || undefined,
  dbPostgresSchema: process.env.BSKY_DB_POSTGRES_SCHEMA || undefined,
  repoProvider: process.env.BSKY_REPO_PROVIDER || undefined,
  didPlcUrl:
    process.env.BSKY_DID_PLC_URL || process.env.DID_PLC_URL || undefined,
  poolSize: process.env.WORKER_DB_POOL_SIZE || undefined,
})

main()
