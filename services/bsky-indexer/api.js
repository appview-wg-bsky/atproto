// @ts-check
/* eslint-env node */

'use strict'

const bsky = require('@atproto/bsky')
const { IdResolver } = require('@atproto/identity')

const main = async () => {
  const env = getEnv()

  const db = new bsky.Database({
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: env.poolSize ?? 3000,
  })

  const idResolver = new IdResolver({
    plcUrl: env.didPlcUrl,
  })

  const sub = new bsky.RepoSubscription({
    service: env.repoProvider,
    db,
    idResolver: idResolver,
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
