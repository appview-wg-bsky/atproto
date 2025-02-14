// @ts-check
/* eslint-env node */

'use strict'

const { FirehoseSubscription } = require('@futuristick/firehose-subscription')

const main = async () => {
  const env = getEnv()

  const dbOptions = {
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: env.poolSize ?? 500,
  }

  const idResolverOptions = {
    plcUrl: env.didPlcUrl,
    timeout: 30_000,
  }

  const sub = new FirehoseSubscription({
    service: env.repoProvider,
    dbOptions,
    idResolverOptions,
    minWorkers: 4,
    maxWorkers: 16,
    onError: (err) => console.error(err),
  })

  void sub.start()

  process.on('SIGTERM', sub.destroy)
  process.on('disconnect', sub.destroy)
}

const getEnv = () => ({
  dbPostgresUrl: process.env.BSKY_DB_POSTGRES_URL || undefined,
  dbPostgresSchema: process.env.BSKY_DB_POSTGRES_SCHEMA || undefined,
  repoProvider: process.env.BSKY_REPO_PROVIDER || undefined,
  didPlcUrl:
    process.env.BSKY_DID_PLC_URL || process.env.DID_PLC_URL || undefined,
  poolSize: process.env.BSKY_DB_POOL_SIZE
    ? parseInt(process.env.BSKY_DB_POOL_SIZE)
    : undefined,
})

void main()
