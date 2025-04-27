// @ts-check
/* eslint-env node */

'use strict'

import { FirehoseSubscription } from '@futuristick/bsky-indexer'

const main = async () => {
  const env = getEnv()

  /** @type {PgOptions} */
  const dbOptions = {
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: env.poolSize ?? 100,
    poolIdleTimeoutMs: 10_000,
  }

  const idResolverOptions = {
    plcUrl: env.didPlcUrl,
    timeout: 60_000,
  }

  const sub = new FirehoseSubscription({
    service: env.repoProvider,
    dbOptions,
    idResolverOptions,
    redisOptions: { url: env.redisUrl },
    minWorkers: env.minWorkers,
    maxWorkers: env.maxWorkers,
    onError: (err) =>
      console.error(...(err.cause ? [err.message, err.cause] : [err])),
    verbose: env.verbose,
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
  redisUrl: process.env.REDIS_URL || undefined,
  poolSize: process.env.BSKY_DB_POOL_SIZE
    ? parseInt(process.env.BSKY_DB_POOL_SIZE)
    : undefined,
  minWorkers: process.env.SUB_MIN_WORKERS
    ? parseInt(process.env.SUB_MIN_WORKERS)
    : undefined,
  maxWorkers: process.env.SUB_MAX_WORKERS
    ? parseInt(process.env.SUB_MAX_WORKERS)
    : undefined,
  verbose: process.env.LOG_VERBOSE === 'true',
})

void main()
