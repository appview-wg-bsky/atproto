// @ts-check
/* eslint-env node */

'use strict'

const { AppViewIndexer } = require('@futuristick/bsky-indexer')
const { ServerConfig } = require('@atproto/bsky')

const main = async () => {
  const env = getEnv()
  const config = ServerConfig.readEnv()

  const dbOptions = {
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: 1000,
  }

  const idResolverOptions = { plcUrl: config.didPlcUrl }

  const sub = new AppViewIndexer({
    identityResolverOptions: idResolverOptions,
    databaseOptions: dbOptions,
    service: env.repoProvider,
  })

  sub.start()

  process.on('SIGTERM', sub.destroy)
  process.on('disconnect', sub.destroy)
}

const getEnv = () => ({
  dbPostgresUrl: process.env.BSKY_DB_POSTGRES_URL || undefined,
  dbPostgresSchema: process.env.BSKY_DB_POSTGRES_SCHEMA || undefined,
  repoProvider: process.env.BSKY_REPO_PROVIDER || undefined,
})

main()
