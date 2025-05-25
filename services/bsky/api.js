// @ts-check
/* eslint-env node */
/* eslint-disable import/order */

'use strict'

const dd = require('dd-trace')

dd.tracer
  .init()
  .use('http2', {
    client: true, // calls into dataplane
    server: false,
  })
  .use('express', {
    hooks: {
      request: (span, req) => {
        maintainXrpcResource(span, req)
      },
    },
  })

// modify tracer in order to track calls to dataplane as a service with proper resource names
const DATAPLANE_PREFIX = '/bsky.Service/'
const origStartSpan = dd.tracer._tracer.startSpan
dd.tracer._tracer.startSpan = function (name, options) {
  if (
    name !== 'http.request' ||
    options?.tags?.component !== 'http2' ||
    !options?.tags?.['http.url']
  ) {
    return origStartSpan.call(this, name, options)
  }
  const uri = new URL(options.tags['http.url'])
  if (!uri.pathname.startsWith(DATAPLANE_PREFIX)) {
    return origStartSpan.call(this, name, options)
  }
  options.tags['service.name'] = 'dataplane-bsky'
  options.tags['resource.name'] = uri.pathname.slice(DATAPLANE_PREFIX.length)
  return origStartSpan.call(this, name, options)
}

// Tracer code above must come before anything else
const path = require('node:path')
const assert = require('node:assert')
const cluster = require('node:cluster')
const { Secp256k1Keypair } = require('@atproto/crypto')
const bsky = require('@atproto/bsky') // import all bsky features

const appview = async () => {
  const env = getEnv()
  const config = bsky.ServerConfig.readEnv()
  assert(env.serviceSigningKey, 'must set BSKY_SERVICE_SIGNING_KEY')
  assert(env.dbPostgresUrl, 'must set BSKY_DB_POSTGRES_URL')
  const signingKey = await Secp256k1Keypair.import(env.serviceSigningKey)

  // starts: involve logics in packages/dev-env/src/bsky.ts >>>>>>>>>>>>>
  // Separate migration db in case migration changes some connection state that we need in the tests, e.g. "alter database ... set ..."
  const migrationDb = new bsky.Database({
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
  })
  await migrationDb.migrateToLatestOrThrow()
  await migrationDb.close()

  const db = new bsky.Database({
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: env.dbPoolSize,
  })

  // ends: involve logics in packages/dev-env/src/bsky.ts   <<<<<<<<<<<<<

  const server = bsky.BskyAppView.create({ config, signingKey })
  await server.start()
  // Graceful shutdown (see also https://aws.amazon.com/blogs/containers/graceful-shutdowns-with-ecs/)
  const shutdown = async () => {
    await server.destroy()
    await db.close()
  }
  process.on('SIGTERM', shutdown)
  process.on('disconnect', shutdown) // when clustering
}

const dataplane = async () => {
  const env = getEnv()
  const config = bsky.ServerConfig.readEnv()
  assert(env.dbPostgresUrl, 'must set BSKY_DB_POSTGRES_URL')
  assert(env.bsyncPort, 'must set BSKY_BSYNC_PORT')
  assert(env.dataplanePort, 'must set BSKY_DATAPLANE_PORT')

  const db = new bsky.Database({
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: env.dbPoolSize,
  })

  const bsync = await bsky.MockBsync.create(db, env.bsyncPort)

  const server = await bsky.DataPlaneServer.create(
    db,
    env.dataplanePort,
    config.didPlcUrl,
  )
  // Graceful shutdown (see also https://aws.amazon.com/blogs/containers/graceful-shutdowns-with-ecs/)
  const shutdown = async () => {
    await server.destroy()
    await bsync.destroy()
    await db.close()
  }
  process.on('SIGTERM', shutdown)
  process.on('disconnect', shutdown) // when clustering
}

const getEnv = () => ({
  serviceSigningKey: process.env.BSKY_SERVICE_SIGNING_KEY || undefined,
  dbPostgresUrl: process.env.BSKY_DB_POSTGRES_URL || undefined,
  dbPostgresSchema: process.env.BSKY_DB_POSTGRES_SCHEMA || undefined,
  dbPoolSize: maybeParseInt(process.env.BSKY_DB_POOL_SIZE) || undefined,
  dataplanePort: maybeParseInt(process.env.BSKY_DATAPLANE_PORT) || undefined,
  bsyncPort: maybeParseInt(process.env.BSKY_BSYNC_PORT) || undefined,
  migration: process.env.ENABLE_MIGRATIONS === 'true' || undefined,
  repoProvider: process.env.BSKY_REPO_PROVIDER || undefined,
})

const maybeParseInt = (str) => {
  if (!str) return
  const int = parseInt(str, 10)
  if (isNaN(int)) return
  return int
}

const maintainXrpcResource = (span, req) => {
  // Show actual xrpc method as resource rather than the route pattern
  if (span && req.originalUrl?.startsWith('/xrpc/')) {
    span.setTag(
      'resource.name',
      [
        req.method,
        path.posix.join(req.baseUrl || '', req.path || '', '/').slice(0, -1), // Ensures no trailing slash
      ]
        .filter(Boolean)
        .join(' '),
    )
  }
}

const workerCount = maybeParseInt(process.env.CLUSTER_WORKER_COUNT)

if (workerCount) {
  if (workerCount < 3) {
    throw new Error('CLUSTER_WORKER_COUNT must be at least 3')
  }

  if (cluster.isPrimary) {
    let teardown = false

    const workers = new Set()

    const spawnWorker = (type) => {
      const worker = cluster.fork({ WORKER_TYPE: type })
      worker.on('exit', onExit(worker, type))
      workers.add(worker)
    }

    const onExit = (worker, type) => () => {
      workers.delete(worker)
      if (!teardown) spawnWorker(type)
    }

    console.log(`primary ${process.pid} is running`)

    spawnWorker('dataplane')
    for (let i = 1; i < workerCount; ++i) {
      spawnWorker('appview')
    }
    process.on('SIGTERM', () => {
      teardown = true
      console.log('disconnecting workers')
      workers.forEach((w) => w.disconnect())
    })
  } else {
    console.log(
      `worker ${process.pid} is running as ${process.env.WORKER_TYPE}`,
    )
    if (process.env.WORKER_TYPE === 'appview') {
      appview()
    } else if (process.env.WORKER_TYPE === 'dataplane') {
      dataplane()
    } else {
      throw new Error(`unknown worker type ${process.env.WORKER_TYPE}`)
    }
  }
} else {
  dataplane()
  appview() // non-clustering
}
