import { availableParallelism } from 'node:os'
import { setTimeout } from 'node:timers/promises'
import { SHARE_ENV } from 'node:worker_threads'
import { WindowsThreadPriority } from '@napi-rs/nice'
import { type RedisClientOptions, createClient } from '@redis/client'
import { FixedQueue, Piscina } from 'piscina'
import SharedMap from 'sharedmap'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'
import { WebSocketKeepAlive } from '@atproto/xrpc-server'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors.js'

let messagesReceived = 0,
  messagesProcessed = 0

export class FirehoseSubscription {
  private REDIS_SEQ_KEY = 'bsky_indexer:seq'
  private WORKER_PATH = new URL('./worker.js', import.meta.url).href

  protected firehose!: WebSocketKeepAlive
  protected piscina: Piscina
  protected redis?: ReturnType<typeof createClient>

  protected cursor = ''
  protected logStatsInterval: NodeJS.Timer | null = null
  protected saveCursorInterval: NodeJS.Timer | null = null

  protected settings = {
    minWorkers: availableParallelism() / 2,
    maxWorkers: availableParallelism() * 4,
    maxConcurrency: 1,
  }

  constructor(protected opts: FirehoseSubscriptionOptions) {
    if (this.opts.minWorkers) this.settings.minWorkers = this.opts.minWorkers
    if (this.opts.maxWorkers) this.settings.maxWorkers = this.opts.maxWorkers
    if (this.opts.maxConcurrency)
      this.settings.maxConcurrency = this.opts.maxConcurrency

    const { dbOptions, idResolverOptions } = this.opts
    const didLockMap = new SharedMap(2 ** 15, 256, 16)

    this.piscina = new Piscina({
      filename: this.WORKER_PATH,
      env: SHARE_ENV,
      minThreads: 20,
      maxThreads: 20,
      concurrentTasksPerWorker: this.settings.maxConcurrency,
      idleTimeout: Infinity,
      taskQueue: new FixedQueue(),
      niceIncrement:
        process.platform !== 'win32'
          ? 10
          : WindowsThreadPriority.ThreadPriorityAboveNormal,
      atomics: 'async',
      workerData: {
        dbOptions,
        idResolverOptions,
        didLockMap,
      },
    })

    if (this.opts.redisOptions)
      this.redis = createClient(this.opts.redisOptions)
  }

  async start(): Promise<void> {
    if (this.opts.redisOptions && !this.redis.isOpen) await this.redis.connect()

    const initialCursor = this.opts.redisOptions
      ? await this.redis.get(this.REDIS_SEQ_KEY)
      : null

    if (initialCursor) {
      console.log(`starting from initial cursor: ${initialCursor}`)
      this.cursor = initialCursor
    } else if (this.opts.cursor) {
      console.log(`starting from cursor: ${this.opts.cursor}`)
      this.cursor = this.opts.cursor.toString()
    } else console.log(`starting from latest`)

    this.firehose = new WebSocketKeepAlive({
      getUrl: async () =>
        `${this.opts.service}/xrpc/com.atproto.sync.subscribeRepos?cursor=${this.cursor}`,
    })

    if (!this.logStatsInterval)
      this.logStatsInterval = setInterval(() => {
        console.log(
          `${Math.round(messagesProcessed / 10)} / ${Math.round(messagesReceived / 10)} per sec (${Math.round((messagesProcessed / messagesReceived) * 100)}%) [${this.piscina.threads.length}]`,
        )
        messagesReceived = messagesProcessed = 0
      }, 10_000)

    if (this.opts.redisOptions && !this.saveCursorInterval) {
      this.saveCursorInterval = setInterval(async () => {
        await this.redis.set(this.REDIS_SEQ_KEY, this.cursor)
      }, 60_000)
    }

    try {
      await setTimeout(10_000)

      for await (const c of this.firehose) {
        messagesReceived++
        // unsure why this is necessary, but the chunk ArrayBuffer otherwise sometimes
        // ends up detached by the time it gets to the worker
        const chunk = new Uint8Array(c)
        void this.processChunk(chunk)
      }
    } catch (err) {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
      return this.start()
    }
  }

  protected processChunk = (chunk: Uint8Array) => {
    void this.piscina
      .run(Piscina.move(chunk))
      .then((res) => {
        if (res?.success) {
          messagesProcessed++
        } else if (res?.error) {
          this.opts.onError?.(new FirehoseWorkerError(res.error))
        }
        if (res?.cursor && !isNaN(res.cursor)) {
          this.cursor = `${res.cursor}`
        }
      })
      .catch((err) => {
        this.opts.onError?.(new FirehoseWorkerError(err))
      })
  }

  async destroy() {
    console.warn('destroying indexer')
    await this?.piscina?.close({ force: true })
    await this?.redis?.quit()
  }
}

export interface FirehoseSubscriptionOptions {
  service: string
  dbOptions: PgOptions
  redisOptions?: RedisClientOptions
  idResolverOptions?: IdentityResolverOpts
  minWorkers?: number | undefined
  maxWorkers?: number | undefined
  maxConcurrency?: number
  onError?: (err: Error) => void
  cursor?: number
  verbose?: boolean
}
