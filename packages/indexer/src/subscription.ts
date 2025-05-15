import { availableParallelism } from 'node:os'
import { type RedisClientOptions, createClient } from '@redis/client'
import { WebSocket } from 'partysocket'
import { DynamicThreadPool } from 'poolifier-web-worker'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors.js'
import type { WorkerInput, WorkerOutput } from './worker.js'

let messagesReceived = 0,
  messagesProcessed = 0

export class FirehoseSubscription {
  private REDIS_SEQ_KEY = 'bsky_indexer:seq'
  private WORKER_PATH = new URL('./worker.js', import.meta.url)

  protected firehose!: WebSocket
  protected pool: DynamicThreadPool<WorkerInput, WorkerOutput>
  protected redis?: ReturnType<typeof createClient>

  protected cursor = ''
  protected logStatsInterval: NodeJS.Timer | null = null
  protected saveCursorInterval: NodeJS.Timer | null = null

  protected settings = {
    minWorkers: availableParallelism() / 2,
    maxWorkers: availableParallelism() * 2,
    maxConcurrency: 50,
  }

  constructor(protected opts: FirehoseSubscriptionOptions) {
    if (this.opts.minWorkers) this.settings.minWorkers = this.opts.minWorkers
    if (this.opts.maxWorkers) this.settings.maxWorkers = this.opts.maxWorkers
    if (this.opts.maxConcurrency)
      this.settings.maxConcurrency = this.opts.maxConcurrency

    const { dbOptions, idResolverOptions } = this.opts

    this.pool = new DynamicThreadPool(
      this.settings.minWorkers,
      this.settings.maxWorkers,
      this.WORKER_PATH,
      {
        enableTasksQueue: true,
        workerChoiceStrategy: 'INTERLEAVED_WEIGHTED_ROUND_ROBIN',
        tasksQueueOptions: {
          concurrency: this.settings.maxConcurrency,
          size: 100,
        },
        workerOptions: {
          workerData: {
            dbOptions,
            idResolverOptions,
          },
          env: {},
        },
      },
    )

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

    this.firehose = new WebSocket(
      () =>
        `${this.opts.service}/xrpc/com.atproto.sync.subscribeRepos?cursor=${this.cursor}`,
    )
    this.firehose.binaryType = 'arraybuffer' // https://github.com/partykit/partykit/issues/774

    this.firehose.onmessage = ({ data }: { data: ArrayBuffer }) => {
      const chunk = new Uint8Array(data)
      messagesReceived++
      void this.pool
        .execute({ chunk }, undefined, [chunk.buffer])
        .then(this.onProcessed)
        .catch((e: unknown) =>
          this.opts.onError?.(new FirehoseSubscriptionError(e)),
        )
    }

    this.firehose.onerror = (e) =>
      this.opts.onError?.(new FirehoseSubscriptionError(e.error))
    if (!this.logStatsInterval)
      this.logStatsInterval = setInterval(() => {
        console.log(
          `${Math.round(messagesProcessed / 10)} / ${Math.round(messagesReceived / 10)} per sec (${Math.round((messagesProcessed / messagesReceived) * 100)}%) [${this.pool.info.workerNodes} workers; ${this.pool.info.queuedTasks} queued; ${this.pool.info.executingTasks} executing]`,
        )
        messagesReceived = messagesProcessed = 0
      }, 10_000)

    if (this.opts.redisOptions && !this.saveCursorInterval) {
      this.saveCursorInterval = setInterval(async () => {
        await this.redis.set(this.REDIS_SEQ_KEY, this.cursor)
      }, 60_000)
    }
  }

  protected onProcessed = (res: WorkerOutput) => {
    if (res?.success) {
      messagesProcessed++
    } else if (res?.error) {
      this.opts.onError?.(new FirehoseWorkerError(res.error))
    }
    if (res?.cursor && !isNaN(res.cursor)) {
      this.cursor = `${res.cursor}`
    }
  }

  async destroy() {
    console.warn('destroying indexer')
    await this?.pool?.destroy()
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
