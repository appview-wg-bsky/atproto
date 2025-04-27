import { availableParallelism } from 'node:os'
import { SHARE_ENV } from 'node:worker_threads'
import { type RedisClientOptions, createClient } from '@redis/client'
import { type Event, Firehose } from '@skyware/firehose'
import PQueue from 'p-queue'
import { FixedQueue, Piscina } from 'piscina'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors.js'
import type { default as worker } from './worker.js'

declare module 'piscina' {
  interface PiscinaTask {
    [Piscina.queueOptionsSymbol]: {
      did: string
      seq: number
      attempt?: number
    }
  }
}

const REDIS_SEQ_KEY = 'bsky_indexer:seq'
const WORKER_PATH = new URL('./worker.js', import.meta.url).href
const MAX_ATTEMPTS = 5

let messagesReceived = 0,
  messagesProcessed = 0

export class FirehoseSubscription {
  protected firehose: Firehose
  protected piscina: Piscina
  protected redis: ReturnType<typeof createClient>
  protected queues = new Map<string, PQueue>()

  protected settings = {
    minWorkers: 16,
    maxWorkers: availableParallelism() * 4,
    maxConcurrency: 50,
  }

  constructor(protected opts: FirehoseSubscriptionOptions) {
    if (this.opts.minWorkers) this.settings.minWorkers = this.opts.minWorkers
    if (this.opts.maxWorkers) this.settings.maxWorkers = this.opts.maxWorkers
    if (this.opts.maxConcurrency)
      this.settings.maxConcurrency = this.opts.maxConcurrency

    this.firehose = new Firehose({
      relay: this.opts.service,
      cursor: this.opts.cursor?.toString(),
    })

    const { dbOptions, idResolverOptions } = this.opts

    this.piscina = new Piscina({
      filename: WORKER_PATH,
      env: SHARE_ENV,
      minThreads: this.settings.minWorkers,
      maxThreads: this.settings.maxWorkers,
      concurrentTasksPerWorker: this.settings.maxConcurrency,
      idleTimeout: 30_000,
      taskQueue: new FixedQueue(),
      workerData: {
        dbOptions,
        idResolverOptions,
      },
    })

    this.redis = createClient(this.opts.redisOptions)
  }

  async start() {
    await this.redis.connect()

    const initialCursor = await this.redis.get(REDIS_SEQ_KEY)

    if (initialCursor)
      console.log(`starting from initial cursor: ${initialCursor}`)
    else if (this.opts.cursor)
      console.log(`starting from cursor: ${this.opts.cursor}`)
    else console.log(`starting from latest`)

    this.firehose = new Firehose({
      relay: this.opts.service,
      cursor: initialCursor ?? this.opts.cursor?.toString(),
    })

    this.firehose.on('commit', this.queueMessage.bind(this))
    this.firehose.on('account', this.queueMessage.bind(this))
    this.firehose.on('identity', this.queueMessage.bind(this))
    this.firehose.on('sync', this.queueMessage.bind(this))
    this.firehose.on('info', (info) => {
      console.info(`[FIREHOSE] ${info}`)
    })
    this.firehose.on('error', (err) => {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
      this.firehose?.close()
      return this.start()
    })
    this.firehose.on('close', () => {
      console.log('firehose closed, exiting...')
      this.destroy()
    })

    this.firehose.start()

    setInterval(() => {
      console.log(
        `${Math.round(messagesProcessed / 10)} / ${Math.round(messagesReceived / 10)} per sec (${Math.round((messagesProcessed / messagesReceived) * 100)}%) [${this.piscina.threads.length}]`,
      )
      messagesReceived = messagesProcessed = 0
    }, 10_000)
  }

  protected queueMessage(message: Event, attempt = 0) {
    messagesReceived++

    const { did, seq } = didAndSeq(message)

    if (attempt > MAX_ATTEMPTS) {
      console.error(`max attempts reached for ${did} ${seq}`, message)
      return
    }

    // Ensure messages for a given did are processed sequentially
    let queue = this.queues.get(did)
    if (!queue) {
      queue = new PQueue({ concurrency: 1, timeout: 120_000 })
      this.queues.set(did, queue)
    }

    // higher seq -> lower priority
    queue
      .add(() => this.processMessage(message), {
        priority: Number.MAX_SAFE_INTEGER - seq,
      })
      .then((res) => {
        if (res.success) {
          if (queue.size === 0) this.queues.delete(did)
          messagesProcessed++
        } else {
          if (res.error) {
            console.warn(new FirehoseWorkerError(res.error))
          }
          return this.queueMessage(message, attempt + 1)
        }
      })
  }

  protected processMessage(message: Event): ReturnType<typeof worker> {
    return this.piscina.run(message, {
      transferList: 'blocks' in message ? [message.blocks.buffer] : [],
    })
  }

  async destroy() {
    this.firehose?.close()
    await this.piscina.close({ force: true })
    await this.redis.quit()
  }
}

function didAndSeq(message: Event) {
  if ('did' in message) return { did: message.did, seq: message.seq }
  else if ('repo' in message) return { did: message.repo, seq: message.seq }
  throw new Error(`message missing did or repo ${JSON.stringify(message)}`)
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
