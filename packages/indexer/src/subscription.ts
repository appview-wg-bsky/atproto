import { cpus } from 'node:os'
import * as path from 'node:path'
import { SHARE_ENV, Worker } from 'node:worker_threads'
import { createClient } from '@redis/client'
import { WebSocketKeepAlive } from '@atproto/xrpc-server/dist/stream/websocket-keepalive'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors'
import { FirehoseSubscriptionOptions, WorkerData, WorkerResponse } from './util'

const WORKER_PATH = path.join(__dirname, 'worker.js')
export const REDIS_STREAM_NAME = 'bsky_indexer:firehose'
export const REDIS_GROUP_NAME = 'bsky_indexer_consumers'
const REDIS_SEQ_KEY = 'bsky_indexer:seq'

export class FirehoseSubscription {
  private redis: ReturnType<typeof createClient>
  private workers: Map<number, { worker: Worker; data: WorkerData }> = new Map()
  private ws: WebSocketKeepAlive | null = null
  private destroyed = false
  private scaleCheckInterval: NodeJS.Timeout | null = null
  private needsToScale = 0
  private totalPending = 0

  private settings = {
    scaleCheckIntervalMs: 5_000,
    minWorkers: 2,
    maxWorkers: cpus().length,
  }

  constructor(private opts: FirehoseSubscriptionOptions) {
    if (this.opts.minWorkers) this.settings.minWorkers = this.opts.minWorkers
    if (this.opts.maxWorkers) this.settings.maxWorkers = this.opts.maxWorkers
    if (this.opts.scaleCheckIntervalMs)
      this.settings.scaleCheckIntervalMs = this.opts.scaleCheckIntervalMs

    this.redis = createClient(this.opts.redisOptions)
    this.redis.on('error', (err) => {
      this.opts.onError?.(err)
    })

    for (let i = 0; i < this.settings.minWorkers; i++) {
      this.setupWorker()
    }
  }

  private setupWorker() {
    const { dbOptions, redisOptions, idResolverOptions } = this.opts
    const worker = new Worker(WORKER_PATH, {
      workerData: {
        dbOptions,
        redisOptions,
        idResolverOptions,
      },
      env: SHARE_ENV,
    })

    this.workers.set(worker.threadId, {
      worker,
      data: {},
    })

    worker.on('message', (msg: WorkerResponse) => {
      const data = this.workers.get(worker.threadId)?.data ?? {}

      if (msg.type === 'processed') {
        // logVerbose(
        //   `received from worker [${worker.threadId}]: ${msg.id}`,
        //   0.0005,
        // )

        void this.onProcessed(msg.id).catch((err) => this.opts.onError?.(err))

        typeof data.processedPerMinute === 'number'
          ? (data.processedPerMinute += 1)
          : (data.processedPerMinute = 1)

        typeof data.averageProcessingTime === 'number'
          ? (data.averageProcessingTime =
              (data.averageProcessingTime * data.processedPerMinute +
                msg.time) /
              (data.processedPerMinute + 1))
          : (data.averageProcessingTime = msg.time)

        this.workers.set(worker.threadId, {
          worker,
          data,
        })
      } else if (msg.type === 'maxed') {
        data.maxed = msg.maxed
        this.workers.set(worker.threadId, {
          worker,
          data,
        })
      } else if (msg.type === 'seq') {
        const seq = msg.seq || null
        if (seq) {
          void this.redis.set(REDIS_SEQ_KEY, seq).catch((err) => {
            this.opts.onError?.(err)
          })
        }
      } else if (msg.type === 'error') {
        this.opts.onError?.(msg.error)
      }
    })

    worker.on('error', (err) => {
      this.opts.onError?.(new FirehoseWorkerError(err))
      this.workers.delete(worker.threadId)
      worker.terminate().then(() => this.setupWorker())
    })
  }

  private async checkScaling() {
    if (this.workers.size === 0) return

    const streamLength = await this.redis.xLen(REDIS_STREAM_NAME)
    if (typeof streamLength !== 'number' || isNaN(streamLength)) return

    // logVerbose(
    //   `pending: ${streamLength} | previously: ${this.totalPending} | scaling: ${this.needsToScale}`,
    // )

    // We need to scale soon if the backlog is growing or if it's just too big
    if (
      (streamLength > this.totalPending && streamLength > 5_000) ||
      streamLength > 75_000
    ) {
      this.needsToScale++
    } else this.needsToScale = Math.max(0, this.needsToScale - 1)
    this.totalPending = streamLength

    // Scale up if the pending count has increased for the last 3 cycles
    if (this.needsToScale >= 3) {
      if (this.workers.size >= this.settings.maxWorkers) {
        console.warn(`pending count ${streamLength} but max workers reached`)
        return
      }

      const newWorkersCount =
        Math.min(
          this.settings.maxWorkers,
          this.workers.size + Math.ceil(this.workers.size * 0.33), // Add 33% more workers
        ) - this.workers.size
      console.log(
        `spawning ${newWorkersCount} workers for a total of ${newWorkersCount + this.workers.size}`,
      )
      for (let i = 0; i < newWorkersCount; i++) {
        this.setupWorker()
      }

      this.needsToScale = 0
      return
    }
    // Scale down if we're well ahead
    else if (
      streamLength < 500 &&
      this.workers.size > this.settings.minWorkers
    ) {
      const targetWorkers = this.workers.size - 1
      if (targetWorkers < this.settings.minWorkers) return

      console.log(`killing 1 worker for a total of ${targetWorkers}`)
      const toTerminate = [...this.workers.values()].sort(
        (a, b) => b.worker.threadId - a.worker.threadId,
      )[0]?.worker

      if (!toTerminate) return

      const { threadId } = toTerminate
      await toTerminate.terminate()
      this.workers.delete(threadId)
      console.log(`killed worker ${threadId}`)

      return
    }

    // If more than half of workers are maxed out, add one
    if (
      [...this.workers.values()].filter(({ data }) => data.maxed).length >=
      this.workers.size / 2
    ) {
      console.log(`spawning 1 worker for a total of ${this.workers.size + 1}`)
      this.setupWorker()
      return
    }
  }

  async start() {
    await this.redis.connect()

    try {
      await this.redis.xGroupCreate(REDIS_STREAM_NAME, REDIS_GROUP_NAME, '$', {
        MKSTREAM: true,
      })
    } catch {
      // will throw if the stream already exists
    }

    const initialCursor = await this.redis.get(REDIS_SEQ_KEY)

    if (initialCursor)
      console.log(`starting from initial cursor: ${initialCursor}`)
    else if (this.opts.cursor)
      console.log(`starting from cursor: ${this.opts.cursor}`)
    else console.log(`starting from latest`)

    try {
      this.ws = new WebSocketKeepAlive({
        getUrl: async () => {
          const cursor = initialCursor || `${this.opts.cursor}`
          const params =
            cursor && !isNaN(parseInt(cursor)) ? { cursor } : undefined
          const query = new URLSearchParams(params).toString()
          return `${this.opts.service}/xrpc/com.atproto.sync.subscribeRepos${query ? '?' + query : ''}`
        },
      })

      if (this.scaleCheckInterval) clearInterval(this.scaleCheckInterval)
      // Only start the scale check after 30 seconds, to give the workers time to get going
      setTimeout(() => {
        this.scaleCheckInterval = setInterval(
          () => this.checkScaling(),
          this.settings.scaleCheckIntervalMs,
        )
      }, 30_000)

      setInterval(() => this.logWorkerStats(), 30_000)

      for await (const chunk of this.ws) {
        if (this.destroyed) break
        void this.handleMessage(chunk).catch((err) => {
          this.opts.onError?.(new FirehoseSubscriptionError(err))
        })
      }
    } catch (err) {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
      if (!this.destroyed) {
        this.ws?.ws?.close()
        await this.start()
      }
    }
  }

  async handleMessage(chunk: Uint8Array) {
    return this.redis.xAdd(REDIS_STREAM_NAME, '*', {
      data: Buffer.from(chunk).toString('base64'),
    })
  }

  async onProcessed(id: string) {
    await this.redis.xDel(REDIS_STREAM_NAME, id)
  }

  logWorkerStats() {
    const workers = [...this.workers.values()].sort(
      (a, b) => b.worker.threadId - a.worker.threadId,
    )

    console.log(
      workers
        .map(({ data }) => {
          const processed = ((data.processedPerMinute ?? 0) / 60).toFixed(2)
          const avg = data.averageProcessingTime?.toFixed(2) ?? '?'

          data.processedPerMinute = null
          data.averageProcessingTime = null

          return `${processed}/s; ${avg}ms/`
        })
        .join(' | '),
    )
  }

  async destroy() {
    this.destroyed = true
    if (this.scaleCheckInterval) {
      clearInterval(this.scaleCheckInterval)
    }
    this.ws?.ws?.close()

    await Promise.all(
      [...this.workers.values()].map(({ worker }) => worker.terminate()),
    )
  }
}
