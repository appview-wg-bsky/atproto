import { cpus } from 'node:os'
import * as path from 'node:path'
import { Worker } from 'node:worker_threads'
import { Redis } from 'ioredis'
import { cborDecodeMulti } from '@atproto/common'
import { WebSocketKeepAlive } from '@atproto/xrpc-server/dist/stream/websocket-keepalive'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors'
import {
  FirehoseSubscriptionOptions,
  WorkerMessage,
  WorkerResponse,
} from './types'

const WORKER_PATH = path.join(__dirname, 'worker.js')
export const REDIS_STREAM_NAME = 'bsky_indexer:firehose'
export const REDIS_GROUP_NAME = 'bsky_indexer_consumers'

export class FirehoseSubscription {
  private redis: Redis
  private workers: Worker[] = []
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

    // :/
    if (typeof this.opts.redisOptions === 'string') {
      this.redis = new Redis(this.opts.redisOptions)
    } else {
      this.redis = new Redis(this.opts.redisOptions)
    }

    for (let i = 0; i < this.settings.minWorkers; i++) {
      this.addWorker()
    }
  }

  private addWorker() {
    const worker = new Worker(WORKER_PATH)
    this.workers.push(worker)
    this.setupWorker(worker)
  }

  private setupWorker(worker: Worker, workerId?: number) {
    workerId ??= this.workers.indexOf(worker)
    if (workerId === -1) throw new Error('Worker not found')

    worker.postMessage({
      type: 'init',
      dbOptions: this.opts.dbOptions,
      redisOptions: this.opts.redisOptions,
      idResolverOptions: this.opts.idResolverOptions ?? {},
    } satisfies WorkerMessage)

    worker.on('message', (msg: WorkerResponse) => {
      if (msg.type === 'error') {
        this.opts.onError?.(msg.error)
      }
    })

    worker.on('error', (err) => {
      this.opts.onError?.(new FirehoseWorkerError(err))
      this.replaceWorker(workerId, worker)
    })
  }

  private replaceWorker(workerId: number, worker: Worker) {
    void worker.terminate()

    if (!this.destroyed) {
      const newWorker = new Worker(WORKER_PATH)
      this.setupWorker(newWorker, workerId)
      this.workers[workerId] = newWorker
    }
  }

  private async checkScaling() {
    if (this.workers.length === 0) return

    const [pending] = await this.redis.xpending(
      REDIS_STREAM_NAME,
      REDIS_GROUP_NAME,
    )
    if (typeof pending !== 'number' || isNaN(pending)) return

    if (pending > this.totalPending) {
      this.needsToScale++
    } else this.needsToScale = Math.max(0, this.needsToScale - 1)
    this.totalPending = pending

    // Scale up if the pending count has increased for the last 3 cycles
    if (this.needsToScale >= 3) {
      if (this.workers.length >= this.settings.maxWorkers) {
        console.warn(`pending count ${pending} but max workers reached`)
        return
      }

      const newWorkersCount = Math.min(
        this.settings.maxWorkers,
        this.workers.length + Math.ceil(this.workers.length * 0.5), // Add 50% more workers
      )
      console.log(
        `spawning ${newWorkersCount - this.workers.length} workers for a total of ${newWorkersCount}`,
      )
      while (this.workers.length < newWorkersCount) {
        this.addWorker()
      }
      return
    }
    // Scale down if we're well ahead
    else if (pending < 500 && this.workers.length > this.settings.minWorkers) {
      const targetWorkers = this.workers.length - 1
      if (targetWorkers < this.settings.minWorkers) return

      console.log(`killing 1 worker for a total of ${targetWorkers}`)
      const toTerminate = this.workers.pop()
      if (!toTerminate) return
      await toTerminate.terminate()
      console.log(`killed worker ${toTerminate.threadId}`)
    }
  }

  async start() {
    try {
      await this.redis.xgroup(
        'CREATE',
        REDIS_STREAM_NAME,
        REDIS_GROUP_NAME,
        '$',
        'MKSTREAM',
      )
    } catch {
      // will throw if the stream already exists
    }

    const recoverFromCursor = await this.redis
      .xrange(REDIS_STREAM_NAME, '-', '+', 'COUNT', 1)
      .then((res) => res?.[0]?.[0])

    try {
      this.ws = new WebSocketKeepAlive({
        getUrl: async () => {
          const cursor = recoverFromCursor || `${this.opts.cursor}`
          const params = cursor ? { cursor } : undefined
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
    const decoded = cborDecodeMulti(chunk)
    const seq = (decoded?.[1] as any)?.seq
    if (seq !== undefined) {
      await this.redis.xaddBuffer(
        REDIS_STREAM_NAME,
        'MAXLEN',
        '~',
        20_000,
        seq,
        Buffer.from(chunk),
      )
    }
  }

  async destroy() {
    this.destroyed = true
    if (this.scaleCheckInterval) {
      clearInterval(this.scaleCheckInterval)
    }
    this.ws?.ws?.close()

    await Promise.all(this.workers.map((worker) => worker.terminate()))
  }
}
