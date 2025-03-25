import { cpus } from 'node:os'
import * as path from 'node:path'
import { Worker } from 'node:worker_threads'
import { createClient } from '@redis/client'
import { cborDecodeMulti } from '@atproto/common'
import { WebSocketKeepAlive } from '@atproto/xrpc-server/dist/stream/websocket-keepalive'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors'
import { FirehoseSubscriptionOptions, WorkerResponse } from './types'

const WORKER_PATH = path.join(__dirname, 'worker.js')
export const REDIS_STREAM_NAME = 'bsky_indexer:firehose'
export const REDIS_GROUP_NAME = 'bsky_indexer_consumers'

export class FirehoseSubscription {
  private redis: ReturnType<typeof createClient>
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

    this.redis = createClient(this.opts.redisOptions)
    this.redis.on('error', (err) => {
      this.opts.onError?.(err)
    })

    for (let i = 0; i < this.settings.minWorkers; i++) {
      this.setupWorker()
    }
  }

  private setupWorker(workerId?: number) {
    if (workerId !== undefined) void this.workers[workerId]?.terminate()

    const { dbOptions, redisOptions, idResolverOptions } = this.opts
    const worker = new Worker(WORKER_PATH, {
      workerData: {
        dbOptions,
        redisOptions,
        idResolverOptions,
      },
    })

    if (workerId !== undefined) {
      this.workers[workerId] = worker
    } else {
      workerId = this.workers.push(worker) - 1
    }

    worker.on('message', (msg: WorkerResponse) => {
      if (msg.type === 'error') {
        this.opts.onError?.(msg.error)
      }
    })

    worker.on('error', (err) => {
      this.opts.onError?.(new FirehoseWorkerError(err))
      this.setupWorker(workerId)
    })
  }

  private async checkScaling() {
    if (this.workers.length === 0) return

    const { pending } = await this.redis.xPending(
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
        this.setupWorker()
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
    await this.redis.connect()

    try {
      await this.redis.xGroupCreate(REDIS_STREAM_NAME, REDIS_GROUP_NAME, '$', {
        MKSTREAM: true,
      })
    } catch {
      // will throw if the stream already exists
    }

    const { id: recoverFromId } = await this.redis
      .xRange(REDIS_STREAM_NAME, '-', '+', { COUNT: 1 })
      .then((res) => res[0] ?? {})

    const initialCursor = recoverFromId?.includes('-')
      ? recoverFromId.split('-').shift()
      : null

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
    if (seq) {
      await this.redis.xAdd(REDIS_STREAM_NAME, '*', {
        seq: `${seq}`,
        data: Buffer.from(chunk).toString('base64'),
      })
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
