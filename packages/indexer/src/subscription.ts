import { cpus } from 'node:os'
import * as path from 'node:path'
import { SHARE_ENV, Worker } from 'node:worker_threads'
import { createClient } from '@redis/client'
import { cborDecodeMulti } from '@atproto/common'
import { WebSocketKeepAlive } from '@atproto/xrpc-server/dist/stream/websocket-keepalive'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors'
import {
  FirehoseSubscriptionOptions,
  WorkerData,
  WorkerResponse,
  logVerbose,
} from './util'

const WORKER_PATH = path.join(__dirname, 'worker.js')
export const REDIS_STREAM_NAME = 'bsky_indexer:firehose'
export const REDIS_GROUP_NAME = 'bsky_indexer_consumers'

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

  private setupWorker(replacingWorkerId?: number) {
    if (replacingWorkerId !== undefined)
      void this.workers.get(replacingWorkerId)?.worker?.terminate()

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
        logVerbose(`received from worker [${worker.threadId}]: ${msg.id}`, 0.01)

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
      } else if (msg.type === 'error') {
        this.opts.onError?.(msg.error)
      }
    })

    worker.on('error', (err) => {
      this.opts.onError?.(new FirehoseWorkerError(err))
      this.setupWorker(replacingWorkerId)
    })
  }

  private async checkScaling() {
    if (this.workers.size === 0) return

    const streamLength = await this.redis.xLen(REDIS_STREAM_NAME)
    if (typeof streamLength !== 'number' || isNaN(streamLength)) return

    logVerbose(
      `pending: ${streamLength} | previously: ${this.totalPending} | scaling: ${this.needsToScale}`,
    )

    if (streamLength > this.totalPending) {
      this.needsToScale++
    } else this.needsToScale = Math.max(0, this.needsToScale - 1)
    this.totalPending = streamLength

    // Scale up if the pending count has increased for the last 3 cycles
    if (this.needsToScale >= 3) {
      if (this.workers.size >= this.settings.maxWorkers) {
        console.warn(`pending count ${streamLength} but max workers reached`)
        return
      }

      const newWorkersCount = Math.min(
        this.settings.maxWorkers,
        this.workers.size + Math.ceil(this.workers.size * 0.5), // Add 50% more workers
      )
      console.log(
        `spawning ${newWorkersCount - this.workers.size} workers for a total of ${newWorkersCount}`,
      )
      while (this.workers.size < newWorkersCount) {
        this.setupWorker()
      }
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

      await toTerminate.terminate()
      console.log(`killed worker ${toTerminate.threadId}`)

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

    const { id: recoverFromId } = await this.redis
      .xRange(REDIS_STREAM_NAME, '-', '+', { COUNT: 1 })
      .then((res) => res[0] ?? {})

    const initialCursor = recoverFromId?.includes('-')
      ? recoverFromId.split('-').shift()
      : null

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

      setInterval(() => this.logWorkerStats(), 60_000)

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

  async onProcessed(id: string) {
    await this.redis.xDel(REDIS_STREAM_NAME, id)
  }

  logWorkerStats() {
    const workers = [...this.workers.values()].sort(
      (a, b) => b.worker.threadId - a.worker.threadId,
    )
    for (const { worker, data } of workers) {
      console.log(
        `${worker.threadId}: proc ${(data.processedPerMinute ?? 0) / 60}/sec, avg: ${data.averageProcessingTime}ms`,
      )

      data.processedPerMinute = null
      data.averageProcessingTime = null
    }
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
