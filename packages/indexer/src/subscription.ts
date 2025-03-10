import { cpus } from 'node:os'
import * as path from 'node:path'
import { Worker } from 'node:worker_threads'
import { WebSocketKeepAlive } from '@atproto/xrpc-server/dist/stream/websocket-keepalive'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors'
import {
  FirehoseSubscriptionOptions,
  WorkerMessage,
  WorkerResponse,
  WorkerStats,
} from './types'

const WORKER_PATH = path.join(__dirname, 'worker.js')

export class FirehoseSubscription {
  private workers: Worker[] = []
  private workerStats: Map<number, WorkerStats> = new Map()
  private ws: WebSocketKeepAlive | null = null
  private destroyed = false
  private currentWorker = 0
  private scaleCheckInterval: NodeJS.Timeout | null = null
  private needsToScale = 0

  private totalProcessed = 0

  private settings = {
    targetLatencyMs: 5_000,
    scaleCheckIntervalMs: 10_000,
    minWorkers: 2,
    maxWorkers: cpus().length,
  }

  constructor(private opts: FirehoseSubscriptionOptions) {
    if (this.opts.minWorkers) this.settings.minWorkers = this.opts.minWorkers
    if (this.opts.maxWorkers) this.settings.maxWorkers = this.opts.maxWorkers
    if (this.opts.targetLatencyMs)
      this.settings.targetLatencyMs = this.opts.targetLatencyMs
    if (this.opts.scaleCheckIntervalMs)
      this.settings.scaleCheckIntervalMs = this.opts.scaleCheckIntervalMs

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
      idResolverOptions: this.opts.idResolverOptions ?? {},
    } satisfies WorkerMessage)

    worker.on('message', (msg: WorkerResponse) => {
      const stats = this.workerStats.get(workerId)
      if (msg.type === 'error') {
        this.opts.onError?.(msg.error)
      } else if (msg.type === 'stats') {
        this.workerStats.set(workerId, { ...stats, ...msg.stats, ready: true })
      }
    })

    worker.on('error', (err) => {
      this.opts.onError?.(new FirehoseWorkerError(err))
      this.replaceWorker(workerId)
    })
  }

  private replaceWorker(workerId: number) {
    if (!this.destroyed) {
      const newWorker = new Worker(WORKER_PATH)
      this.setupWorker(newWorker, workerId)
      this.workers[workerId] = newWorker
    }
  }

  private async getNextWorker(): Promise<Worker> {
    const worker = this.workers[this.currentWorker]
    this.currentWorker = (this.currentWorker + 1) % this.workers.length
    if (!worker) {
      await new Promise((resolve) => setTimeout(resolve, 100))
      return this.getNextWorker()
    }
    return worker
  }

  private checkScaling() {
    if (this.workers.length === 0) return

    const workerStats = [...this.workerStats.values()]

    const avgLatency =
      workerStats.reduce((a, b) => a + (b.currentLatencyMs ?? 0), 0) /
      workerStats.length

    if (Number.isNaN(avgLatency)) {
      console.warn('avgLatency is NaN')
      return
    }

    const newTotalProcessed = workerStats.reduce(
      (a, b) => a + b.processedCount,
      0,
    )

    if (avgLatency > this.settings.targetLatencyMs * 0.8) {
      let log = `avg latency: ${(avgLatency / 1000).toFixed(3)}s`
      if (this.totalProcessed) {
        const processedRate =
          (newTotalProcessed - this.totalProcessed) /
          (this.settings.scaleCheckIntervalMs / 1000)
        log += `\t\tprocessed: ${processedRate.toFixed(0)}/s`
      }
      console.log(log)
    }

    this.totalProcessed = newTotalProcessed

    // Scale up if we're falling behind
    if (
      avgLatency > this.settings.targetLatencyMs &&
      this.workers.length < this.settings.maxWorkers
    ) {
      // Only scale up if we've been waiting for 3 cycles
      this.needsToScale++
      if (this.needsToScale < 3) return
      this.needsToScale = 0

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
    if (
      avgLatency < this.settings.targetLatencyMs / 2 &&
      this.workers.length > this.settings.minWorkers
    ) {
      const targetWorkers = Math.max(
        this.settings.minWorkers,
        Math.ceil(this.workers.length * 0.75), // Remove 25% of workers
      )

      console.log(
        `killing ${this.workers.length - targetWorkers} workers for a total of ${targetWorkers}`,
      )

      const toTerminate = this.workers.splice(targetWorkers)
      Promise.all(toTerminate.map((worker) => worker.terminate())).then(() => {
        console.log(`killed ${toTerminate.length} workers`)
      })
    }
  }

  async start() {
    try {
      this.ws = new WebSocketKeepAlive({
        getUrl: async () => {
          const params = this.opts.cursor
            ? { cursor: `${this.opts.cursor}` }
            : undefined
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

        const worker = await this.getNextWorker()
        worker.postMessage({
          type: 'chunk',
          data: chunk,
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

  async destroy() {
    this.destroyed = true
    if (this.scaleCheckInterval) {
      clearInterval(this.scaleCheckInterval)
    }
    this.ws?.ws?.close()

    await Promise.all(this.workers.map((worker) => worker.terminate()))
  }
}
