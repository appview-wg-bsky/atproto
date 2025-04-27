import { availableParallelism } from 'node:os'
import * as path from 'node:path'
import { SHARE_ENV } from 'node:worker_threads'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors'
import { FirehoseSubscriptionOptions } from './util'
import { Firehose } from '@skyware/firehose'
import Piscina, { FixedQueue } from 'piscina'
import type {
  PiscinaLoadBalancer,
  PiscinaWorker,
} from 'piscina/dist/worker_pool'
import PQueue from 'p-queue'
import { createClient } from '@redis/client'

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
const WORKER_PATH = path.join(__dirname, 'worker.js')
const MAX_ATTEMPTS = 5

let messagesReceived = 0,
  messagesProcessed = 0

export class FirehoseSubscription {
  protected firehose: Firehose
  protected balancer: DidQueueBalancer
  protected piscina: Piscina
  protected redis: ReturnType<typeof createClient>

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
    this.firehose.on('error', (err) => {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
    })

    this.balancer = new DidQueueBalancer(this.settings.maxConcurrency)

    const { dbOptions, idResolverOptions } = this.opts

    this.piscina = new Piscina({
      filename: WORKER_PATH,
      env: SHARE_ENV,
      minThreads: this.settings.minWorkers,
      maxThreads: this.settings.maxWorkers,
      concurrentTasksPerWorker: this.settings.maxConcurrency,
      idleTimeout: 30_000,
      taskQueue: new FixedQueue(),
      loadBalancer: this.balancer.balancer.bind(this.balancer),
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

    this.firehose.on('commit', (c) => this.handleMessage(c.repo, c.seq, c))
    this.firehose.on('account', (a) => this.handleMessage(a.did, a.seq, a))
    this.firehose.on('identity', (i) => this.handleMessage(i.did, i.seq, i))
    this.firehose.on('sync', (s) => this.handleMessage(s.did, s.seq, s))
    this.firehose.on('info', (info) => {
      console.info(`[FIREHOSE] ${info}`)
    })
    this.firehose.on('error', (err) => {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
      this.firehose?.close()
      return this.start()
    })

    setInterval(() => {
      console.log(
        `${Math.round(messagesProcessed / 10)} / ${Math.round(messagesReceived / 10)} (${Math.round((messagesProcessed / messagesReceived) * 100)}%)`,
      )
      messagesReceived = messagesProcessed = 0
    }, 10_000)
  }

  async handleMessage(
    did: string,
    seq: number,
    message: object,
    attempt = 0,
  ): Promise<void> {
    messagesReceived++

    if (attempt > MAX_ATTEMPTS) {
      console.error(`max attempts reached for ${did} ${seq}`, message)
      return
    }

    const res = await this.piscina.run(
      this.makeTask(did, seq, message, attempt),
    )
    if (res.success) {
      await this.onProcessed(did, seq)
      messagesProcessed++
    } else {
      if (res.error) {
        console.warn(new FirehoseWorkerError(res.error))
      }
      return this.handleMessage(did, seq, message, attempt + 1)
    }
  }

  async onProcessed(did: string, seq: number) {
    const worker = this.balancer.queues.get(did)
    if (!worker) return

    const resolve = worker.tasks.get(seq)
    if (!resolve) return

    resolve()
    worker.tasks.delete(seq)
    if (worker.queue.size === 0 || worker.tasks.size === 0) {
      this.balancer.queues.delete(did)
    }
  }

  protected makeTask(did: string, seq: number, message: object, attempt = 0) {
    return {
      ...message,
      [Piscina.queueOptionsSymbol]: {
        did,
        seq,
        attempt,
      },
    }
  }

  async destroy() {
    this.firehose?.close()
    await this.piscina.close({ force: true })
    await this.redis.quit()
  }
}

class DidQueueBalancer {
  queues: Map<
    string,
    { queue: PQueue; worker: PiscinaWorker; tasks: Map<number, () => void> }
  >
  baseBalancer: PiscinaLoadBalancer

  constructor(maxConcurrency: number) {
    this.queues = new Map()
    this.baseBalancer = LeastBusyBalancer({ maximumUsage: maxConcurrency })
  }

  balancer: PiscinaLoadBalancer = (task, workers) => {
    const { did, seq, attempt } = task[Piscina.queueOptionsSymbol]
    if (!did || !seq) {
      console.error(`task missing did or seq`, task)
      return this.baseBalancer(task, workers)
    }

    let didData = this.queues.get(did)
    if (!didData) {
      const worker = this.baseBalancer(task, workers)
      if (!worker) {
        console.warn(`failed to acquire worker for event ${seq} for ${did}`)
        return null
      }

      didData = {
        worker,
        queue: new PQueue({ concurrency: 1, timeout: 120_000 }),
        tasks: new Map(),
      }
    }

    // The resolve function in the tasks map needs to be called by the subscription
    // when the event is processed, to allow the next event from this did to be processed
    const promise = new Promise<void>((resolve) => {
      didData.tasks.set(seq, resolve)
    })
    void didData.queue.add(() => promise)
    return didData.worker
  }
}

// https://github.com/piscinajs/piscina/blob/c1221608783174ea73e7dee482ee20fea6053bfe/src/worker_pool/balancer/index.ts
function LeastBusyBalancer(opts: {
  maximumUsage: number
}): PiscinaLoadBalancer {
  const { maximumUsage } = opts

  return (task, workers) => {
    let candidate: PiscinaWorker | null = null
    let checkpoint = maximumUsage
    for (const worker of workers) {
      if (worker.currentUsage === 0) {
        candidate = worker
        break
      }

      if (worker.isRunningAbortableTask) continue

      if (!task.isAbortable && worker.currentUsage < checkpoint) {
        candidate = worker
        checkpoint = worker.currentUsage
      }
    }

    return candidate
  }
}
