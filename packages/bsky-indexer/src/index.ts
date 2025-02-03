import cluster, { Worker } from 'node:cluster'
import { createDeferrable, Deferrable, wait } from '@atproto/common'
import { Subscription } from '@atproto/xrpc-server'
import {
  RepoEvent,
  isValidRepoEvent,
  isAccount,
  isCommit,
  isIdentity,
} from './lexicons'
import {
  Event,
  FirehoseOptions,
  FirehoseSubscriptionError,
  FirehoseHandlerError,
  parseCommitUnauthenticated,
  parseCommitAuthenticated,
  parseAccount,
  parseIdentity,
  FirehoseParseError,
  FirehoseValidationError,
} from '@atproto/sync'
import {
  IdentityResolverOpts,
  IdResolver,
  MemoryCache,
} from '@atproto/identity'
import { WriteOpAction } from '@atproto/repo'
import { BackgroundQueue, Database } from '@atproto/bsky'
import { IndexingService } from '@atproto/bsky/dist/data-plane/server/indexing'

const MIN_WORKERS = 5
const MAX_WORKERS = 20
const SCALE_CHECK_INTERVAL = 5000
const MAX_ACCEPTABLE_SKEW = 3000

interface WorkerMessage {
  type: 'ready' | 'processed' | 'tracked-complete'
  workerId: number
  timestamp?: number
  error?: Error
  eventId?: string
}

interface TrackedEvent {
  id: string
  evt: RepoEvent
  resolve: () => void
  reject: (err: Error) => void
}

export interface IndexerOptions extends Omit<FirehoseOptions, 'idResolver'> {
  identityResolverOptions?: IdentityResolverOpts
  databaseOptions: ConstructorParameters<typeof Database>[0]
}

export class AppViewIndexer {
  private sub!: Subscription<RepoEvent>
  private indexingSvc?: IndexingService
  private idResolver?: IdResolver
  private abortController: AbortController
  private destroyDefer: Deferrable
  private workers: Map<number, Worker> = new Map()
  private workerQueue: number[] = []
  private eventsReceived = 0
  private eventsProcessed = 0
  private lastEventTimestamp = Date.now()
  private scalingInterval: NodeJS.Timeout | null = null
  private trackedEvents: Map<string, TrackedEvent> = new Map()

  constructor(public opts: IndexerOptions) {
    this.destroyDefer = createDeferrable()
    this.abortController = new AbortController()

    if (this.opts.getCursor && this.opts.runner) {
      throw new Error('Must set only `getCursor` or `runner`')
    }

    if (cluster.isPrimary) {
      this.initializePrimary()
    } else {
      this.initializeWorker()
    }
  }

  private initializePrimary() {
    for (let i = 0; i < MIN_WORKERS; i++) {
      this.spawnWorker()
    }

    cluster.on('message', (worker, message: WorkerMessage) => {
      if (message.type === 'ready') {
        this.workerQueue.push(worker.id)
      } else if (message.type === 'processed') {
        this.eventsProcessed++
        this.workerQueue.push(worker.id)
        if (message.timestamp) {
          this.lastEventTimestamp = message.timestamp
        }
        if (message.error) {
          this.opts.onError(message.error)
        }
      } else if (message.type === 'tracked-complete' && message.eventId) {
        const tracked = this.trackedEvents.get(message.eventId)
        if (tracked) {
          if (message.error) {
            tracked.reject(message.error)
          } else {
            tracked.resolve()
          }
          this.trackedEvents.delete(message.eventId)
        }
        this.workerQueue.push(worker.id)
      }
    })

    cluster.on('exit', (worker) => {
      this.workers.delete(worker.id)
      this.workerQueue = this.workerQueue.filter((id) => id !== worker.id)

      if (!this.abortController.signal.aborted) {
        this.spawnWorker()
      }
    })

    this.sub = new Subscription({
      ...this.opts,
      service: this.opts.service ?? 'wss://bsky.network',
      method: 'com.atproto.sync.subscribeRepos',
      signal: this.abortController.signal,
      getParams: async () => {
        const getCursorFn = () =>
          this.opts.runner?.getCursor() ?? this.opts.getCursor?.()
        if (!getCursorFn) {
          return undefined
        }
        const cursor = await getCursorFn()
        return { cursor }
      },
      validate: (value: unknown) => {
        try {
          return isValidRepoEvent(value)
        } catch (err) {
          this.opts.onError(new FirehoseValidationError(err, value))
        }
      },
    })

    this.scalingInterval = setInterval(
      () => this.checkScaling(),
      SCALE_CHECK_INTERVAL,
    )
  }

  private initializeWorker() {
    this.idResolver = new IdResolver({
      didCache: new MemoryCache(),
      ...(this.opts.identityResolverOptions ?? {}),
    })
    const db = new Database(this.opts.databaseOptions)
    this.indexingSvc = new IndexingService(
      db,
      this.idResolver!,
      new BackgroundQueue(db),
    )

    process.on('message', this.workerHandleEvent.bind(this))

    process.send?.({
      type: 'ready',
      workerId: cluster.worker!.id,
    })
  }

  private spawnWorker() {
    if (this.workers.size >= MAX_WORKERS) return

    const worker = cluster.fork()
    this.workers.set(worker.id, worker)
  }

  private terminateWorker() {
    if (this.workers.size <= MIN_WORKERS) return

    const [workerId] = this.workers.keys()
    const worker = this.workers.get(workerId)
    if (worker) {
      worker.kill()
      this.workers.delete(workerId)
      this.workerQueue = this.workerQueue.filter((id) => id !== workerId)
    }
  }

  private async workerHandleEvent(evt: RepoEvent) {
    const eventId = (evt as any).__trackingId
    let parsed: Event[]

    try {
      parsed = await this.parseEvt(evt)
    } catch (err) {
      if (eventId) {
        process.send?.({
          type: 'tracked-complete',
          workerId: cluster.worker!.id,
          eventId,
          error: new FirehoseParseError(err, evt),
        })
      } else {
        process.send?.({
          type: 'processed',
          workerId: cluster.worker!.id,
          timestamp: Date.now(),
          error: new FirehoseParseError(err, evt),
        })
      }
      return
    }

    for (const write of parsed) {
      try {
        if (write.event === 'identity') {
          await this.indexingSvc!.indexHandle(write.did, write.time, true)
        } else if (write.event === 'account') {
          if (write.active === false && write.status === 'deleted') {
            await this.indexingSvc!.deleteActor(write.did)
          } else {
            await this.indexingSvc!.updateActorStatus(
              write.did,
              write.active,
              write.status,
            )
          }
        } else {
          const indexFn =
            write.event === 'delete'
              ? this.indexingSvc!.deleteRecord(write.uri)
              : this.indexingSvc!.indexRecord(
                  write.uri,
                  write.cid,
                  write.record,
                  write.event === 'create'
                    ? WriteOpAction.Create
                    : WriteOpAction.Update,
                  write.time,
                )
          await Promise.all([
            indexFn,
            this.indexingSvc!.setCommitLastSeen(
              write.did,
              write.commit,
              write.rev,
            ),
            this.indexingSvc!.indexHandle(write.did, write.time),
          ])
        }
      } catch (err) {
        this.opts.onError(new FirehoseHandlerError(err, write))
      }
    }

    if (eventId) {
      process.send?.({
        type: 'tracked-complete',
        workerId: cluster.worker!.id,
        eventId,
        timestamp: Date.now(),
      })
    } else {
      process.send?.({
        type: 'processed',
        workerId: cluster.worker!.id,
        timestamp: Date.now(),
      })
    }
  }

  private checkScaling() {
    const currentSkew = Date.now() - this.lastEventTimestamp
    const workerCount = this.workers.size

    if (currentSkew > MAX_ACCEPTABLE_SKEW && workerCount < MAX_WORKERS) {
      this.spawnWorker()
    } else if (
      currentSkew < MAX_ACCEPTABLE_SKEW / 2 &&
      workerCount > MIN_WORKERS
    ) {
      this.terminateWorker()
    }
  }

  private async sendToWorker(evt: RepoEvent, tracked = false): Promise<void> {
    while (this.workerQueue.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 10))
    }

    const workerId = this.workerQueue.shift()!
    const worker = this.workers.get(workerId)

    if (worker) {
      if (tracked) {
        const eventId = Math.random().toString(36).slice(2)
        return new Promise((resolve, reject) => {
          this.trackedEvents.set(eventId, {
            id: eventId,
            evt,
            resolve,
            reject,
          })
          const evtWithId = {
            ...evt,
            __trackingId: eventId,
          }
          worker.send(evtWithId)
        })
      } else {
        worker.send(evt)
      }
    }
  }

  async start(): Promise<void> {
    if (!cluster.isPrimary) return

    try {
      for await (const evt of this.sub) {
        this.eventsReceived++

        if (this.opts.runner) {
          const parsed = didAndSeqForEvt(evt)
          if (parsed) {
            this.opts.runner.trackEvent(parsed.did, parsed.seq, async () => {
              await this.sendToWorker(evt, true)
            })
          }
        } else {
          await this.sendToWorker(evt, false)
        }
      }
    } catch (err) {
      if (err && (err as any).name === 'AbortError') {
        this.destroyDefer.resolve()
        return
      }
      this.opts.onError(new FirehoseSubscriptionError(err))
      await wait(this.opts.subscriptionReconnectDelay ?? 3000)
      return this.start()
    }
  }

  private async parseEvt(evt: RepoEvent): Promise<Event[]> {
    if (isCommit(evt) && !this.opts.excludeCommit) {
      return this.opts.unauthenticatedCommits
        ? await parseCommitUnauthenticated(evt, this.opts.filterCollections)
        : await parseCommitAuthenticated(
            this.idResolver!,
            evt,
            this.opts.filterCollections,
          )
    } else if (isAccount(evt) && !this.opts.excludeAccount) {
      const parsed = parseAccount(evt)
      return parsed ? [parsed] : []
    } else if (isIdentity(evt) && !this.opts.excludeIdentity) {
      const parsed = await parseIdentity(
        this.idResolver!,
        evt,
        this.opts.unauthenticatedHandles,
      )
      return parsed ? [parsed] : []
    } else {
      return []
    }
  }

  async destroy(): Promise<void> {
    this.abortController.abort()

    if (this.scalingInterval) {
      clearInterval(this.scalingInterval)
    }

    for (const worker of this.workers.values()) {
      worker.kill()
    }

    await this.destroyDefer.complete
  }
}

const didAndSeqForEvt = (
  evt: RepoEvent,
): { did: string; seq: number } | undefined => {
  if (isCommit(evt)) return { seq: evt.seq, did: evt.repo }
  else if (isAccount(evt) || isIdentity(evt))
    return { seq: evt.seq, did: evt.did }
  return undefined
}
