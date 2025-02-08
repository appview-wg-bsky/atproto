import cluster, { Worker } from 'node:cluster'
import { createDeferrable, Deferrable, wait } from '@atproto/common'
import { WebSocketKeepAlive } from '@atproto/xrpc-server/dist/stream/websocket-keepalive'
import {
  isAccount,
  isCommit,
  isIdentity,
  isValidRepoEvent,
  RepoEvent,
} from './lexicons'
import {
  Event,
  EventRunner,
  FirehoseHandlerError,
  FirehoseParseError,
  FirehoseSubscriptionError,
  FirehoseValidationError,
  MemoryRunner,
  parseAccount,
  parseCommitAuthenticated,
  parseCommitUnauthenticated,
  parseIdentity,
} from '@atproto/sync'
import {
  IdentityResolverOpts,
  IdResolver,
  MemoryCache,
} from '@atproto/identity'
import { WriteOpAction } from '@atproto/repo'
import { BackgroundQueue, Database } from '@atproto/bsky'
import { IndexingService } from '@atproto/bsky/dist/data-plane/server/indexing'
import { ensureChunkIsMessage } from '@atproto/xrpc-server'
import type { FirehoseOptions } from '@atproto/sync'

const MIN_WORKERS = 5
const MAX_WORKERS = 20
const SCALE_CHECK_INTERVAL = 10_000
const MAX_ACCEPTABLE_SKEW = 3_000
const MAX_EVENTS_PER_WORKER = 200

type WorkerMessage =
  | { type: 'ready'; workerId: number }
  | {
      type: 'processed'
      workerId: number
      eventId: string
      timestamp: number
    }
  | {
      type: 'processed'
      workerId: number
      eventId: string
      error: Error
    }

interface TrackedEvent {
  id: string
  resolve: () => void
  reject: (err: Error) => void
}

interface WorkerState {
  worker: Worker
  activeEvents: number
  lastEventTime: number
  terminated: boolean
}

export interface IndexerOptions
  extends Partial<Omit<FirehoseOptions, 'idResolver'>> {
  identityResolverOptions?: IdentityResolverOpts
  databaseOptions: ConstructorParameters<typeof Database>[0]
  onError?: (err: Error) => void
  sub?: AsyncIterable<Uint8Array>
  minWorkers?: number
  maxWorkers?: number
}

export class AppViewIndexer {
  protected sub!: AsyncIterable<Uint8Array>
  protected indexingSvc?: IndexingService
  protected idResolver?: IdResolver
  protected runner!: EventRunner
  protected abortController: AbortController
  protected destroyDefer: Deferrable
  protected workers: Map<number, WorkerState> = new Map()
  protected lastEventTimestamp = Date.now()
  protected skewDangerousSince: number | undefined
  protected scalingInterval: NodeJS.Timeout | null = null
  protected trackedEvents: Map<string, TrackedEvent> = new Map()
  protected minWorkers: number
  protected maxWorkers: number

  constructor(public opts: IndexerOptions) {
    this.destroyDefer = createDeferrable()
    this.abortController = new AbortController()

    if (this.opts.getCursor && this.opts.runner) {
      throw new Error('Must set only `getCursor` or `runner`')
    }

    if (this.opts.runner) this.runner = this.opts.runner
    this.opts.onError ??= (err) =>
      // @ts-expect-error
      console.error(err.message, err.cause?.message ?? 'error in subscription')

    this.minWorkers = this.opts.minWorkers ?? MIN_WORKERS
    this.maxWorkers = this.opts.maxWorkers ?? MAX_WORKERS

    if (cluster.isPrimary) {
      this.initializePrimary()
    } else {
      this.initializeWorker()
    }
  }

  protected initializePrimary() {
    for (let i = 0; i < this.minWorkers; i++) {
      this.spawnWorker()
    }

    this.runner ??= new MemoryRunner()

    cluster.on('message', (worker, message: WorkerMessage) => {
      const workerState = this.workers.get(worker.id)
      if (!workerState) return

      if (message.type === 'ready') {
        console.log(`worker ${worker.id} ready`)
      } else if (message.type === 'processed' && message.eventId) {
        if ('timestamp' in message) {
          workerState.lastEventTime = message.timestamp
          this.updateLastEventTimestamp(message.timestamp)
        }

        workerState.activeEvents--

        const tracked = this.trackedEvents.get(message.eventId)
        if (tracked) {
          if ('error' in message) {
            tracked.reject(message.error)
          } else {
            tracked.resolve()
          }
          this.trackedEvents.delete(message.eventId)
        }
      }
    })

    cluster.on('exit', (worker) => {
      this.workers.delete(worker.id)

      if (!this.abortController?.signal.aborted) {
        this.spawnWorker()
      }
    })

    if (this.opts.sub) {
      console.log('using provided subscription')
      this.sub = this.opts.sub
    } else {
      console.log('initializing subscription')
      this.sub = new WebSocketKeepAlive({
        signal: this.abortController.signal,
        heartbeatIntervalMs: 10_000,
        getUrl: async () => {
          const getCursorFn = () =>
            this.runner?.getCursor() ?? this.opts.getCursor?.()
          let url = `${this.opts.service}/xrpc/com.atproto.sync.subscribeRepos`
          const cursor = await getCursorFn()
          if (cursor !== undefined) {
            url += `?cursor=${cursor}`
          }
          return url
        },
      })
    }

    this.scalingInterval = setInterval(
      () => this.checkScaling(),
      SCALE_CHECK_INTERVAL,
    )
  }

  protected updateLastEventTimestamp(timestamp: number) {
    if (
      this.lastEventTimestamp === undefined ||
      timestamp > this.lastEventTimestamp
    ) {
      this.lastEventTimestamp = timestamp
    }
  }

  protected getNextAvailableWorker(): WorkerState | undefined {
    let leastBusyWorker: WorkerState | undefined
    let minActiveEvents = MAX_EVENTS_PER_WORKER

    for (const state of this.workers.values()) {
      if (state.terminated) continue
      if (state.activeEvents < minActiveEvents) {
        minActiveEvents = state.activeEvents
        leastBusyWorker = state
      }
    }

    return (leastBusyWorker?.activeEvents ?? Infinity) < MAX_EVENTS_PER_WORKER
      ? leastBusyWorker
      : undefined
  }

  protected initializeWorker() {
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

    process.on('message', async (msg) => {
      if (
        !msg ||
        typeof msg !== 'object' ||
        !('chunk' in msg) ||
        !Array.isArray(msg.chunk) ||
        msg.chunk.length === 0 ||
        !('eventId' in msg) ||
        typeof msg.eventId !== 'string'
      ) {
        this.opts.onError?.(new Error('received invalid message'))
        return
      }

      const evt = await this.parseEvtBytes(Uint8Array.from(msg.chunk))
      if (!evt) return

      void this.workerHandleEvent(evt, msg.eventId)
    })

    process.send?.({
      type: 'ready',
      workerId: cluster.worker!.id,
    })
  }

  protected spawnWorker() {
    if (this.workers.size >= MAX_WORKERS) return

    const worker = cluster.fork()
    this.workers.set(worker.id, {
      worker,
      activeEvents: 0,
      lastEventTime: Date.now(),
      terminated: false,
    })
  }

  protected terminateWorker() {
    if (this.workers.size <= this.minWorkers) return

    let workerToTerminate: [number, WorkerState] | undefined
    let minActiveEvents = Infinity

    for (const [id, state] of this.workers.entries()) {
      if (state.activeEvents < minActiveEvents) {
        minActiveEvents = state.activeEvents
        workerToTerminate = [id, state]
      }
    }

    if (workerToTerminate && workerToTerminate[1].activeEvents === 0) {
      const [id, state] = workerToTerminate
      state.worker.kill()
      this.workers.delete(id)
    } else if (workerToTerminate) {
      const [id, state] = workerToTerminate
      state.terminated = true
      setInterval(() => {
        if (state.activeEvents === 0) {
          state.worker.kill()
          this.workers.delete(id)
        }
      }, 1000)
    } else {
      console.warn('no workers to terminate')
    }
  }

  protected checkScaling() {
    const currentSkew = Date.now() - this.lastEventTimestamp
    const currentSkewStr = (currentSkew / 1000).toFixed(1) + 's'
    const workerCount = this.workers.size
    const totalActiveEvents = Array.from(this.workers.values()).reduce(
      (sum, state) => sum + state.activeEvents,
      0,
    )
    const avgEventsPerWorker = totalActiveEvents / workerCount
    const avgEventsStr = avgEventsPerWorker.toFixed(1)

    // If skew is too high,
    if (currentSkew > MAX_ACCEPTABLE_SKEW) {
      // Take note of the last time it became dangerous, then wait and see if it stays dangerous
      if (this.skewDangerousSince === undefined) {
        this.skewDangerousSince = Date.now()
        return
      } else if (
        // If it's been dangerous 3x the scaling check interval, try to scale up workers
        Date.now() - this.skewDangerousSince >
        3 * SCALE_CHECK_INTERVAL
      ) {
        this.skewDangerousSince = undefined
        if (workerCount < this.maxWorkers) {
          console.log(
            `scaling up to ${workerCount + 1} workers with skew ${currentSkewStr} and ${avgEventsStr} events/worker`,
          )
          this.spawnWorker()
        } else {
          console.warn(
            `skew is ${currentSkewStr} and ${avgEventsStr} events/worker, but max workers reached`,
          )
        }
      }
    } else {
      // If skew isn't dangerous, reset the dangerous skew timer
      this.skewDangerousSince = undefined
      // If skew has gone down significantly and workers aren't overloaded, scale down
      if (
        currentSkew < MAX_ACCEPTABLE_SKEW / 2 &&
        avgEventsPerWorker < MAX_EVENTS_PER_WORKER * 0.9 &&
        workerCount > this.minWorkers
      ) {
        console.log(
          `scaling down to ${workerCount - 1} workers with skew ${currentSkewStr} and ${avgEventsStr} events/worker`,
        )
        this.terminateWorker()
      }
    }
  }

  protected async sendToWorker(chunk: Uint8Array): Promise<void> {
    let workerState: WorkerState | undefined

    while (!(workerState = this.getNextAvailableWorker())) {
      await new Promise((resolve) => setTimeout(resolve, 10))
    }

    const eventId = Math.random().toString(36).slice(2)
    return new Promise((resolve, reject) => {
      this.trackedEvents.set(eventId, {
        id: eventId,
        resolve,
        reject,
      })
      workerState!.activeEvents++
      const evt = {
        chunk: [...chunk],
        eventId,
      }
      workerState!.worker.send(evt)
    })
  }

  async start(): Promise<void> {
    if (!cluster.isPrimary) return

    try {
      for await (const chunk of this.sub) {
        const evt = await this.parseEvtBytes(chunk)
        if (!evt) continue

        const parsed = didAndSeqForEvt(evt)
        if (parsed) {
          void this.runner.trackEvent(parsed.did, parsed.seq, () =>
            this.sendToWorker(chunk),
          )
        }
      }
    } catch (err) {
      if (err && (err as any).name === 'AbortError') {
        this.destroyDefer.resolve()
        return
      }
      this.opts.onError?.(new FirehoseSubscriptionError(err))
      await wait(this.opts.subscriptionReconnectDelay ?? 3000)
      return this.start()
    }
  }

  protected async workerHandleEvent(evt: RepoEvent, eventId: string) {
    let parsed: Event[]

    try {
      parsed = await this.parseEvt(evt)
    } catch (err) {
      process.send?.({
        type: 'processed',
        workerId: cluster.worker!.id,
        eventId,
        error: new FirehoseParseError(err, evt),
      })
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
        this.opts.onError?.(new FirehoseHandlerError(err, write))
      }
    }

    process.send?.({
      type: 'processed',
      workerId: cluster.worker!.id,
      eventId,
      timestamp: Date.now(),
    })
  }

  protected async parseEvtBytes(
    chunk: Uint8Array,
  ): Promise<RepoEvent | undefined> {
    const message = ensureChunkIsMessage(chunk)
    const t = message.header.t
    const clone = message.body !== undefined ? { ...message.body } : undefined
    if (clone !== undefined && t !== undefined) {
      // @ts-expect-error
      clone['$type'] = t.startsWith('#')
        ? 'com.atproto.sync.subscribeRepos' + t
        : t
    }

    try {
      return isValidRepoEvent(clone)
    } catch (err) {
      this.opts.onError?.(new FirehoseValidationError(err, clone))
    }
  }

  protected async parseEvt(evt: RepoEvent): Promise<Event[]> {
    try {
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
    } catch (err) {
      this.opts.onError?.(new FirehoseParseError(err, evt))
      return []
    }
  }

  async destroy(): Promise<void> {
    this.abortController?.abort()

    if (this.scalingInterval) {
      clearInterval(this.scalingInterval)
    }

    if (cluster.isPrimary) {
      for (const { worker } of this.workers.values()) {
        worker.kill()
      }
    } else {
      this.destroyDefer?.resolve()
    }

    await this.destroyDefer?.complete
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
