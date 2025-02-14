import { parentPort } from 'node:worker_threads'
import { BackgroundQueue, Database } from '@atproto/bsky'
import { IndexingService } from '@atproto/bsky/dist/data-plane/server/indexing'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { WriteOpAction } from '@atproto/repo'
import {
  Event,
  FirehoseParseError,
  parseAccount,
  parseCommitUnauthenticated,
  parseIdentity,
} from '@atproto/sync'
import { ensureChunkIsMessage } from '@atproto/xrpc-server'
import { FirehoseWorkerError } from './errors'
import {
  RepoEvent,
  isAccount,
  isCommit,
  isIdentity,
  isValidRepoEvent,
} from './lexicons'
import { WorkerMessage, WorkerResponse, WorkerStats } from './types'

if (!parentPort) {
  throw new Error('Must be run as a worker')
}

const stats: WorkerStats = {
  ready: false,
  processedCount: 0,
  avgProcessingTimeMs: 0,
  currentLatencyMs: 0,
}

let processingTimes: number[] = [] // Rolling window of processing times
const MAX_TIMES_WINDOW = 100

let indexingSvc: IndexingService
let background: BackgroundQueue
let idResolver: IdResolver

parentPort.on('message', async (msg: WorkerMessage) => {
  try {
    if (msg.type === 'init') {
      const db = new Database(msg.dbOptions)
      idResolver = new IdResolver({
        ...msg.idResolverOptions,
        didCache: new MemoryCache(),
      })
      background = new BackgroundQueue(db)
      indexingSvc = new IndexingService(db, idResolver, background)
      stats.ready = true
      parentPort?.postMessage({ type: 'stats', stats } satisfies WorkerResponse)
    } else if (msg.type === 'chunk') {
      if (!indexingSvc) {
        throw new Error('Worker not initialized')
      }

      const start = Date.now()

      const message = ensureChunkIsMessage(msg.data)
      const t = message.header.t
      const clone: Record<string, unknown> | undefined =
        message.body !== undefined ? { ...message.body } : undefined
      if (clone !== undefined && t !== undefined) {
        clone['$type'] = t.startsWith('#')
          ? 'com.atproto.sync.subscribeRepos' + t
          : t
      }

      let event: RepoEvent
      try {
        event = isValidRepoEvent(clone)
        if (event === undefined) throw new Error('empty event')
      } catch (err) {
        parentPort?.postMessage({
          type: 'error',
          error: new FirehoseWorkerError(err),
        } satisfies WorkerResponse)
        return
      }

      const parsed = await parseEvt(event)
      await Promise.all(
        parsed.map((ev) => processEvent(ev).then(() => updateStats(ev, start))),
      )
    }
  } catch (err) {
    parentPort?.postMessage({
      type: 'error',
      error: new FirehoseWorkerError(err),
    } satisfies WorkerResponse)
  }
})

async function parseEvt(evt: RepoEvent): Promise<Event[]> {
  try {
    if (isCommit(evt)) {
      return parseCommitUnauthenticated(evt)
    } else if (isAccount(evt)) {
      const parsed = parseAccount(evt)
      return parsed ? [parsed] : []
    } else if (isIdentity(evt)) {
      const parsed = await parseIdentity(idResolver, evt, true)
      return parsed ? [parsed] : []
    } else {
      return []
    }
  } catch (err) {
    if (err instanceof Error && err.name === 'AbortError') {
      return []
    }
    parentPort?.postMessage({
      type: 'error',
      error: new FirehoseParseError(err, evt),
    })
    return []
  }
}

async function processEvent(evt: Event) {
  if (evt.event === 'identity') {
    return indexingSvc.indexHandle(evt.did, evt.time, true)
  } else if (evt.event === 'account') {
    if (evt.active === false && evt.status === 'deleted') {
      return indexingSvc.deleteActor(evt.did)
    } else {
      return indexingSvc.updateActorStatus(evt.did, evt.active, evt.status)
    }
  } else {
    const indexFn =
      evt.event === 'delete'
        ? indexingSvc.deleteRecord(evt.uri)
        : indexingSvc.indexRecord(
            evt.uri,
            evt.cid,
            evt.record,
            evt.event === 'create'
              ? WriteOpAction.Create
              : WriteOpAction.Update,
            evt.time,
          )
    background.add(() => indexingSvc.indexHandle(evt.did, evt.time))
    await Promise.all([
      indexFn,
      indexingSvc.setCommitLastSeen(evt.did, evt.commit, evt.rev),
    ])
  }
}

function updateStats(evt: Event, start: number) {
  const processingTime = Date.now() - start
  processingTimes.push(processingTime)
  if (processingTimes.length > MAX_TIMES_WINDOW) {
    processingTimes = processingTimes.slice(-MAX_TIMES_WINDOW)
  }

  stats.processedCount++
  stats.avgProcessingTimeMs =
    processingTimes.reduce((a, b) => a + b, 0) / processingTimes.length
  stats.lastEventTime = evt.time
  stats.currentLatencyMs = Date.now() - new Date(evt.time).getTime()

  parentPort?.postMessage({
    type: 'stats',
    stats,
  } satisfies WorkerResponse)
}
