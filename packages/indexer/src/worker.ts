import { parentPort, workerData } from 'node:worker_threads'
import { Redis } from 'ioredis'
import PQueue from 'p-queue'
import { BackgroundQueue, Database } from '@atproto/bsky'
import { IndexingService } from '@atproto/bsky/dist/data-plane/server/indexing'
import { parseIntWithFallback } from '@atproto/common'
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
import { REDIS_GROUP_NAME, REDIS_STREAM_NAME } from './subscription'
import { FirehoseSubscriptionOptions, WorkerResponse } from './types'

interface Message {
  id: string | null
  seq: number | null
  data: Buffer | null
}

if (!workerData) {
  throw new Error('Must be run as a worker')
}

let redis: Redis
let indexingSvc: IndexingService
let background: BackgroundQueue
let idResolver: IdResolver

const queue = new PQueue({ concurrency: 50 })

void main()

async function main() {
  init()

  let cursor: string | null = null
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const { cursor: nextCursor, ...message } = await readNextMessage(cursor)
    await queueMessage(message)
    cursor = nextCursor
  }
}

function init() {
  const { dbOptions, redisOptions, idResolverOptions } =
    workerData as FirehoseSubscriptionOptions
  if (!dbOptions || !redisOptions || !idResolverOptions) {
    throw new Error('worker missing options')
  }

  const db = new Database(dbOptions)
  // :/
  if (typeof redisOptions === 'string') {
    redis = new Redis(redisOptions)
  } else {
    redis = new Redis(redisOptions)
  }
  idResolver = new IdResolver({
    ...idResolverOptions,
    didCache: new MemoryCache(),
  })
  background = new BackgroundQueue(db)
  indexingSvc = new IndexingService(db, idResolver, background)
}

async function readNextMessage(cursor: string | null = null): Promise<
  Message & {
    cursor: string | null
  }
> {
  const ret: Message & { cursor: string | null } = {
    id: null,
    seq: null,
    data: null,
    cursor: null,
  }

  try {
    // First try to claim unclaimed messages
    // [cursor, [[id, ['seq', seq, 'data', data]]]]
    let res = await redis
      .xautoclaim(
        REDIS_STREAM_NAME,
        REDIS_GROUP_NAME,
        `${process.pid}`,
        60_000,
        cursor || '0-0',
        'COUNT',
        1,
      )
      .then((res: any[]) => {
        ret.cursor = res?.[0] ?? null
        return res?.[1]?.[0]
      })

    if (!res?.length || !res[1]) {
      // If there's nothing, read from the stream
      // [[REDIS_STREAM_NAME, [[id, ['seq', seq, 'data', data]]]]]
      res = await redis
        .xreadgroup(
          'GROUP',
          REDIS_GROUP_NAME,
          `${process.pid}`,
          'COUNT',
          1,
          'BLOCK',
          5000,
          'STREAMS',
          REDIS_STREAM_NAME,
          '>',
        )
        .then((res: any[]) => res?.[0]?.[1]?.[0])

      if (!res?.length || !res[1]) {
        throw 0
      }
    }

    const [id, msg] = res
    ret.id = id
    ret.seq = parseIntWithFallback(msg[1], null)
    ret.data = msg[3]?.length ? Buffer.from(msg[3], 'base64') : null
  } catch (err) {
    console.warn('error reading message', err)
    // will return ret as is */
  }

  ret.cursor ||= '0-0'

  return ret
}

async function queueMessage({ id, seq, data }: Message) {
  if (!id || !seq || !data) {
    console.warn('invalid message', id, seq, data)
    return
  }

  await waitUntilQueueLessThan(1000)

  void queue.add(
    () =>
      handleMessage(data)
        .then(() => redis.xack(REDIS_STREAM_NAME, REDIS_GROUP_NAME, id))
        .catch((err) =>
          parentPort?.postMessage({
            type: 'error',
            error: new FirehoseWorkerError(err),
          } satisfies WorkerResponse),
        ),
    {
      // earlier messages are more important
      priority: Number.MAX_SAFE_INTEGER - seq,
    },
  )
}

async function handleMessage(msg: Buffer) {
  if (!indexingSvc) {
    throw new Error('Worker not initialized')
  }

  const message = ensureChunkIsMessage(msg)
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
  for (const evt of parsed) {
    await processEvent(evt)
  }
}

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
    if (err instanceof Error) {
      switch (err.name) {
        case 'AbortError':
          return []
        case 'TypeError':
          if (
            err.cause instanceof Error &&
            err.cause.name === 'ConnectTimeoutError'
          ) {
            return []
          }
      }
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

async function waitUntilQueueLessThan(size: number) {
  return new Promise<void>((resolve) => {
    const listener = () => {
      if (queue.size < size) {
        queue.off('next', listener)
        resolve()
      }
    }
    queue.on('next', listener)
  })
}
