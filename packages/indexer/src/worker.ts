import { workerData } from 'node:worker_threads'
import { CID } from 'multiformats/cid'
import { BackgroundQueue, Database } from '@atproto/bsky'
import { IndexingService } from '@atproto/bsky/dist/data-plane/server/indexing'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { WriteOpAction } from '@atproto/repo'
import {
  Event,
  parseAccount,
  parseCommitUnauthenticated,
  parseIdentity,
  parseSync,
} from '@atproto/sync'
import {
  RepoEvent,
  isAccount,
  isCommit,
  isIdentity,
  isSync,
  isValidRepoEvent,
} from './lexicons'
import { type FirehoseSubscriptionOptions, SubscribeReposMessage } from './util'

if (!workerData) {
  throw new Error('Must be run as a worker')
}

const { dbOptions, idResolverOptions } =
  workerData as FirehoseSubscriptionOptions
if (!dbOptions || !idResolverOptions) {
  throw new Error('worker missing options')
}

const db = new Database(dbOptions)

const idResolver = new IdResolver({
  ...idResolverOptions,
  didCache: new MemoryCache(),
})
const background = new BackgroundQueue(db)
const indexingSvc = new IndexingService(db, idResolver, background)

export default async function handleMessage(msg: SubscribeReposMessage) {
  if (!indexingSvc) {
    throw new Error('Worker not initialized')
  }

  if ('commit' in msg && msg.commit.$link)
    msg.commit = CID.parse(msg.commit.$link)
  if ('ops' in msg && msg.ops.length)
    msg.ops.forEach((op) => {
      // @ts-expect-error - required but nullable
      op.cid = op.cid ? CID.parse(op.cid) : null
    })

  const parsed = await parseEvt(isValidRepoEvent(msg))
  if ('error' in parsed) return parsed

  for (const evt of parsed) {
    await processEvent(evt)
  }
  return { success: true }
}

async function parseEvt(evt: RepoEvent): Promise<Event[] | { error: unknown }> {
  try {
    if (isCommit(evt)) {
      return parseCommitUnauthenticated(evt)
    } else if (isAccount(evt)) {
      const parsed = parseAccount(evt)
      return parsed ? [parsed] : []
    } else if (isIdentity(evt)) {
      const parsed = await parseIdentity(idResolver, evt, true)
      return parsed ? [parsed] : []
    } else if (isSync(evt)) {
      const parsed = await parseSync(evt)
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
    return {
      error: 'error in parsing and authenticating firehose event ' + evt.seq,
    }
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
  } else if (evt.event === 'sync') {
    return Promise.all([
      indexingSvc.setCommitLastSeen(evt.did, evt.cid, evt.rev),
      indexingSvc.indexHandle(evt.did, evt.time),
    ])
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
