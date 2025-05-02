import { setTimeout } from 'node:timers/promises'
import { workerData } from 'node:worker_threads'
import { readCar } from '@atcute/car'
import type {
  AccountEvent,
  CommitEvent,
  IdentityEvent,
  SyncEvent,
} from '@skyware/firehose'
import { CID } from 'multiformats/cid'
import { BackgroundQueue, Database } from '@atproto/bsky'
import { IndexingService } from '@atproto/bsky/dist/data-plane/server/indexing/index.js'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { BlobRef } from '@atproto/lexicon'
import { WriteOpAction } from '@atproto/repo'
import { AtUri } from '@atproto/syntax'
import type { FirehoseSubscriptionOptions } from './subscription.js'

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

export default async (
  msg: CommitEvent | AccountEvent | IdentityEvent | SyncEvent,
) => {
  if (!msg) return { success: true }
  try {
    if (msg.$type === 'com.atproto.sync.subscribeRepos#identity') {
      // await indexingSvc.indexHandle(msg.did, msg.time, true)
    } else if (msg.$type === 'com.atproto.sync.subscribeRepos#account') {
      // if (msg.active === false && msg.status === 'deleted') {
      //   await indexingSvc.deleteActor(msg.did)
      // } else {
      //   await indexingSvc.updateActorStatus(msg.did, msg.active, msg.status)
      // }
    } else if (msg.$type === 'com.atproto.sync.subscribeRepos#sync') {
      // const cid = parseCid(readCar(msg.blocks).header.data.roots[0])
      // await Promise.all([
      //   indexingSvc.setCommitLastSeen(msg.did, cid, msg.rev),
      //   indexingSvc.indexHandle(msg.did, msg.time),
      // ])
    } else if (msg.$type === 'com.atproto.sync.subscribeRepos#commit') {
      for (const op of msg.ops) {
        const uri = AtUri.make(msg.repo, ...op.path.split('/'))
        if (op.action !== 'delete')
          // @ts-expect-error
          indexingSvc
            .findIndexerForCollection(uri.collection)
            ?.assertValidRecord(jsonToLex(op.record))

        // const indexFn =
        //   op.action === 'delete'
        //     ? indexingSvc.deleteRecord(uri)
        //     : indexingSvc.indexRecord(
        //         uri,
        //         op.cid,
        //         jsonToLex(op.record),
        //         op.action === 'create'
        //           ? WriteOpAction.Create
        //           : WriteOpAction.Update,
        //         msg.time,
        //       )
        // background.add(() => indexingSvc.indexHandle(msg.repo, msg.time))
        // await Promise.all([
        //   indexFn,
        //   indexingSvc.setCommitLastSeen(msg.repo, msg.commit, msg.rev),
        // ])
      }
    }
    return { success: true }
  } catch (err) {
    return { success: false, error: err }
  }
}

function parseCid(
  cid: { $link: string } | { bytes: Uint8Array } | CID | string,
) {
  if (cid instanceof CID) {
    return cid
  } else if (typeof cid === 'string') {
    return CID.parse(cid)
  } else if ('$link' in cid) {
    return CID.parse(cid.$link)
  } else if ('bytes' in cid) {
    return CID.decode(cid.bytes)
  }
  throw new Error('Invalid CID ' + JSON.stringify(cid))
}

function jsonToLex(val: Record<string, unknown>): unknown {
  try {
    // walk arrays
    if (Array.isArray(val)) {
      return val.map((item) => jsonToLex(item))
    }
    // objects
    if (val && typeof val === 'object') {
      // check for dag json values
      if (
        '$link' in val &&
        typeof val['$link'] === 'string' &&
        Object.keys(val).length === 1
      ) {
        return CID.parse(val['$link'])
      }
      if ('bytes' in val && val['bytes'] instanceof Uint8Array) {
        return CID.decode(val.bytes)
      }
      if (
        val['$type'] === 'blob' ||
        (typeof val['cid'] === 'string' && typeof val['mimeType'] === 'string')
      ) {
        if ('ref' in val && typeof val['size'] === 'number') {
          return new BlobRef(
            CID.decode((val.ref as any).bytes),
            val.mimeType as string,
            val.size,
          )
        } else {
          return new BlobRef(
            CID.parse(val.cid as string),
            val.mimeType as string,
            -1,
            val as never,
          )
        }
      }
      // walk plain objects
      const toReturn: Record<string, unknown> = {}
      for (const key of Object.keys(val)) {
        // @ts-expect-error
        toReturn[key] = jsonToLex(val[key])
      }
      return toReturn
    }
  } catch {}
  // pass through
  return val
}
