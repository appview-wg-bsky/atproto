import { workerData } from 'node:worker_threads'
import { readCar as iterateCar } from '@atcute/car'
import { decode, decodeFirst, fromBytes, toCidLink } from '@atcute/cbor'
import type { ComAtprotoSyncSubscribeRepos } from '@atcute/client/lexicons'
import type { Event, RepoOp } from '@skyware/firehose'
import { CID } from 'multiformats/cid'
import { ClusterWorker } from 'poolifier'
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

type WorkerData = Pick<
  FirehoseSubscriptionOptions,
  'dbOptions' | 'idResolverOptions'
>

export type WorkerInput = {
  chunk: Uint8Array
}

export type WorkerOutput = {
  success?: boolean
  cursor?: number
  error?: unknown
}

const dbOptions = JSON.parse(
  process.env.DB_OPTIONS || 'null',
) as WorkerData['dbOptions']
const idResolverOptions = JSON.parse(
  process.env.ID_RESOLVER_OPTIONS || 'null',
) as WorkerData['idResolverOptions']

if (!dbOptions || !idResolverOptions) {
  throw new Error('worker missing options')
}

class Worker extends ClusterWorker<WorkerInput, WorkerOutput> {
  db = new Database(dbOptions)
  idResolver = new IdResolver({
    ...idResolverOptions,
    didCache: new MemoryCache(),
  })
  background = new BackgroundQueue(this.db)
  indexingSvc = new IndexingService(this.db, this.idResolver, this.background)

  constructor() {
    super((data) => this.process(data), { maxInactiveTime: 120_000 })
  }

  process = async ({ chunk }: WorkerInput): Promise<WorkerOutput> => {
    try {
      const event = decodeChunk(chunk)
      if (!event) return { success: true }
      const { success, cursor, error } = await this.tryIndexEvent(event)
      if (success) {
        return { success, cursor }
      } else {
        return { success, error }
      }
    } catch (err) {
      return { success: false, error: err }
    }
  }

  async tryIndexEvent(
    event: Event,
  ): Promise<{ success: boolean; cursor?: number; error?: unknown }> {
    const { did, seq } = didAndSeq(event)
    let attempt = 0

    while (attempt <= 5) {
      try {
        // TODO: some way to lock on did across workers
        // that or accept possible out of order events on occasion
        await this.indexEvent(event)
        return { success: true, cursor: seq }
      } catch (err) {
        attempt++
        if (attempt > 5) {
          return {
            success: false,
            error: `max attempts reached for ${did} ${seq}\n${err}`,
          }
        }
      }
    }

    return {
      success: false,
      error: `max attempts reached for ${did} ${seq}`,
    }
  }

  async indexEvent(event: Event) {
    if (!event) return { success: true }

    try {
      if (event.$type === 'com.atproto.sync.subscribeRepos#identity') {
        await this.indexingSvc.indexHandle(event.did, event.time, true)
      } else if (event.$type === 'com.atproto.sync.subscribeRepos#account') {
        if (event.active === false && event.status === 'deleted') {
          await this.indexingSvc.deleteActor(event.did)
        } else {
          await this.indexingSvc.updateActorStatus(
            event.did,
            event.active,
            event.status,
          )
        }
      } else if (event.$type === 'com.atproto.sync.subscribeRepos#sync') {
        const cid = parseCid(iterateCar(event.blocks).header.data.roots[0])
        await Promise.all([
          this.indexingSvc.setCommitLastSeen(event.did, cid, event.rev),
          this.indexingSvc.indexHandle(event.did, event.time),
        ])
      } else if (event.$type === 'com.atproto.sync.subscribeRepos#commit') {
        for (const op of event.ops) {
          const uri = AtUri.make(event.repo, ...op.path.split('/'))
          const indexFn =
            op.action === 'delete'
              ? this.indexingSvc.deleteRecord(uri)
              : this.indexingSvc.indexRecord(
                  uri,
                  op.cid,
                  jsonToLex(op.record),
                  op.action === 'create'
                    ? WriteOpAction.Create
                    : WriteOpAction.Update,
                  event.time,
                )
          this.background.add(() =>
            this.indexingSvc.indexHandle(event.repo, event.time),
          )
          await Promise.all([
            indexFn,
            this.indexingSvc.setCommitLastSeen(
              event.repo,
              event.commit,
              event.rev,
            ),
          ])
        }
      }
      return { success: true }
    } catch (err) {
      return { success: false, error: err }
    }
  }
}

function decodeChunk(chunk: Uint8Array): Event {
  const [header, remainder] = decodeFirst(chunk)
  const [body, remainder2] = decodeFirst(remainder)
  if (remainder2.length > 0) {
    throw new Error('excess bytes in message')
  }

  const { t, op } = parseHeader(header)

  if (op === -1) {
    throw new Error(`error: ${body.message}\nerror code: ${body.error}`)
  }

  if (t === '#commit') {
    const {
      seq,
      repo,
      commit,
      rev,
      since,
      blocks: blocksBytes,
      ops: commitOps,
      prevData,
      time,
    } = body as ComAtprotoSyncSubscribeRepos.Commit

    if (!blocksBytes?.$bytes?.length) return

    const blocks = fromBytes(blocksBytes)
    if (!blocks?.length) return

    const car = readCar(blocks)

    const ops: Array<RepoOp> = []
    for (const op of commitOps) {
      const action: 'create' | 'update' | 'delete' = op.action as any
      if (action === 'create') {
        if (!op.cid) continue
        const record = car.get(op.cid.$link)
        if (!record) continue
        ops.push({
          action,
          path: op.path,
          cid: op.cid.$link,
          record,
        })
      } else if (action === 'update') {
        if (!op.cid) continue
        const record = car.get(op.cid.$link)
        if (!record) continue
        ops.push({
          action,
          path: op.path,
          cid: op.cid.$link,
          ...(op.prev ? { prev: op.prev.$link } : {}),
          record,
        })
      } else if (action === 'delete') {
        ops.push({
          action,
          path: op.path,
          ...(op.prev ? { prev: op.prev.$link } : {}),
        })
      } else {
        throw new Error(`Unknown action: ${action}`)
      }
    }

    return {
      $type: 'com.atproto.sync.subscribeRepos#commit',
      seq,
      repo,
      commit: commit.$link,
      rev,
      since,
      blocks,
      ops,
      ...(prevData ? { prevData: prevData.$link } : {}),
      time,
    }
  } else if (t === '#sync') {
    const {
      seq,
      did,
      blocks: blocksBytes,
      rev,
      time,
    } = body as ComAtprotoSyncSubscribeRepos.Sync

    if (!blocksBytes?.$bytes?.length) return
    const blocks = fromBytes(blocksBytes)

    return {
      $type: 'com.atproto.sync.subscribeRepos#sync',
      seq,
      did,
      blocks,
      rev,
      time,
    }
  } else if (t === '#account' || t === '#identity') {
    return {
      $type: `com.atproto.sync.subscribeRepos${t}`,
      ...body,
    }
  } else {
    console.warn(`unknown message type ${t} ${body}`)
  }
}

function parseHeader(header: any): { t: string; op: 1 | -1 } {
  if (
    !header ||
    typeof header !== 'object' ||
    !header.t ||
    typeof header.t !== 'string' ||
    !header.op ||
    typeof header.op !== 'number'
  ) {
    throw new Error('invalid header received')
  }
  return { t: header.t, op: header.op }
}

function readCar(buffer: Uint8Array): Map<string, unknown> {
  const records = new Map<string, unknown>()
  for (const { cid, bytes } of iterateCar(buffer).iterate()) {
    records.set(toCidLink(cid).$link, decode(bytes))
  }
  return records
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
  } catch {
    // pass through
  }
  return val
}

function didAndSeq(evt: Event) {
  if ('did' in evt) return { did: evt.did, seq: evt.seq }
  else if ('repo' in evt) return { did: evt.repo, seq: evt.seq }
  throw new Error(`evt missing did or repo ${JSON.stringify(evt)}`)
}

export default new Worker()
