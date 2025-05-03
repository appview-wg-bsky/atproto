import { availableParallelism } from 'node:os'
import { SHARE_ENV } from 'node:worker_threads'
import { readCar } from '@atcute/car'
import { decode, decodeFirst, fromBytes, toCidLink } from '@atcute/cbor'
import type { ComAtprotoSyncSubscribeRepos } from '@atcute/client/lexicons'
import { type RedisClientOptions, createClient } from '@redis/client'
import type { Event, RepoOp } from '@skyware/firehose'
import _PQueue from 'p-queue'
// @ts-expect-error — https://github.com/sindresorhus/p-queue/issues/145
const PQueue = _PQueue.default
type PQueue = _PQueue
import { FixedQueue, Piscina } from 'piscina'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'
import { WebSocketKeepAlive } from '@atproto/xrpc-server'
import { FirehoseSubscriptionError, FirehoseWorkerError } from './errors.js'
import type { default as worker } from './worker.js'

declare module 'piscina' {
  interface PiscinaTask {
    [Piscina.queueOptionsSymbol]: {
      did: string
      seq: number
      attempt?: number
    }
  }
}

let messagesReceived = 0,
  messagesParsed = 0,
  messagesProcessed = 0

export class FirehoseSubscription {
  private REDIS_SEQ_KEY = 'bsky_indexer:seq'
  private WORKER_PATH = new URL('./worker.js', import.meta.url).href
  private MAX_ATTEMPTS = 5

  protected firehose!: WebSocketKeepAlive
  protected piscina: Piscina
  protected redis?: ReturnType<typeof createClient>
  protected chunkQueue = new PQueue()
  protected didQueues = new Map<string, PQueue>()

  protected logStatsInterval: NodeJS.Timer | null = null
  protected saveCursorInterval: NodeJS.Timer | null = null

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

    const { dbOptions, idResolverOptions } = this.opts

    this.piscina = new Piscina({
      filename: this.WORKER_PATH,
      env: SHARE_ENV,
      minThreads: this.settings.minWorkers,
      maxThreads: this.settings.maxWorkers,
      concurrentTasksPerWorker: this.settings.maxConcurrency,
      idleTimeout: 30_000,
      taskQueue: new FixedQueue(),
      workerData: {
        dbOptions,
        idResolverOptions,
      },
    })

    if (this.opts.redisOptions)
      this.redis = createClient(this.opts.redisOptions)
  }

  async start(): Promise<void> {
    if (this.opts.redisOptions && !this.redis.isOpen) await this.redis.connect()

    const initialCursor = this.opts.redisOptions
      ? await this.redis.get(this.REDIS_SEQ_KEY)
      : null

    let cursor = ''
    if (initialCursor) {
      console.log(`starting from initial cursor: ${initialCursor}`)
      cursor = initialCursor
    } else if (this.opts.cursor) {
      console.log(`starting from cursor: ${this.opts.cursor}`)
      cursor = this.opts.cursor.toString()
    } else console.log(`starting from latest`)

    this.firehose = new WebSocketKeepAlive({
      getUrl: async () =>
        `${this.opts.service}/xrpc/com.atproto.sync.subscribeRepos?cursor=${cursor}`,
    })

    if (!this.logStatsInterval)
      this.logStatsInterval = setInterval(() => {
        console.log(
          `${Math.round(messagesProcessed / 10)} / ${Math.round(messagesParsed / 10)} / ${Math.round(messagesReceived / 10)} per sec (${Math.round((messagesProcessed / messagesReceived) * 100)}%) [${this.piscina.threads.length}]`,
        )
        messagesReceived = messagesParsed = messagesProcessed = 0
      }, 10_000)

    if (this.opts.redisOptions && !this.saveCursorInterval) {
      this.saveCursorInterval = setInterval(async () => {
        await this.redis.set(this.REDIS_SEQ_KEY, cursor)
      }, 60_000)
    }

    try {
      for await (const c of this.firehose) {
        messagesReceived++
        const chunk = new Uint8Array(c)
        this.chunkQueue.add(() => this.processChunk(chunk))
      }
    } catch (err) {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
      return this.start()
    }
  }

  protected processChunk(chunk: Uint8Array) {
    try {
      const [header, remainder] = decodeFirst(chunk)
      const [body, remainder2] = decodeFirst(remainder)
      if (remainder2.length > 0) {
        throw new Error('excess bytes in message')
      }

      const { t, op } = this.parseHeader(header)

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
        const car = this.readCar(blocks)

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

        this.queueMessage({
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
        })
      } else if (t === '#sync') {
        const {
          seq,
          did,
          blocks: blocksBytes,
          rev,
          time,
        } = body as ComAtprotoSyncSubscribeRepos.Sync

        const blocks = blocksBytes?.$bytes?.length
          ? fromBytes(blocksBytes)
          : new Uint8Array()
        this.queueMessage({
          $type: 'com.atproto.sync.subscribeRepos#sync',
          seq,
          did,
          blocks,
          rev,
          time,
        })
      }

      this.queueMessage({
        $type: `com.atproto.sync.subscribeRepos${t}`,
        ...body,
      })
    } catch (err) {
      this.opts.onError?.(new FirehoseSubscriptionError(err))
    }
  }

  protected queueMessage(message: Event, attempt = 0) {
    messagesParsed++

    const { did, seq } = didAndSeq(message)

    if (attempt > this.MAX_ATTEMPTS) {
      console.error(`max attempts reached for ${did} ${seq}`)
      return
    }

    // Ensure messages for a given did are processed sequentially
    let queue = this.didQueues.get(did)
    if (!queue) {
      queue = new PQueue({ concurrency: 1, timeout: 120_000 })
      this.didQueues.set(did, queue)
    }

    // higher seq → lower priority
    queue
      .add(() => this.processMessage(message), {
        priority: Number.MAX_SAFE_INTEGER - seq,
      })
      .then((res) => {
        if (res?.success) {
          if (queue.size === 0) this.didQueues.delete(did)
          messagesProcessed++
        } else {
          if (res?.error) {
            console.warn(new FirehoseWorkerError(res.error))
          }
          return this.queueMessage(message, attempt + 1)
        }
      })
      .catch((err) => {
        console.error(`uncaught error on ${did} ${seq}`)
        if (err instanceof DOMException && err.name === 'DataCloneError') {
          console.error(`${err.message}\n${JSON.stringify(message)}`)
        } else {
          this.opts.onError?.(new FirehoseWorkerError(err))
        }
      })
  }

  protected parseHeader(header: any): { t: string; op: 1 | -1 } {
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

  protected readCar(buffer: Uint8Array): Map<string, unknown> {
    const records = new Map<string, unknown>()
    for (const { cid, bytes } of readCar(buffer).iterate()) {
      records.set(toCidLink(cid).$link, decode(bytes))
    }
    return records
  }

  protected processMessage(message: Event): ReturnType<typeof worker> {
    return this.piscina.run(message, {
      transferList: 'blocks' in message ? [message.blocks.buffer] : [],
    })
  }

  async destroy() {
    await this?.piscina?.close({ force: true })
    await this?.redis?.quit()
  }
}

function didAndSeq(message: Event) {
  if ('did' in message) return { did: message.did, seq: message.seq }
  else if ('repo' in message) return { did: message.repo, seq: message.seq }
  throw new Error(`message missing did or repo ${JSON.stringify(message)}`)
}

export interface FirehoseSubscriptionOptions {
  service: string
  dbOptions: PgOptions
  redisOptions?: RedisClientOptions
  idResolverOptions?: IdentityResolverOpts
  minWorkers?: number | undefined
  maxWorkers?: number | undefined
  maxConcurrency?: number
  onError?: (err: Error) => void
  cursor?: number
  verbose?: boolean
}
