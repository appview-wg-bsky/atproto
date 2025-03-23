import type { RedisOptions } from 'ioredis'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'

export interface FirehoseSubscriptionOptions {
  service: string
  dbOptions: PgOptions
  redisOptions: RedisOptions | string
  idResolverOptions?: IdentityResolverOpts
  minWorkers?: number
  maxWorkers?: number
  onError?: (err: Error) => void
  cursor?: number
  scaleCheckIntervalMs?: number
}

export type WorkerMessage =
  | {
      type: 'chunk'
      data: Uint8Array
    }
  | {
      type: 'init'
      dbOptions: PgOptions
      redisOptions: RedisOptions | string
      idResolverOptions: IdentityResolverOpts
    }

export type WorkerResponse = {
  type: 'error'
  error: Error
}
