import type { RedisClientOptions } from '@redis/client'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'

export interface FirehoseSubscriptionOptions {
  service: string
  dbOptions: PgOptions
  redisOptions: RedisClientOptions
  idResolverOptions?: IdentityResolverOpts
  minWorkers?: number
  maxWorkers?: number
  onError?: (err: Error) => void
  cursor?: number
  scaleCheckIntervalMs?: number
}

export interface WorkerData {
  processedPerMinute: number | null
  averageProcessingTime: number | null
}

export type WorkerResponse =
  | {
      type: 'processed'
      id: string
      time: number
    }
  | {
      type: 'error'
      error: Error
    }
