import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'

export interface FirehoseSubscriptionOptions {
  service: string
  dbOptions: PgOptions
  idResolverOptions?: IdentityResolverOpts
  minWorkers?: number
  maxWorkers?: number
  onError?: (err: Error) => void
  cursor?: number
  targetLatencyMs?: number
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
      idResolverOptions: IdentityResolverOpts
    }

export interface WorkerStats {
  processedCount: number
  avgProcessingTimeMs: number
  lastEventTime?: string
  currentLatencyMs: number
  ready?: boolean
}

export type WorkerResponse =
  | {
      type: 'stats'
      stats: WorkerStats
    }
  | {
      type: 'error'
      error: Error
    }
