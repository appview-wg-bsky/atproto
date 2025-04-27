import type { RedisClientOptions } from '@redis/client'
import { ParsedCommit } from '@skyware/firehose'
import type { ComAtprotoSyncSubscribeRepos } from '@atproto/api'
import type { PgOptions } from '@atproto/bsky/dist/data-plane/server/db/types'
import type { IdentityResolverOpts } from '@atproto/identity'

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

export type SubscribeReposMessage =
  | ComAtprotoSyncSubscribeRepos.Account
  | ComAtprotoSyncSubscribeRepos.Identity
  | ComAtprotoSyncSubscribeRepos.Sync
  | ParsedCommit
