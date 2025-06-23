import { CID } from 'multiformats/cid'
import { AtUri, AtpAgent } from '@atproto/api'
import { WriteOpAction } from '@atproto/repo'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { BackgroundQueue } from '../../../../data-plane/server/background'
import { Database } from '../../../../data-plane/server/db'
import { IndexingService } from '../../../../data-plane/server/indexing'
import {
  HydrateCtx,
  HydrationState,
  Hydrator,
} from '../../../../hydration/hydrator'
import { Server } from '../../../../lexicon'
import { QueryParams } from '../../../../lexicon/types/app/bsky/actor/getProfile'
import { createPipeline, noRules } from '../../../../pipeline'
import { Views } from '../../../../views'
import { resHeaders } from '../../../util'

const indexProfilesDb = new Database({
    url: process.env.BSKY_DB_POSTGRES_URL!,
    schema: process.env.BSKY_DB_POSTGRES_SCHEMA!,
  }),
  indexProfilesQueue = new BackgroundQueue(indexProfilesDb)

export default function (server: Server, ctx: AppContext) {
  const getProfile = createPipeline(skeleton, hydration, noRules, presentation)
  server.app.bsky.actor.getProfile({
    auth: ctx.authVerifier.optionalStandardOrRole,
    handler: async ({ auth, params, req }) => {
      const { viewer, includeTakedowns } = ctx.authVerifier.parseCreds(auth)
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
        includeTakedowns,
      })

      const result = await getProfile({ ...params, hydrateCtx }, ctx)

      // awful hack, remove asap
      if (!result.createdAt && !result.displayName) {
        indexProfilesQueue.add(async () => {
          const idx = new IndexingService(
            indexProfilesDb,
            ctx.idResolver,
            indexProfilesQueue,
          )
          const agent = new AtpAgent({
            service: (await ctx.idResolver.did.resolveAtprotoData(result.did))
              .pds,
          })
          const { data: profile } = await agent.com.atproto.repo.getRecord({
            repo: result.did,
            collection: 'app.bsky.actor.profile',
            rkey: 'self',
          })
          if (profile)
            await idx.indexRecord(
              new AtUri(profile.uri),
              CID.parse(profile.cid!),
              profile.value,
              WriteOpAction.Create,
              // @ts-expect-error
              profile.value.createdAt || new Date().toISOString(),
            )
        })
      }

      const repoRev = await ctx.hydrator.actor.getRepoRevSafe(viewer)

      return {
        encoding: 'application/json',
        body: result,
        headers: resHeaders({
          repoRev,
          labelers: hydrateCtx.labelers,
        }),
      }
    },
  })
}

const skeleton = async (input: {
  ctx: Context
  params: Params
}): Promise<SkeletonState> => {
  const { ctx, params } = input
  const [did] = await ctx.hydrator.actor.getDids([params.actor])
  if (!did) {
    throw new InvalidRequestError('Profile not found')
  }
  return { did }
}

const hydration = async (input: {
  ctx: Context
  params: Params
  skeleton: SkeletonState
}) => {
  const { ctx, params, skeleton } = input
  return ctx.hydrator.hydrateProfilesDetailed(
    [skeleton.did],
    params.hydrateCtx.copy({
      includeActorTakedowns: true,
    }),
  )
}

const presentation = (input: {
  ctx: Context
  params: Params
  skeleton: SkeletonState
  hydration: HydrationState
}) => {
  const { ctx, params, skeleton, hydration } = input
  const profile = ctx.views.profileDetailed(skeleton.did, hydration)
  if (!profile) {
    throw new InvalidRequestError('Profile not found')
  } else if (!params.hydrateCtx.includeTakedowns) {
    if (ctx.views.actorIsTakendown(skeleton.did, hydration)) {
      throw new InvalidRequestError(
        'Account has been suspended',
        'AccountTakedown',
      )
    } else if (ctx.views.actorIsDeactivated(skeleton.did, hydration)) {
      throw new InvalidRequestError(
        'Account is deactivated',
        'AccountDeactivated',
      )
    }
  }
  return profile
}

type Context = {
  hydrator: Hydrator
  views: Views
}

type Params = QueryParams & {
  hydrateCtx: HydrateCtx
}

type SkeletonState = { did: string }
