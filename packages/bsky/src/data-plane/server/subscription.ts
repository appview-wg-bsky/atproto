import { IdResolver } from '@atproto/identity'
import { WriteOpAction } from '@atproto/repo'
import { Firehose, MemoryRunner } from '@atproto/sync'
import { subLogger as log } from '../../logger'
import { BackgroundQueue } from './background'
import { Database } from './db'
import { IndexingService } from './indexing'

export class RepoSubscription {
  firehose: Firehose
  runner: MemoryRunner
  background: BackgroundQueue
  indexingSvc: IndexingService

  constructor(
    public opts: { service: string; db: Database; idResolver: IdResolver },
  ) {
    const { service, db, idResolver } = opts
    this.background = new BackgroundQueue(db)
    this.indexingSvc = new IndexingService(db, idResolver, this.background)

    const { runner, firehose } = createFirehose({
      idResolver,
      service,
      indexingSvc: this.indexingSvc,
      background: this.background,
    })
    this.runner = runner
    this.firehose = firehose
  }

  start() {
    this.firehose.start()
  }

  async restart() {
    await this.destroy()
    const { runner, firehose } = createFirehose({
      idResolver: this.opts.idResolver,
      service: this.opts.service,
      indexingSvc: this.indexingSvc,
      background: this.background,
    })
    this.runner = runner
    this.firehose = firehose
    this.start()
  }

  async processAll() {
    await this.runner.processAll()
    await this.background.processAll()
  }

  async destroy() {
    await this.firehose.destroy()
    await this.runner.destroy()
    await this.background.processAll()
  }
}

let lastReceived = 0,
  lastProcessed = 0,
  eventCount = 0

const createFirehose = (opts: {
  idResolver: IdResolver
  service: string
  indexingSvc: IndexingService
  background: BackgroundQueue
}) => {
  const { idResolver, service, indexingSvc, background } = opts
  const runner = new MemoryRunner()

  setInterval(() => {
    console.log(
      `skew - received - ${fmtTime(Math.abs(lastReceived - Date.now()))}\t—\tev/s ${eventCount / Math.abs(lastReceived - Date.now())}`,
    )
    console.log(
      `skew - processed - ${fmtTime(Math.abs(lastProcessed - Date.now()))}\t—\tev/s ${eventCount / Math.abs(lastProcessed - Date.now())}`,
    )
    eventCount = 0
  }, 10_000)

  const firehose = new Firehose({
    idResolver,
    runner,
    service,
    unauthenticatedHandles: true, // indexing service handles these
    unauthenticatedCommits: true, // @TODO there seems to be a very rare issue where the authenticator thinks a block is missing in deletion ops
    onError: (err) => log.error({ err }, 'error in subscription'),
    handleEvent: async (evt) => {
      eventCount++
      if (evt.time > lastReceived) {
        lastReceived = evt.time
      }
      if (evt.event === 'identity') {
        await indexingSvc.indexHandle(evt.did, evt.time, true)
      } else if (evt.event === 'account') {
        if (evt.active === false && evt.status === 'deleted') {
          await indexingSvc.deleteActor(evt.did)
        } else {
          await indexingSvc.updateActorStatus(evt.did, evt.active, evt.status)
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
      if (evt.time - lastProcessed > 10_000) {
        lastProcessed = evt.time
      }
    },
  })
  return { firehose, runner }
}

const fmtTime = (ms: number) => {
  const secs = Math.floor(ms / 1000)
  const mins = Math.floor(secs / 60)

  if (mins > 0) {
    return `${mins}m${secs % 60}.${(ms % 1000).toFixed(0)}s`
  } else if (secs > 0) {
    return `${secs}.${(ms % 1000).toFixed(0)}s`
  } else {
    return `${ms.toFixed(0)}ms`
  }
}
