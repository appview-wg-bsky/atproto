export class FirehoseSubscriptionError extends Error {
  constructor(err: unknown) {
    super('error in firehose subscription', { cause: err })
  }
}

export class FirehoseWorkerError extends Error {
  constructor(err: unknown) {
    super('error in firehose worker', { cause: err })
  }
}
