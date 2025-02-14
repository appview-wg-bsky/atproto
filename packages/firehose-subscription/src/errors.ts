export class FirehoseSubscriptionError extends Error {
  constructor(err: unknown) {
    super('Error in firehose subscription', { cause: err })
  }
}

export class FirehoseWorkerError extends Error {
  constructor(err: unknown) {
    super('Error in firehose worker', { cause: err })
  }
}
