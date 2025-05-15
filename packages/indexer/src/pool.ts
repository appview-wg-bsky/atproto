import { DynamicThreadPool } from 'poolifier-web-worker'

export class CustomThreadPool<In, Out> extends DynamicThreadPool<In, Out> {
  // more aggressive scaling; considers the pool busy if
  // 80% of workers are operating at 80% of their concurrency
  // rather than 100% in both conditions by default
  protected override internalBusy(): boolean {
    return (
      this.workerNodes.reduce(
        (accumulator, _, workerNodeKey) =>
          this._isWorkerNodeBusy(workerNodeKey) ? accumulator + 1 : accumulator,
        0,
      ) ===
      this.workerNodes.length * 0.8
    )
  }

  // stolen private method
  // modified to consider a worker busy if it's operating at >= 80% of its concurrency
  private _isWorkerNodeBusy(workerNodeKey: number): boolean {
    const workerNode = this.workerNodes[workerNodeKey]
    if (this.opts.enableTasksQueue === true) {
      return (
        workerNode.info.ready &&
        workerNode.usage.tasks.executing >=
          this.opts.tasksQueueOptions!.concurrency! * 0.8
      )
    }
    return workerNode.info.ready && workerNode.usage.tasks.executing > 0
  }
}
