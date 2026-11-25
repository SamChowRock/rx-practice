import assert from 'node:assert/strict'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  Observable, Subscriber, Subscription, UnsubscriptionError, finalize, lastValueFrom, using,
  type CompletionObserver, type ErrorObserver, type NextObserver, type Observer,
  type PartialObserver, type SubscriptionLike, type TeardownLogic,
  type Unsubscribable,
} from 'rxjs'
import { finalize as pipelineFinalize } from 'rxjs/operators'

assert.equal(finalize, pipelineFinalize)
type Exit = 'complete' | 'error' | 'close'

async function openPanel(exit: Exit) {
  const rows: number[] = []
  const signals: string[] = []
  let started = 0, ticks = 0, cleaned = 0, finalized = 0, released = 0
  let finish!: () => void
  const finished = new Promise<void>(resolve => { finish = resolve })
  const clock = new Observable<number>((sink: Subscriber<number>): TeardownLogic => {
    started++
    const timer = setInterval(() => {
      ticks++
      sink.next(ticks)
      if (ticks === 2 && exit === 'complete') sink.complete()
      if (ticks === 2 && exit === 'error') sink.error(new Error('offline'))
    }, 3)
    return () => {
      clearInterval(timer)
      cleaned++
    }
  })
  const source = using(
    (): Unsubscribable => ({ unsubscribe() { released++ } }),
    () => clock,
  ).pipe(finalize(() => { finalized++; finish() }))
  assert.equal(started, 0)
  const observer: Observer<number> = {
    next(row) {
      rows.push(row)
      // The first clock tick is asynchronous: panel has been assigned.
      if (row === 2 && exit === 'close') panel.unsubscribe()
    },
    complete() { signals.push('complete') },
    error(e: Error) { signals.push(e.message) },
  }
  const panel: SubscriptionLike = source.subscribe(observer)
  await finished
  panel.unsubscribe()
  await sleep(12)
  assert.deepEqual(rows, [1, 2])
  assert.deepEqual(signals, exit === 'close' ? [] : [exit === 'error' ? 'offline' : 'complete'])
  assert.deepEqual([started, ticks, cleaned, finalized, released, panel.closed],
    [1, 2, 1, 1, 1, true])
  console.log(exit, { rows, signals, started, ticks, cleaned, finalized, released })
}
for (const exit of ['complete', 'error', 'close'] as const) await openPanel(exit)

const order: string[] = []
const owner = new Subscription(() => order.push('owner'))
const child = new Subscription(() => order.push('child'))
owner.add(child)
owner.remove(child)
owner.add(() => { throw new Error('socket') })
owner.add(() => { throw new Error('file') })
owner.add(() => order.push('tail'))
assert.throws(() => owner.unsubscribe(), (e: unknown) =>
  e instanceof UnsubscriptionError && e.errors.map(x => x.message).join(',') === 'socket,file')
assert.equal(child.closed, false)
child.unsubscribe()
owner.add(() => order.push('late'))
assert.deepEqual(order, ['owner', 'tail', 'child', 'late'])
assert.equal(Subscription.EMPTY.closed, true)
console.log('父订阅聚合两个清理错误，仍执行 tail；移出的子订阅单独关闭', order)

const one = new Observable<number>(s => { s.next(1); s.complete(); s.next(99) })
const nextOnly: NextObserver<number> = { next: n => assert.equal(n, 1) }
const errorOnly: ErrorObserver<number> = { error: assert.fail }
const endOnly: CompletionObserver<number> = { complete() {} }
for (const observer of [nextOnly, errorOnly, endOnly] satisfies PartialObserver<number>[]) {
  assert.equal(one.subscribe(observer).closed, true)
}
await one.forEach(nextOnly.next)
await one.forEach(nextOnly.next, Promise)
assert.equal(await one.toPromise(), await lastValueFrom(one))
const empty = new Observable<never>(s => s.complete())
assert.equal(await empty.toPromise(), await lastValueFrom(empty, { defaultValue: undefined }))
const legacy: number[] = [], modern: number[] = []
Observable.create((s: Subscriber<number>) => { s.next(1); s.complete() })
  .subscribe((n: number) => legacy.push(n), assert.fail, () => legacy.push(0))
one.subscribe({ next: n => modern.push(n), complete: () => modern.push(0) })
assert.deepEqual(legacy, modern)
const observed: number[] = []
const destination: Observer<number> = {
  next: n => observed.push(n), error: assert.fail, complete: () => observed.push(0),
}
one.subscribe(new Subscriber(destination))
one.subscribe(Subscriber.create<number>(n => destination.next(n!), destination.error, destination.complete))
one.subscribe(destination)
assert.deepEqual(observed, [...modern, ...modern, ...modern])
const passthrough = { call(s: Subscriber<number>, source: Observable<number>) { return source.subscribe(s) } }
const lifted = one.lift<number>(passthrough)
assert.equal(lifted.source, one)
assert.equal(lifted.operator, passthrough)
const forwarded = one.pipe(source => new Observable<number>(s => source.subscribe(s)))
assert.equal(await lastValueFrom(lifted), await lastValueFrom(forwarded))
console.log('旧订阅和对象订阅收到相同值与完成标记', modern)

// Compile-time exercises. These declarations are checked without executing their bodies.
function checkTypes(s: Subscriber<number>, full: Observer<number>) {
  const partial: PartialObserver<number> = full
  one.subscribe(partial)
  // @ts-expect-error The full observer also needs complete and error.
  const nextAsFull: Observer<number> = nextOnly
  // @ts-expect-error A next observer must supply next.
  const noNext: NextObserver<number> = { complete() {} }
  // @ts-expect-error An error observer must supply error.
  const noError: ErrorObserver<number> = { next() {} }
  // @ts-expect-error A completion observer must supply complete.
  const noEnd: CompletionObserver<number> = { error() {} }
  // @ts-expect-error A boolean is not a cancellation method.
  const noMethod: Unsubscribable = { unsubscribe: true }
  // @ts-expect-error A number subscriber accepts only numbers.
  s.next('1')
  // @ts-expect-error PartialObserver requires at least one notification handler.
  const missing: PartialObserver<number> = {}
  // @ts-expect-error A timer identifier is not cleanup logic.
  const leak: TeardownLogic = 42
  // @ts-expect-error A handle with no closed flag is only Unsubscribable.
  const incomplete: SubscriptionLike = { unsubscribe() {} }
  return [missing, leak, incomplete, nextAsFull, noNext, noError, noEnd, noMethod]
}

import { take } from 'rxjs'

// Three panels read the same cache. Count the work at the decoder entrance.
function preview(mode: 'handle' | 'limit' | 'cooperate') {
  let decoded = 0
  const received: string[] = []
  const cache = new Observable<string>(sink => {
    for (const encoded of ['A', 'B', 'C']) {
      if (mode === 'cooperate' && sink.closed) break
      decoded++
      sink.next(encoded)
    }
    sink.complete()
  })
  let handle: Subscription | undefined
  const rows = mode === 'handle' ? cache : cache.pipe(take(1))
  handle = rows.subscribe(row => {
    received.push(row)
    if (mode === 'handle') handle?.unsubscribe()
  })
  return { decoded, received }
}
assert.deepEqual(preview('handle'), { decoded: 3, received: ['A', 'B', 'C'] })
assert.deepEqual(preview('limit'), { decoded: 3, received: ['A'] })
assert.deepEqual(preview('cooperate'), { decoded: 1, received: ['A'] })

const sessionTrace: string[] = []
new Observable<string>(sink => {
  sessionTrace.push('acquire')
  sink.next('title')
  sink.complete()
  sessionTrace.push('return ticket')
  return () => sessionTrace.push('release')
}).subscribe({
  next: title => sessionTrace.push(title),
  complete: () => sessionTrace.push('end'),
})
assert.deepEqual(sessionTrace, ['acquire', 'title', 'end', 'return ticket', 'release'])

function closeMain(captureTicket: boolean) {
  const tickets = new Map<number, Subscriber<string>>()
  let latest = 0
  const feed = new Observable<string>(sink => {
    const ticket = ++latest
    tickets.set(ticket, sink)
    return () => { tickets.delete(captureTicket ? ticket : latest) }
  })
  const seen: string[] = []
  const main = feed.subscribe(() => seen.push('main'))
  const side = feed.subscribe(() => seen.push('side'))
  main.unsubscribe()
  const remaining = [...tickets.keys()]
  for (const sink of tickets.values()) sink.next('update')
  side.unsubscribe()
  return [remaining, seen, tickets.size]
}
assert.deepEqual(closeMain(false), [[1], [], 1])
assert.deepEqual(closeMain(true), [[2], ['side'], 0])
console.log('首行预览解码次数 3、3、1；同步完成后释放；关闭主窗口仅保留票 2')
