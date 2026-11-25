import assert from 'node:assert/strict'
import { EventEmitter, getEventListeners } from 'node:events'
import {
  Observable, Subscription, bindCallback, bindNodeCallback, defer, EMPTY, finalize,
  from, fromEvent, fromEventPattern, generate, iif, isObservable, NEVER, observable,
  observeOn, of, queueScheduler, range, scheduled, throwError, subscribeOn, VirtualTimeScheduler,
  type FactoryOrValue, type InteropObservable, type ObservableInput, type ObservableLike,
  type ReadableStreamLike, type Subscribable, type SubscribableOrPromise,
} from 'rxjs'

async function read<T>(source: Observable<T>) {
  const values: T[] = []
  await source.forEach(value => values.push(value))
  return values
}
const history = ['A', 'B']
assert.deepEqual(await read(of(history)), [['A', 'B']])
assert.deepEqual(await read(from(history)), history)
assert.deepEqual(await read(from(history)), history)
assert.deepEqual(await read(of(null, undefined)), [null, undefined])
assert.deepEqual(await read(of()), [])
assert.deepEqual(await read(range(2)), [0, 1])
assert.deepEqual(await read(range(7, 2)), [7, 8])
const options = {
  initialState: 1, condition: (n: number) => n < 5, iterate: (n: number) => n * 2,
}
assert.deepEqual(await read(generate(options)), [1, 2, 4])
const label = (n: number) => 'offset-' + n
assert.deepEqual(await read(generate({ ...options, resultSelector: label })),
  await read(generate(options.initialState, options.condition, options.iterate, label)))
const migrations: [Observable<unknown>, Observable<unknown>][] = [
  [from(history, queueScheduler), scheduled(history, queueScheduler)],
  [of('A', 'B', queueScheduler), scheduled(history, queueScheduler)],
  [range(7, 2, queueScheduler), range(7, 2).pipe(observeOn(queueScheduler))],
  [of<string>(), of()],
]
for (const [oldSource, newSource] of migrations) {
  assert.deepEqual(await read(oldSource), await read(newSource))
}
console.log('历史每次回放 A、B；of(history) 整批发送；倍增游标为 1、2、4')

let calls = 0
const request = () => Promise.resolve(++calls)
const existing = from(request())
assert.equal(calls, 1)
const fresh = defer(() => from(request()))
let online = false
const panel = iif(() => online, fresh, existing)
assert.deepEqual(await read(panel), [1])
online = true
assert.deepEqual(await read(panel), [2])
assert.deepEqual(await read(panel), [3])
console.log('切换来源后，三次打开的请求编号为 1、2、3')

const button = new EventTarget()
const click = () => button.dispatchEvent(new Event('click'))
click()
let starts = 0, stops = 0
const clicks = new Observable<Event>(sink => {
  starts++
  return fromEvent<Event>(button, 'click', { capture: false }).subscribe(sink)
}).pipe(finalize(() => stops++))
const seen: number[] = []
for (const id of [1, 2]) {
  const mount = new Subscription()
  mount.add(clicks.subscribe(() => seen.push(id)))
  assert.equal(getEventListeners(button, 'click').length, 1)
  click()
  mount.unsubscribe()
  assert.equal(getEventListeners(button, 'click').length, 0)
  click()
}
assert.deepEqual([seen, starts, stops], [[1, 2], 2, 2])
console.log('两次挂载各收到一次点击，卸载后监听器均为 0', { starts, stops })

const bus = new EventEmitter()
const wire = fromEvent(bus, 'line', (id, text) => [id, text])
const multi: unknown[] = []
const watch = wire.subscribe(x => multi.push(x))
bus.emit('line', 4, 'ready')
watch.unsubscribe()
assert.deepEqual(multi, [[4, 'ready']])
const sdk = new Map<number, (line: string) => void>()
const feed = fromEventPattern<string>(
  handler => { sdk.set(17, handler); return 17 },
  (_handler, token) => sdk.delete(token),
)
const streamed: string[] = []
const handle = feed.subscribe(line => streamed.push(line))
sdk.get(17)?.('A')
sdk.get(17)?.('B')
handle.unsubscribe()
assert.equal(sdk.size, 0)
assert.deepEqual(streamed, history)
let lookups = 0
const lookup = bindCallback((id: number, done: (id: number, msg: string) => void) => {
  lookups++
  done(id, 'ready')
})
const snapshot = lookup(4)
assert.equal(lookups, 0)
for (let i = 0; i < 2; i++) assert.deepEqual(await read(snapshot), [[4, 'ready']])
assert.equal(lookups, 1)
const format = bindCallback(
  (done: (id: number, msg: string) => void) => done(4, 'ready'),
  (id: number, msg: string) => id + ' ' + msg,
)
assert.deepEqual(await read(format()), ['4 ready'])
assert.deepEqual(await read(bindCallback((done: () => void) => done())()), [undefined])
const load = bindNodeCallback((ok: boolean, done: (err: Error | null, msg?: string) => void) =>
  done(ok ? null : Error('offline'), 'A'))
assert.deepEqual(await read(load(true)), ['A'])
await assert.rejects(read(load(false)), /offline/)
console.log('SDK 连续回调得到两行；单次查询的两个订阅共用一次调用；Node 错误进入 error')

const silent = NEVER.subscribe({ next: assert.fail, complete: assert.fail })
assert.equal(silent.closed, false)
silent.unsubscribe()
assert.deepEqual(await read(EMPTY), [])
const bad = new Error('bad row')
await assert.rejects(read(throwError(bad)), e => e === bad)
await assert.rejects(read(throwError(() => bad)), e => e === bad)
await assert.rejects(read(throwError(bad, queueScheduler)), e => e === bad)
await assert.rejects(read(throwError(() => bad).pipe(observeOn(queueScheduler))), e => e === bad)
const makeFailure = throwError(() => new Error('bad row'))
const factoryCalls = [0,0,0]
const clock = new VirtualTimeScheduler()
const factory = (index: number) => () => { factoryCalls[index]++; return bad }
for (const source of [throwError(factory(0),clock),throwError(factory(1)).pipe(observeOn(clock)),throwError(factory(2)).pipe(subscribeOn(clock))]) {
  source.subscribe({error:assert.fail}).unsubscribe()
}
clock.flush()
assert.deepEqual(factoryCalls,[0,1,0])
const failures: unknown[] = []
for (let i = 0; i < 2; i++) makeFailure.subscribe({ error: e => failures.push(e) })
assert.notEqual(failures[0], failures[1])
console.log('空历史立即完成；静默连接保持打开；错误工厂可为每次订阅生成新错误')

const contract: Subscribable<string> = {
  subscribe(observer) {
    observer.next?.('interop')
    observer.complete?.()
    return { unsubscribe() {} }
  },
}
// Expose both the standard key (when available) and the RxJS 7 fallback.
const bridge: InteropObservable<string> & { '@@observable'(): Subscribable<string> } = {
  [Symbol.observable]: () => contract,
  '@@observable': () => contract,
}
const oldBridge: ObservableLike<string> = bridge
assert.equal(observable, Symbol.observable ?? '@@observable')
assert.deepEqual(await read(from(oldBridge)), ['interop'])
assert.equal(isObservable(contract), false)
assert.equal(isObservable(bridge), false)
assert.equal(isObservable(from(bridge)), true)
let unlocked = false
const stream: ReadableStreamLike<string> = {
  getReader() {
    let sent = false
    return {
      async read() {
        if (sent) return { done: true as const }
        sent = true
        return { done: false as const, value: 'chunk' }
      },
      releaseLock() { unlocked = true },
    }
  },
}
async function* records() { yield 'async' }
const inputs: ObservableInput<string>[] = [
  stream, records(), new Set(['set']), { 0: 'slot', length: 1 }, of('native'),
]
for (const [i, source] of inputs.entries()) {
  assert.deepEqual(await read(from(source)), [['chunk'], ['async'], ['set'], ['slot'], ['native']][i])
}
function resolveTitle(title: FactoryOrValue<string>) {
  return typeof title === 'function' ? title() : title
}
assert.equal(unlocked, true)
assert.equal(resolveTitle('task'), resolveTitle(() => 'task'))
function checkInput() {
  const oldInput: SubscribableOrPromise<string> = contract
  // @ts-expect-error Bare subscribe has no interop key.
  from(oldInput)
  // @ts-expect-error Both bridge type names require an interop method.
  const incompleteBridge: ObservableLike<string> = contract
  // @ts-expect-error A subscription must return a handle.
  const noHandle: Subscribable<string> = { subscribe() {} }
  // @ts-expect-error An array satisfies ObservableInput but not the old union.
  const oldArray: SubscribableOrPromise<string> = ['A']
  // @ts-expect-error A reader must support read and releaseLock.
  const invalid: ReadableStreamLike<string> = { getReader: () => ({}) }
  // @ts-expect-error A factory must return the requested value type.
  resolveTitle(() => 42)
  return [invalid, incompleteBridge, noHandle, oldArray]
}
console.log('互操作键、流读取器、异步迭代器、集合和类数组均可接入')

function* archive() {
  yield 'A'
  yield 'B'
}
const iterator = archive()
const reuseCursor = defer(() => from(iterator))
assert.deepEqual(await read(reuseCursor), ['A', 'B'])
assert.deepEqual(await read(reuseCursor), [])
const reopen = defer(() => from(archive()))
assert.deepEqual(await read(reopen), ['A', 'B'])
assert.deepEqual(await read(reopen), ['A', 'B'])

const ledger = [{ id: 7, text: '等待' }]
const copySlots = ledger.slice()
const published = ledger.map(({ id, text }) => ({ id, text }))
ledger[0].text = '结束'
ledger.push({ id: 8, text: '新行' })
assert.deepEqual(await read(from(copySlots)), [{ id: 7, text: '结束' }])
assert.deepEqual(await read(from(published)), [{ id: 7, text: '等待' }])
const inProgress = ['A', 'B']
const visited: string[] = []
from(inProgress).subscribe(row => {
  visited.push(row)
  if (row === 'A') inProgress.push('C')
})
assert.deepEqual(visited, ['A', 'B', 'C'])

const writes: string[] = []
let deliver!: (row: string) => void
const pending = new Promise<string>(resolve => { deliver = resolve })
const previous = from(pending).subscribe(row => writes.push(row))
previous.unsubscribe()
await from(Promise.resolve('乙')).forEach(row => writes.push(row))
deliver('甲')
assert.equal(await pending, '甲')
assert.deepEqual(writes, ['乙'])

function openLog(canResume: boolean) {
  const stored = [1, 2]
  const received = stored.slice()
  const after = received[received.length - 1]
  const emitter = new EventEmitter()
  function append(id: number) {
    stored.push(id)
    emitter.emit('line', id)
  }
  append(3)
  if (canResume) received.push(...stored.filter(id => id > after))
  // No writer runs between this synchronous catch-up and listener registration.
  const live = fromEvent(emitter, 'line', (id: number) => id)
    .subscribe(id => received.push(id))
  append(4)
  live.unsubscribe()
  return received
}
assert.deepEqual(openLog(false), [1, 2, 4])
assert.deepEqual(openLog(true), [1, 2, 3, 4])
console.log('归档重开可重读；副本固定正文；切换后只写乙；补读找回第 3 行')
