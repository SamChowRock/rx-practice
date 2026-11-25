import assert from 'node:assert/strict'
import {
  AsyncSubject, BehaviorSubject, ObjectUnsubscribedError, Observable, ReplaySubject,
  Subject, Subscription, combineLatest, connect, connectable, defer, finalize, map,
  merge, of, share, shareReplay, withLatestFrom
} from 'rxjs'
import type { Connectable, ConnectConfig, ShareConfig, ShareReplayConfig, SubjectLike } from 'rxjs'

const trace = <T>(source: Observable<T>) => {
  const values: (T | '完成' | '错误')[] = []
  source.subscribe({ next: v => values.push(v), complete: () => values.push('完成'), error: () => values.push('错误') })
  return values
}
for (const [bus, before, after] of [
  [new Subject<number>(), [2,'完成'], ['完成']],
  [new BehaviorSubject(0), [1,2,'完成'], ['完成']],
  [new ReplaySubject<number>(2), [1,2,'完成'], [1,2,'完成']],
  [new AsyncSubject<number>(), [2,'完成'], [2,'完成']]
] as const) {
  bus.next(1)
  const early = trace(bus)
  bus.next(2)
  if (bus instanceof AsyncSubject) assert.deepEqual(early, [])
  bus.complete()
  assert.deepEqual(early, before)
  assert.deepEqual(trace(bus), after)
  console.log(bus.constructor.name, '1 之后加入 / 完成后加入', early, after)
}
let now = 0
const recent = new ReplaySubject<number>(2, 5, { now: () => now })
recent.next(10); now = 4; recent.next(20); now = 5
assert.deepEqual(trace(recent), [20])
const state = new BehaviorSubject(7)
assert.equal(state.value, state.getValue())
state.error(new Error('离线'))
assert.throws(() => state.getValue(), /离线/)
const disposed = new Subject<number>()
disposed.unsubscribe()
assert.equal(disposed.closed, true)
assert.throws(() => disposed.next(1), ObjectUnsubscribedError)

// 旧实现允许读写走两个独立方向。分别记录两个方向，防止迁移时合并错通道。
function bridgeTrace(legacy: boolean) {
  const destination = new Subject<number>()
  const source = new Subject<number>()
  const writes = trace(destination)
  const bridge = legacy ? Subject.create(destination, source) as Subject<number> : undefined
  const reads = trace(bridge ?? source.asObservable())
  if (bridge) bridge.next(8)
  else destination.next(8)
  source.next(9)
  const observed = legacy ? source.observers.length > 0 : source.observed
  source.complete()
  return { writes, reads, observed }
}
assert.deepEqual(bridgeTrace(true), bridgeTrace(false))
assert.deepEqual(bridgeTrace(false), { writes:[8], reads:[9,'完成'], observed:true })

const wire = new Subject<number>()
let opens = 0, closes = 0
const channel = new Observable<number>(subscriber => {
  opens++
  return wire.subscribe(subscriber)
}).pipe(finalize(() => closes++))
const naiveA = channel.subscribe(), naiveB = channel.subscribe()
assert.equal(opens, 2)
naiveA.unsubscribe(); naiveB.unsubscribe()
opens = 0; closes = 0
const shared = channel.pipe(share())
const detached: string[] = []
const panelRows: string[] = []
function mount(name: string) {
  const panel = new Subscription()
  panel.add(shared.pipe(finalize(() => detached.push(name))).subscribe(value => panelRows.push(name + value)))
  return panel
}
const first = mount('日志')
wire.next(1)
const second = mount('计数')
wire.next(2)
first.unsubscribe()
assert.deepEqual([opens, closes, detached.slice()], [1,0,['日志']])
wire.next(3)
assert.deepEqual(panelRows, ['日志1','日志2','计数2','计数3'])
second.unsubscribe()
assert.deepEqual([opens, closes, detached], [1,1,['日志','计数']])
console.log('两面板共享：启动、最后离开时清理、面板退出', opens, closes, detached)

const replayConfig: ShareReplayConfig = { bufferSize: 1, refCount: true }
let loads = 0
const cached = defer(() => of(++loads)).pipe(shareReplay(replayConfig))
assert.deepEqual(trace(cached), [1,'完成'])
assert.deepEqual(trace(cached), [1,'完成'])
const reset = new Subject<void>()
const options: ShareConfig<number> = { connector: () => new ReplaySubject(1),
  resetOnComplete: () => reset, resetOnError: true, resetOnRefCountZero: true }
const expiring = defer(() => of(++loads)).pipe(share(options))
assert.deepEqual(trace(expiring), [2,'完成'])
assert.deepEqual(trace(expiring), [2,'完成'])
reset.next()
assert.deepEqual(trace(expiring), [3,'完成'])
console.log('完成缓存：两次读取载入一次；显式失效后版本', loads)

const config: ConnectConfig<number> = { connector: () => new Subject<number>() }
let starts = 0
const dual = defer(() => { starts++; return of(1,2) }).pipe(connect(s =>
  merge(s.pipe(map(n => '行' + n)), s.pipe(map(n => '计' + n))), config))
assert.deepEqual(trace(dual), ['行1','计1','行2','计2','完成'])
assert.equal(starts, 1)
const manual: Connectable<number> = connectable(channel, { ...config, resetOnDisconnect: true })
const rows = trace(manual)
wire.next(1)
const connection = manual.connect()
wire.next(2); connection.unsubscribe(); wire.next(3)
assert.deepEqual(rows, [2])
console.log('手动连接区间内收到', rows)

const log = new Subject<number>(), terms = new Subject<string>(), clicks = new Subject<void>()
const view = trace(combineLatest([log, terms]))
const files = trace(clicks.pipe(withLatestFrom(log, terms), map(([,n,t]) => [n,t])))
log.next(1); clicks.next(); terms.next('A'); clicks.next(); terms.next('B')
assert.deepEqual(view, [[1,'A'],[1,'B']])
assert.deepEqual(files, [[1,'A']])
console.log('晚挂筛选栏：视图 / 导出', view, files)

const input: SubjectLike<number> = new Subject<number>()
// @ts-expect-error 可订阅的只读视图缺少写入端
const readOnly: SubjectLike<number> = wire.asObservable()
// @ts-expect-error 普通 Observable 无 connect()
const auto: Connectable<number> = shared
// @ts-expect-error connect 配置一旦传入就必须声明 connector
const noFactory: ConnectConfig<number> = {}
// @ts-expect-error reset 工厂必须返回 ObservableInput
const badReset: ShareConfig<number> = { resetOnComplete: () => true }
// @ts-expect-error 对象重载要求明确 refCount
const ambiguous: ShareReplayConfig = { bufferSize: 1 }

import { VirtualTimeScheduler, scan, timer } from 'rxjs'

{
  const time = new VirtualTimeScheduler()
  let ageLoads = 0, ttlLoads = 0
  const aged = defer(() => of(++ageLoads)).pipe(shareReplay({
    bufferSize: 1, windowTime: 5, scheduler: time, refCount: true
  }))
  const renewed = defer(() => of(++ttlLoads)).pipe(share({
    connector: () => new ReplaySubject<number>(1),
    resetOnComplete: () => timer(5, time)
  }))
  for (const source of [aged, renewed]) assert.deepEqual(trace(source), [1,'完成'])
  time.schedule(() => assert.deepEqual(trace(aged), [1,'完成']), 4)
  time.schedule(() => {
    assert.deepEqual(trace(renewed), [1,'完成'])
    assert.equal(ttlLoads, 1)
  }, 4)
  time.schedule(() => assert.deepEqual(trace(aged), ['完成']), 5)
  time.schedule(() => assert.deepEqual([ageLoads, ttlLoads], [1,1]), 7)
  time.schedule(() => assert.deepEqual(trace(renewed), [2,'完成']), 8)
  time.flush()
}

{
  const time = new VirtualTimeScheduler()
  const rows = new Subject<string>()
  const lifetime: string[] = []
  const session = defer(() => {
    lifetime.push('开@' + time.now())
    return rows.pipe(finalize(() => lifetime.push('关@' + time.now())))
  }).pipe(share({ resetOnRefCountZero: () => timer(3, time) }))
  const visible: string[] = []
  let reader = session.subscribe(v => visible.push(v))
  time.schedule(() => {
    reader.unsubscribe()
    rows.next('空档')
  }, 2)
  time.schedule(() => { reader = session.subscribe(v => visible.push(v)) }, 3)
  time.schedule(() => rows.next('在线'), 5)
  time.schedule(() => reader.unsubscribe(), 6)
  time.schedule(() => session.subscribe().unsubscribe(), 10)
  time.flush()
  assert.deepEqual(lifetime, ['开@0','关@9','开@10','关@13'])
  assert.deepEqual(visible, ['在线'])
}

{
  const bus = new Subject<string>()
  const table: string[] = []
  bus.subscribe(event => {
    table.push(event)
    if (event === '清屏') bus.next('已清屏')
  })
  const audit = trace(bus)
  bus.next('清屏')
  assert.deepEqual(table, ['清屏','已清屏'])
  assert.deepEqual(audit, ['已清屏','清屏'])
}
{
  const errors = new Subject<void>()
  const count = errors.pipe(scan(total => total + 1, 0))
  const sessionTotal = new BehaviorSubject(0)
  const owner = count.subscribe(value => sessionTotal.next(value))
  errors.next()
  errors.next()
  const lateSessionView = trace(sessionTotal)
  const latePanelCount = trace(count)
  errors.next()
  assert.deepEqual(lateSessionView, [2,3])
  assert.deepEqual(latePanelCount, [1])
  owner.unsubscribe()
}
console.log('过期读取、延时断连、同步回写与会话计数均已核对')
