import assert from 'node:assert/strict'
import {
  BehaviorSubject, EMPTY, NEVER, Observable, Subject, VirtualTimeScheduler,
  combineLatest, combineLatestAll, combineLatestWith, concat, concatMap, concatWith,
  debounceTime, exhaustMap, filter, finalize, forkJoin, map, merge, mergeWith,
  of, race, raceWith, scan, sequenceEqual, startWith, switchMap, timer,
  withLatestFrom, zip, zipAll, zipWith
} from 'rxjs'
import type {
  Cons, Head, Tail, ObservableInputTuple, ObservedValueOf, ObservedValuesFromArray,
  ObservedValueTupleFromArray, ObservedValueUnionFromArray, ValueFromArray
} from 'rxjs'

function check<T>(label: string, source: Observable<T>, expected: T[]) {
  const values: T[] = []
  let complete = false
  source.subscribe({ next: v => values.push(v), complete: () => { complete = true } })
  assert.deepEqual(values, expected)
  assert.ok(complete)
  console.log(label, values, '完成')
}

const history = of('H1', 'H2')
const labels = of('A', 'B')
for (const source of [combineLatest([history, labels]), history.pipe(combineLatestWith(labels)),
  of(history, labels).pipe(combineLatestAll())]) {
  check('最新值：第二路到时第一路已到 H2', source, [['H2', 'A'], ['H2', 'B']])
}
for (const source of [zip(history, labels), history.pipe(zipWith(labels)), of(history, labels).pipe(zipAll())]) {
  check('序号配对', source, [['H1', 'A'], ['H2', 'B']])
}
check('完成后交最后一组', forkJoin({ history, labels }), [{ history: 'H2', labels: 'B' }])
check('旧展开参数', forkJoin(history, labels), [['H2', 'B']])
check('新版数组', forkJoin([history, labels]), [['H2', 'B']])
check('零输入', forkJoin([]), [])
for (const input of [null, undefined]) {
  let reason: unknown
  forkJoin(input).subscribe({ error: err => { reason = err } })
  assert.ok(reason instanceof TypeError)
}
console.log('null 和 undefined 虽然能通过重载检查，订阅时都收到 TypeError')
check('旧筛选投影', combineLatest(history, labels, (h,t) => h+t), ['H2A','H2B'])
check('数组筛选投影', combineLatest([history, labels], (h,t) => h+t), ['H2A','H2B'])
check('空的一路使汇总无值结束', forkJoin([EMPTY, NEVER]), [])
for (const source of [combineLatest([EMPTY, NEVER]), forkJoin([history, NEVER])]) {
  const sub = source.subscribe({ next: () => assert.fail('意外值'), complete: () => assert.fail('意外完成') })
  assert.equal(sub.closed, false)
  sub.unsubscribe()
}
check('竞争遇到同步空完成', race(EMPTY, history), [])
for (const source of [race([history, labels]), history.pipe(raceWith(labels))]) {
  check('首路胜出，继续读胜者', source, ['H1', 'H2'])
}
check('按 ID 比对回放', of({ id: 1 }).pipe(sequenceEqual([{ id: 1 }], (a,b) => a.id === b.id)), [true])

const replay = new Subject<string>()
const live = new Subject<string>()
const observe = (source: Observable<string>) => {
  const rows: string[] = []
  source.subscribe(v => rows.push(v))
  return rows
}
const serial = [concat(replay, live), replay.pipe(concatWith(live))].map(observe)
const concurrent = [merge(replay, live), replay.pipe(mergeWith(live))].map(observe)
replay.next('H1'); live.next('L1'); replay.complete(); live.next('L2'); live.complete()
for (const rows of serial) assert.deepEqual(rows, ['H1', 'L2'])
for (const rows of concurrent) assert.deepEqual(rows, ['H1', 'L1', 'L2'])
console.log('历史加载期间的热消息：串接 / 并接', serial[0], concurrent[0])

const logs = new Subject<string>()
const condition = new BehaviorSubject('A')
const errors = logs.pipe(filter(v => v === 'error'), map(() => 1), scan((a,b) => a+b, 0), startWith(0))
const preview = observe(combineLatest({ errors, condition }).pipe(map(v => v.condition + ':' + v.errors)))
logs.next('info'); logs.next('error')
const clicks = new Subject<void>()
const finish = new Subject<void>()
const jobs = clicks.pipe(withLatestFrom(condition), map(([,term]) => term))
const queued: string[] = [], accepted: string[] = []
jobs.pipe(concatMap(term => { queued.push(term); return finish })).subscribe()
jobs.pipe(exhaustMap(term => { accepted.push(term); return finish })).subscribe()
clicks.next(); condition.next('B'); clicks.next(); condition.next('C'); finish.complete()
assert.deepEqual(preview, ['A:0','A:1','B:1','C:1'])
assert.deepEqual(queued, ['A','B'])
assert.deepEqual(accepted, ['A'])
console.log('视图 / 排队启动 / 忙时忽略', preview, queued, accepted)

const clock = new VirtualTimeScheduler()
const terms = new Subject<string>()
const trace: string[] = []
terms.pipe(debounceTime(3, clock), switchMap(term => {
  trace.push('查' + term + '@' + clock.now())
  return timer(10, clock).pipe(map(() => term), finalize(() => trace.push('清理' + term + '@' + clock.now())))
})).subscribe(term => trace.push('显示' + term + '@' + clock.now()))
for (const [frame, term] of [[0,'A'], [5,'B'], [6,'C']] as const) clock.schedule(() => terms.next(term), frame)
clock.flush()
assert.deepEqual(trace, ['查A@3','清理A@9','查C@9','显示C@19','清理C@19'])
console.log('慢查询的生存期', trace)

type Inputs = [Observable<number>, Promise<string>]
type Pair = ObservedValueTupleFromArray<Inputs>
type Union = ObservedValueUnionFromArray<Inputs>
type Equal<A,B> = (<T>()=>T extends A ? 1 : 2) extends (<T>()=>T extends B ? 1 : 2) ? true : false
type Expect<T extends true> = T
type Contracts = [
  Expect<Equal<ObservedValueOf<Inputs[0]>, number>>, Expect<Equal<Pair,[number,string]>>,
  Expect<Equal<ValueFromArray<Pair>,Union>>, Expect<Equal<Union,number|string>>,
  Expect<Equal<ObservedValuesFromArray<Inputs>,Union>>, Expect<Equal<Cons<number,[string]>,Pair>>,
  Expect<Equal<Head<Pair>,number>>, Expect<Equal<Tail<Pair>,[string]>>
]
const inputs: ObservableInputTuple<Pair> = [of(1), Promise.resolve('A')]
// @ts-expect-error 第二路必须产出 string
const swapped: ObservableInputTuple<Pair> = [of(1), of(false)]
// @ts-expect-error 联合不能保证固定位置
type Wrong = Expect<Equal<Union[], Pair>>

import { mergeMap } from 'rxjs'

// 两个同时挂好的导出入口。切任务后、结果到达前的那一次点击是分界点。
{
  const results = { A: new Subject<string>(), B: new Subject<string>() }
  const task = new Subject<keyof typeof results>()
  const exportClick = new Subject<void>()
  const lastReply = merge(results.A, results.B)
  const mixed = combineLatest([task, lastReply]).pipe(map(([key, row]) => key + '/' + row))
  const phases = task.pipe(switchMap(key =>
    results[key].pipe(
      map(row => ({ key, row })),
      startWith({ key, row: null })
    )
  ))
  const wrong = observe(exportClick.pipe(withLatestFrom(mixed), map(([, value]) => value)))
  const valid = observe(exportClick.pipe(withLatestFrom(phases),
    filter(([, phase]) => phase.row !== null), map(([, phase]) => phase.key + '/' + phase.row)))
  task.next('A')
  results.A.next('a1')
  task.next('B')
  exportClick.next()
  assert.deepEqual(wrong, ['B/a1'])
  assert.deepEqual(valid, [])
  results.A.next('a2')
  results.B.next('b1')
  assert.deepEqual(valid, [])
  exportClick.next()
  assert.deepEqual(valid, ['B/b1'])
}

{
  const ids = [11]
  const selected = new BehaviorSubject(ids)
  const click = new Subject<void>()
  const referenced: number[][] = [], copied: number[][] = []
  click.pipe(withLatestFrom(selected)).subscribe(([, current]) => {
    referenced.push(current)
    copied.push([...current])
  })
  click.next()
  ids.push(12)
  selected.next(ids)
  assert.deepEqual(referenced, [[11,12]])
  assert.deepEqual(copied, [[11]])
}

{
  const liveRows = new Subject<number>(), replayRows = new Subject<number>()
  const seen = new Set<number>()
  const accepted: number[] = []
  merge(liveRows, replayRows).pipe(filter(id => !seen.has(id))).subscribe(id => {
    seen.add(id)
    accepted.push(id)
  })
  liveRows.next(42)
  assert.deepEqual(accepted, [42])
  replayRows.next(41)
  replayRows.next(42)
  assert.deepEqual(accepted, [42,41])
}
{
  const arrivals = new Subject<11 | 12>()
  const replies = { 11: new Subject<string>(), 12: new Subject<string>() }
  const byPosition = observe(zip(arrivals, merge(replies[11], replies[12])).pipe(
    map(([id, verdict]) => id + verdict)))
  const byRequest = observe(arrivals.pipe(mergeMap(id =>
    replies[id].pipe(map(verdict => id + verdict)))))
  arrivals.next(11)
  arrivals.next(12)
  replies[12].next('通过')
  replies[11].next('拒绝')
  assert.deepEqual(byPosition, ['11通过','12拒绝'])
  assert.deepEqual(byRequest, ['12通过','11拒绝'])
}
console.log('跨任务点击、可变选择、历史接缝与乱序回执均已核对')
