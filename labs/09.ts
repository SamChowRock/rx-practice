import assert from 'node:assert/strict'
import {
  BehaviorSubject, Observable, Subject, VirtualTimeScheduler, buffer, bufferCount,
  bufferTime, bufferToggle, bufferWhen, combineLatest, concatMap, groupBy, map,
  mergeMap, of, scan, timer, toArray, window, windowCount, windowTime,
  windowToggle, windowWhen, withLatestFrom
} from 'rxjs'
import type { BasicGroupByOptions, GroupByOptionsWithElement, GroupedObservable } from 'rxjs'

function collect<T>(source: Observable<T>) {
  const rows: T[] = []
  source.subscribe(v => rows.push(v))
  return rows
}
const lines = new Subject<number>()
const cut = new Subject<void>(), open = new Subject<string>()
const batches = collect(lines.pipe(buffer(cut)))
const delivered: number[] = []
lines.pipe(window(cut), mergeMap(w => w)).subscribe(n => delivered.push(n))
const after = collect(lines.pipe(bufferWhen(() => cut)))
const windows = collect(lines.pipe(windowWhen(() => cut), mergeMap(w => w.pipe(toArray()))))
const incident = collect(lines.pipe(bufferToggle(open, () => cut)))
const alarm = collect(lines.pipe(windowToggle(open, () => cut), mergeMap(w => w)))
lines.next(1)
assert.deepEqual(batches, [])
assert.deepEqual(delivered, [1])
open.next('警报'); lines.next(2)
assert.deepEqual(alarm, [2]); assert.deepEqual(incident, [])
cut.next(); lines.next(3); lines.complete()
for (const rows of [batches, after, windows]) assert.deepEqual(rows, [[1,2],[3]])
assert.deepEqual(incident, [[2]])
console.log('边界结批', batches, '警报明细', alarm, '警报关闭交付', incident)
const series = of(1,2,3,4)
const sliding = collect(series.pipe(bufferCount(3,2)))
const framed = collect(series.pipe(windowCount(3,2), mergeMap(w => w.pipe(toArray()))))
assert.deepEqual(sliding, [[1,2,3],[3,4]])
assert.deepEqual(framed, [...sliding, []])
console.log('三个一批，每两条开一批；末尾空窗口', sliding, framed)

const clock = new VirtualTimeScheduler()
const timed = new Subject<number>()
const periodic = collect(timed.pipe(bufferTime(5, clock)))
const timedWindows = collect(timed.pipe(windowTime(5, clock), mergeMap(w => w.pipe(toArray()))))
for (const n of [1,2,6]) clock.schedule(() => timed.next(n), n)
clock.schedule(() => timed.complete(), 7)
clock.flush()
assert.deepEqual(periodic, [[1,2],[6]])
assert.deepEqual(timedWindows, periodic)
console.log('每五刻交付，末批在源完成时交付', periodic)

// 同一个上限。连续开批时容量关闭会补开，固定时刻开批则可能漏掉中间行。
function capped(interval: number | null) {
  const time = new VirtualTimeScheduler()
  const input = new Subject<number>()
  const batches = collect(input.pipe(bufferTime(10, interval, 2, time)))
  const windows = collect(input.pipe(windowTime(10, interval, 2, time), mergeMap(w => w.pipe(toArray()))))
  for (const n of [1,2,3,6]) time.schedule(() => input.next(n), n)
  time.schedule(() => input.complete(), 7)
  time.flush()
  assert.deepEqual(windows, batches)
  return batches
}
assert.deepEqual(capped(null), [[1,2],[3,6],[]])
assert.deepEqual(capped(5), [[1,2],[6]])
console.log('满两条提前关闭：固定每五刻才重开时，第 3 刻的行落在空档')

type Line = { task: string; at: number }
const stop = new Subject<void>()
const plain: BasicGroupByOptions<string,Line> = { duration: () => stop }
const options: GroupByOptionsWithElement<string,number,Line> = {
  element: row => row.at, duration: () => stop, connector: () => new Subject<number>()
}
const stream = new Subject<Line>()
const counts = stream.pipe(groupBy(row => row.task, options), mergeMap(group =>
  group.pipe(scan((s, at) => ({ at, n: s.n + 1 }), { at: 0, n: 0 }),
    map(s => group.key + ':' + s.n + '@' + s.at))))
const stats = collect(counts)
const old = collect(stream.pipe(groupBy(v => v.task, options.element, () => stop),
  mergeMap(g => g.pipe(toArray()))))
const modern = collect(stream.pipe(groupBy(v => v.task, options), mergeMap(g => g.pipe(toArray()))))
stream.next({task:'A',at:1}); stream.next({task:'B',at:2}); stream.next({task:'A',at:3})
stop.next(); stream.next({task:'A',at:4}); stream.complete()
assert.deepEqual(stats, ['A:1@1','B:1@2','A:2@3','A:1@4'])
assert.deepEqual(old, [[1,3],[2],[4]]); assert.deepEqual(modern, old)
console.log('并行计数及重开；迁移前后分组', stats, modern)

const writeClock = new VirtualTimeScheduler()
const chunks = series.pipe(bufferCount(2))
const write = (chunk: number[]) => timer(chunk[0] === 1 ? 5 : 1, writeClock).pipe(map(() => chunk))
const ordered = collect(chunks.pipe(concatMap(write)))
const unordered = collect(chunks.pipe(mergeMap(write)))
writeClock.flush()
assert.deepEqual(ordered, [[1,2],[3,4]])
assert.deepEqual(unordered, [[3,4],[1,2]])
console.log('慢首批：串行落盘 / 并行落盘', ordered, unordered)

const clear = new Subject<void>(), task = new BehaviorSubject('A')
clear.next(); task.next('B')
assert.deepEqual(collect(clear), [])
assert.deepEqual(collect(task), ['B'])
function drive(early: boolean) {
  const source = new Subject<Line>(), flush = new Subject<string>()
  const totals = source.pipe(groupBy(v => v.task), mergeMap(g => g.pipe(
    scan((s,line) => ({ at:line.at, n:s.n+1 }), { at:0, n:0 }),
    map(s => g.key + ':' + s.n + '@' + s.at))))
  const view = collect(combineLatest([flush, totals]))
  const writes = collect(flush.pipe(withLatestFrom(totals)))
  source.next({task:'A',at:1})
  if (early) flush.next('落盘')
  source.next({task:'A',at:2})
  if (!early) flush.next('落盘')
  return { view, writes }
}
const early = drive(true), late = drive(false)
assert.deepEqual(early.view, [['落盘','A:1@1'],['落盘','A:2@2']])
assert.deepEqual(early.writes, [['落盘','A:1@1']])
assert.deepEqual(late.view, [['落盘','A:2@2']]); assert.deepEqual(late.writes, late.view)
console.log('写信号提前 / 延后', early, late)

// 以下契约只在编译时检查
function keyContract(group: GroupedObservable<string,number>) {
  const key: string = group.key
  // @ts-expect-error 分组键只读
  group.key = key
}
// @ts-expect-error 基础选项保留原始行
const projecting: BasicGroupByOptions<string,Line> = { element: (v: Line) => v.at }
// @ts-expect-error 投影后的组要求数值元素
const mismatch: GroupByOptionsWithElement<string,number,Line> = { element: v => v.task }

import { filter, takeUntil } from 'rxjs'

{
  const time = new VirtualTimeScheduler()
  const rows = new Subject<number>(), stop = new Subject<void>()
  const discarded = collect(rows.pipe(bufferCount(3), takeUntil(stop)))
  const confirmed: [number, number[]][] = []
  let completedAt: number | undefined
  rows.pipe(takeUntil(stop), bufferCount(3), concatMap(batch =>
    timer(batch.length === 3 ? 5 : 1, time).pipe(map(() => batch))
  )).subscribe({
    next: batch => confirmed.push([time.now(), batch]),
    complete: () => { completedAt = time.now() }
  })
  for (const n of [1,2,3,4,5]) time.schedule(() => rows.next(n), n)
  time.schedule(() => rows.next(6), 7)
  time.schedule(() => {
    stop.next()
    assert.deepEqual(confirmed, [])
    assert.equal(completedAt, undefined)
  }, 6)
  time.flush()
  assert.deepEqual(discarded, [[1,2,3]])
  assert.deepEqual(confirmed, [[8,[1,2,3]],[9,[4,5]]])
  assert.equal(completedAt, 9)
}

{
  const rows = new Subject<number>(), start = new Subject<string>(), end = new Subject<string>()
  const allClose = collect(rows.pipe(bufferToggle(start, () => end)))
  const byAlarm = collect(rows.pipe(bufferToggle(start, id => end.pipe(filter(done => done === id)))))
  start.next('甲')
  rows.next(2)
  start.next('乙')
  rows.next(4)
  end.next('甲')
  assert.deepEqual(allClose, [[2,4],[4]])
  assert.deepEqual(byAlarm, [[2,4]])
  rows.next(6)
  end.next('乙')
  assert.deepEqual(byAlarm, [[2,4],[4,6]])
}

{
  const time = new VirtualTimeScheduler()
  const rows = new Subject<number>()
  const write = (batch: number[]) => timer(5, time).pipe(map(() => batch))
  const queuedWindows = collect(rows.pipe(windowCount(2),
    concatMap(w => w.pipe(toArray(), concatMap(write)))))
  const queuedBatches = collect(rows.pipe(bufferCount(2), concatMap(write)))
  for (const n of [1,2,3,4]) time.schedule(() => rows.next(n), n)
  time.schedule(() => rows.complete(), 5)
  time.flush()
  assert.deepEqual(queuedWindows, [[1,2],[],[]])
  assert.deepEqual(queuedBatches, [[1,2],[3,4]])
}
{
  const time = new VirtualTimeScheduler()
  const happenedAt = new Subject<number>()
  const received = collect(happenedAt.pipe(bufferTime(5, time)))
  const eventBuckets = collect(happenedAt.pipe(
    groupBy(at => Math.floor(at / 5)), mergeMap(g => g.pipe(toArray()))))
  time.schedule(() => happenedAt.next(1), 2)
  time.schedule(() => {
    happenedAt.next(3)
    happenedAt.next(6)
  }, 7)
  time.schedule(() => happenedAt.complete(), 11)
  time.flush()
  assert.deepEqual(received, [[1],[3,6],[]])
  assert.deepEqual(eventBuckets, [[1,3],[6]])
}
console.log('写入确认、独立解除、热窗口排队与迟到行的归属均已核对')
