import assert from 'node:assert/strict'
import {
  Observable, distinct, distinctUntilChanged, distinctUntilKeyChanged, filter, from,
  fromEvent, identity, map, noop, pairwise, partition, pipe, tap,
  type Falsy, type MonoTypeOperatorFunction, type OperatorFunction,
  type TapObserver, type TruthyTypesOf, type UnaryFunction,
} from 'rxjs'
import * as operators from 'rxjs/operators'

for (const [a, b] of [[map, operators.map], [filter, operators.filter], [tap, operators.tap],
  [distinct, operators.distinct], [distinctUntilChanged, operators.distinctUntilChanged],
  [distinctUntilKeyChanged, operators.distinctUntilKeyChanged], [pairwise, operators.pairwise]]) {
  assert.equal(a, b)
}
// All callers of collect supply synchronous finite fixtures.
function collect<T>(source: Observable<T>): T[] {
  const values: T[] = []
  const handle = source.subscribe(value => values.push(value))
  assert.equal(handle.closed, true)
  return values
}
type Row = { kind: 'row'; text: string }
type Frame = Row | { kind: 'ping' }
const parse: UnaryFunction<string, Frame> = raw => {
  if (raw === 'ping') return { kind: 'ping' }
  if (!raw.startsWith('row:')) throw Error('bad frame: ' + raw)
  return { kind: 'row', text: raw.slice(4) }
}
const isRow = (frame: Frame): frame is Row => frame.kind === 'row'
const parsed: OperatorFunction<string, Row> = pipe(map(parse), filter(isRow))
const adjacent: MonoTypeOperatorFunction<Row> = distinctUntilKeyChanged('text')
const frames = ['row:A', 'row:A', 'ping', 'row:B', 'row:A']
const source = from(frames).pipe(parsed)
const texts = (rows: Observable<Row>) => collect(rows.pipe(map(r => r.text)))
assert.deepEqual(texts(source.pipe(distinct(r => r.text))), ['A', 'B'])
assert.deepEqual(texts(source.pipe(adjacent)), ['A', 'B', 'A'])
assert.deepEqual(texts(source.pipe(distinctUntilChanged())), ['A', 'A', 'B', 'A'])
const same = (a: string, b: string) => a.toLowerCase() === b.toLowerCase()
const mixed = from(['row:A', 'row:a', 'row:b']).pipe(parsed)
assert.deepEqual(texts(mixed.pipe(distinctUntilChanged(same, r => r.text))), ['A', 'b'])
assert.deepEqual(texts(mixed.pipe(distinctUntilKeyChanged('text', same))), ['A', 'b'])
assert.deepEqual(collect(from(['A', 'A', 'B', 'A']).pipe(pairwise())),
  [['A', 'A'], ['A', 'B'], ['B', 'A']])
const clock = new EventTarget()
const duringRefresh = new Observable<string>(sink => {
  sink.next('A'); sink.next('A')
  clock.dispatchEvent(new Event('reset'))
  sink.next('A'); sink.complete()
})
const reset = fromEvent(clock, 'reset')
assert.deepEqual(collect(duringRefresh.pipe(distinct(identity, reset))), ['A'])
const afterReset: string[] = []
const live = fromEvent<CustomEvent<string>>(clock, 'line').pipe(map(e => e.detail), distinct(identity, reset))
const listening = live.subscribe(s => afterReset.push(s))
const emitA = () => clock.dispatchEvent(new CustomEvent('line', { detail: 'A' }))
emitA(); emitA()
clock.dispatchEvent(new Event('reset'))
emitA()
listening.unsubscribe()
assert.deepEqual(afterReset, ['A', 'A'])
console.log('全局 A B；相邻 A B A；新对象按引用比较 A A B A；重置后 A 再次通过')

const button = new EventTarget()
const click = (raw: string) => button.dispatchEvent(new CustomEvent('preview', { detail: raw }))
const previews = fromEvent<CustomEvent<string>>(button, 'preview').pipe(map(e => e.detail), parsed)
const early: string[] = [], late: string[] = []
const first = previews.subscribe(row => early.push(row.text))
click('row:A')
const second = previews.subscribe(row => late.push(row.text))
click('ping'); click('row:B')
first.unsubscribe(); second.unsubscribe()
assert.deepEqual(early, ['A', 'B'])
assert.deepEqual(late, ['B'])
for (let i = 0; i < 2; i++) assert.deepEqual(texts(source), ['A', 'A', 'B', 'A'])
console.log('同一解析函数：历史两次都回放全部行；按钮先开得 A B，晚开得 B')

let runs = 0
const counted = new Observable<Frame>(sink => {
  runs++
  return from(frames).pipe(map(parse)).subscribe(sink)
})
const [rows, pings] = partition(counted, isRow)
assert.deepEqual(texts(rows), ['A', 'A', 'B', 'A'])
assert.deepEqual(collect(pings), [{ kind: 'ping' }])
assert.equal(runs, 2)
const trace: string[] = []
const probe: TapObserver<string> = {
  subscribe: () => trace.push('open'), next: s => trace.push(s),
  complete: () => trace.push('end'), error: () => trace.push('error'),
  unsubscribe: () => trace.push('cancel'), finalize: () => trace.push('release'),
}
const sameProbe: operators.TapObserver<string> = probe
assert.deepEqual(collect(from(['A']).pipe(tap(sameProbe))), ['A'])
new Observable<string>(() => {}).pipe(tap(probe)).subscribe().unsubscribe()
from(['bad']).pipe(map(parse), map(identity), tap({ ...probe, next: noop })).subscribe({ error: noop })
assert.deepEqual(trace, ['open', 'A', 'end', 'release', 'open', 'cancel', 'release',
  'open', 'error', 'release'])
console.log('分流订阅使源启动两次；诊断钩子顺序', trace)

const sample = from([0, 1, 2])
const cfg = { min: 1 }
const old = sample.pipe(filter(function (this: typeof cfg, n) { return n >= this.min }, cfg),
  map(function (this: typeof cfg, n) { return n + this.min }, cfg))
assert.deepEqual(collect(old), collect(sample.pipe(filter(n => n >= cfg.min), map(n => n + cfg.min))))
const [pass, fail] = partition([0, 1], function (this: typeof cfg, n) { return n >= this.min }, cfg)
const branches = partition([0, 1], n => n >= cfg.min)
assert.deepEqual([collect(pass), collect(fail)], branches.map(s => collect(s)))
const oldTap: string[] = [], newTap: string[] = []
collect(sample.pipe(tap(n => oldTap.push(String(n)), noop, () => oldTap.push('end'))))
collect(sample.pipe(tap({ next: n => newTap.push(String(n)), complete: () => newTap.push('end') })))
assert.deepEqual(oldTap, newTap)
assert.deepEqual(collect(from([null, '', 0, 'ok'] as const).pipe(filter(Boolean))), ['ok'])
assert.deepEqual(collect(sample.pipe(filter((_n, i) => i % 2 === 0), map((n, i) => [i, n]))),
  [[0, 0], [1, 2]])
assert.equal(pipe()(42), identity(42))
assert.equal(noop(), undefined)
function typeExamples(value: Falsy, text: TruthyTypesOf<false | 'ok'>) {
  const empty: Falsy = 0n
  // @ts-expect-error 1 is truthy
  const nonempty: Falsy = 1
  // @ts-expect-error false was excluded from this union
  const excluded: typeof text = false
  // @ts-expect-error parsed changes the element type
  const wrong: MonoTypeOperatorFunction<string> = parsed
  // @ts-expect-error A full TapObserver includes lifecycle hooks.
  const incomplete: TapObserver<string> = { next: noop, error: noop, complete: noop }
  // @ts-expect-error The output of parsed is a Row, not a number.
  const badOperator: OperatorFunction<string, number> = parsed
  // @ts-expect-error parse accepts text
  parse(17)
  return [empty, value, nonempty, excluded, wrong, incomplete, badOperator]
}
console.log('闭包与旧 thisArg 输出一致；保留原始序号 0、2 后，下游重新编号 0、1')

const progress = new Observable<{ phase: string; blocks: number }>(sink => {
  const card = { phase: '等待', blocks: 2 }
  sink.next(card)
  card.phase = '下载'
  card.blocks = 5
  sink.next(card)
  card.phase = '完成'
  sink.next(card)
  sink.complete()
})
assert.deepEqual(collect(progress.pipe(distinctUntilKeyChanged('phase'), map(c => c.phase))), ['等待'])
assert.deepEqual(collect(progress.pipe(distinctUntilChanged((prev, cur) => prev === cur, c => c.phase), map(c => c.phase))),
  ['等待', '下载', '完成'])
assert.deepEqual(collect(progress.pipe(pairwise(), map(([a, b]) => b.blocks - a.blocks))), [0, 0])
assert.deepEqual(collect(progress.pipe(map(c => c.blocks), pairwise(), map(([a, b]) => b - a))), [3, 0])

type Phase = { task: string; phase: string }
const activity = from<Phase[]>([
  { task: '甲', phase: '等待' }, { task: '乙', phase: '等待' },
  { task: '乙', phase: '运行' }, { task: '甲', phase: '等待' },
  { task: '甲', phase: '运行' }, { task: '甲', phase: '等待' },
])
const title = ({ task, phase }: Phase) => task + phase
assert.deepEqual(collect(activity.pipe(distinctUntilKeyChanged('phase'), map(title))),
  ['甲等待', '乙运行', '甲等待', '甲运行', '甲等待'])
const changes = new Observable<string>(sink => {
  const lastByTask = new Map<string, string>()
  return activity.pipe(filter(({ task, phase }) => {
    if (lastByTask.get(task) === phase) return false
    lastByTask.set(task, phase)
    return true
  }), map(title)).subscribe(sink)
})
for (let mount = 0; mount < 2; mount++) {
  assert.deepEqual(collect(changes), ['甲等待', '乙等待', '乙运行', '甲运行', '甲等待'])
}

const incoming = from(['ping', 'row:A', 'oops', 'row:B'])
const strictTrace: string[] = []
incoming.pipe(parsed).subscribe({
  next: row => strictTrace.push(row.text),
  error: (err: Error) => strictTrace.push(err.message),
})
assert.deepEqual(strictTrace, ['A', 'bad frame: oops'])
const defects: { raw: string; index: number }[] = []
const diagnostic = incoming.pipe(
  map((raw, index) => raw === 'ping' || raw.startsWith('row:')
    ? parse(raw) : { kind: 'invalid' as const, raw, index }),
  tap(frame => {
    if (frame.kind === 'invalid') defects.push({ raw: frame.raw, index: frame.index })
  }),
  filter((frame): frame is Row => frame.kind === 'row'),
  map(row => row.text),
)
assert.deepEqual(collect(diagnostic), ['A', 'B'])
assert.deepEqual(defects, [{ raw: 'oops', index: 2 }])
console.log('固定读数得到增量 3、0；甲乙各记上次阶段；诊断保留原始位置 2 后继续显示 B')
