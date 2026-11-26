import assert from 'node:assert/strict';
import * as rx from 'rxjs';
import * as old from 'rxjs/operators';
import { TestScheduler } from 'rxjs/testing';
import type { Observable, Operator } from 'rxjs';

const collect = <T>(s: Observable<T>) => rx.lastValueFrom(s.pipe(rx.toArray()));
const numbers = rx.of(1, 2), tag = rx.of('ok');
const input: { stats?: { errors: number } }[] = [{ stats: { errors: 0 } }, {}, { stats: { errors: 2 } }];
const payloads = rx.from(input);
const projected = payloads.pipe(rx.map(x => x.stats?.errors), rx.filter((x): x is number => x !== undefined));
assert.deepEqual(await collect(projected), [0, 2]);
assert.deepEqual(await collect(payloads.pipe(old.pluck('stats', 'errors'))), [0, undefined, 2]);
console.log('缺失字段和零分开处理：迁移后计数为 [0, 2]。');
for (const [legacy, modern] of [[rx.animationFrame, rx.animationFrameScheduler], [rx.asap, rx.asapScheduler],
  [rx.async, rx.asyncScheduler], [rx.queue, rx.queueScheduler]]) assert.equal(legacy, modern);
assert.deepEqual(await collect(rx.empty()), await collect(rx.EMPTY));
assert.equal(rx.never(), rx.NEVER);
assert.deepEqual(await collect(rx.pairs({ a: 1 })), await collect(rx.from(Object.entries({ a: 1 }))));
const pairs: [string, Observable<unknown>, Observable<unknown>][] = [
  ['固定值', numbers.pipe(old.mapTo('ok')), numbers.pipe(rx.map(() => 'ok'))],
  ['排队', numbers.pipe(old.concatMapTo(tag)), numbers.pipe(rx.concatMap(() => tag))],
  ['并发', numbers.pipe(old.mergeMapTo(tag, 2)), numbers.pipe(rx.mergeMap(() => tag, 2))],
  ['切换', numbers.pipe(old.switchMapTo(tag)), numbers.pipe(rx.switchMap(() => tag))],
  ['并发投影', numbers.pipe(old.flatMap(x => rx.of(x * 2))), numbers.pipe(rx.mergeMap(x => rx.of(x * 2)))],
  ['忙时跳过', rx.of(tag, tag).pipe(old.exhaust()), rx.of(tag, tag).pipe(rx.exhaustAll())],
  ['等待外层完成', rx.of(numbers, tag).pipe(old.combineAll<number | string>()), rx.of(numbers, tag).pipe(rx.combineLatestAll<number | string>())],
  ['组合最新值', numbers.pipe(old.combineLatest(tag)), numbers.pipe(rx.combineLatestWith(tag))],
  ['串接', numbers.pipe(old.concat(tag)), numbers.pipe(rx.concatWith(tag))],
  ['合流', numbers.pipe(old.merge(tag)), numbers.pipe(rx.mergeWith(tag))],
  ['竞速', numbers.pipe(old.race(tag)), numbers.pipe(rx.raceWith(tag))],
  ['配对', numbers.pipe(old.zip(tag)), numbers.pipe(rx.zipWith(tag))],
  ['跳过错误', numbers.pipe(old.onErrorResumeNext(rx.throwError(() => 1), tag)), rx.onErrorResumeNext([numbers, rx.throwError(() => 1), tag])],
  ['完成后再读', numbers.pipe(old.repeatWhen(done => done.pipe(rx.take(1), rx.delay(1)))), numbers.pipe(rx.repeat({ count: 2, delay: 1 }))],
];
for (const [name, before, after] of pairs) {
  const values = await collect(after);
  assert.deepEqual(await collect(before), values, name);
  console.log(name, '两版输出', values);
}
const [even, odd] = old.partition<number>(n => n % 2 === 0)(numbers);
const [newEven, newOdd] = rx.partition(numbers, n => n % 2 === 0);
assert.deepEqual(await Promise.all([collect(even), collect(odd)]), await Promise.all([collect(newEven), collect(newOdd)]));
const forward: Operator<number, number> = { call(sub, source: Observable<number>) { return source.subscribe(sub); } };
assert.deepEqual(await collect(numbers.lift(forward)), [1, 2]);
// @ts-expect-error an old-style call must return teardown logic
const bad: Operator<number, number> = { call: () => 42 };

for (const kind of ['publish', 'behavior', 'last', 'replay'] as const) {
  function probe(legacy: boolean) {
    const wire = new rx.Subject<number>();
    const oldSource = kind === 'publish' ? old.publish<number>()(wire) :
      kind === 'last' ? old.publishLast<number>()(wire) :
      kind === 'behavior' ? old.publishBehavior(0)(wire) :
      old.multicast(new rx.ReplaySubject<number>(1))(wire);
    const socket = legacy ? oldSource : rx.connectable(wire, {
      connector: () => kind === 'publish' ? new rx.Subject<number>() :
        kind === 'behavior' ? new rx.BehaviorSubject(0) :
        kind === 'last' ? new rx.AsyncSubject<number>() : new rx.ReplaySubject<number>(1),
      resetOnDisconnect: false
    });
    const early: number[] = [], late: number[] = [];
    socket.subscribe(v => early.push(v));
    const connection = socket.connect();
    wire.next(1);
    socket.subscribe(v => late.push(v));
    const beforeCompletion = late.slice();
    wire.next(2);
    wire.complete();
    connection.unsubscribe();
    return { early, beforeCompletion, late };
  }
  const result = probe(true);
  assert.deepEqual(result, probe(false));
  console.log(kind, '手动连接两版一致', result);
}

type End = 'complete' | 'error' | 'leave';
function observe(build: (s: Observable<number>) => Observable<number>, end: End) {
  const wire = new rx.Subject<number>();
  let opens = 0, closes = 0;
  const view = build(rx.defer(() => { opens++; return wire.pipe(rx.finalize(() => closes++)); }));
  const first = view.subscribe({ error: () => {} });
  wire.next(1);
  if (end === 'leave') first.unsubscribe();
  else if (end === 'error') wire.error('E');
  else wire.complete();
  const late: unknown[] = [];
  const second = view.subscribe({ next: v => late.push(v), error: e => late.push(e), complete: () => late.push('C') });
  second.unsubscribe();
  return { opens, closes, late };
}
for (const end of ['complete', 'error', 'leave'] as const) {
  const legacy = observe(s => s.pipe(old.publishReplay(1), old.refCount()), end);
  const migrated = observe(s => {
    const cache = new rx.ReplaySubject<number>(1);
    return s.pipe(rx.share({ connector: () => cache, resetOnRefCountZero: true, resetOnError: false, resetOnComplete: false }));
  }, end);
  assert.deepEqual(migrated, legacy);
  const replay = observe(s => s.pipe(rx.shareReplay({ bufferSize: 1, refCount: true })), end);
  assert.deepEqual(replay, end === 'complete' ? legacy : { opens: 2, closes: 2, late: end === 'error' ? ['E'] : [] });
  console.log(end, '旧版与保留语义的 share', migrated, 'shareReplay', replay);
}
for (const legacy of [true, false]) {
  new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ cold, expectSubscriptions, expectObservable }) => {
    const source = cold('-----x|');
    const failure = () => rx.throwError(() => '超时');
    const out = legacy ? source.pipe(old.timeoutWith(2, failure()), old.retryWhen(errors => errors.pipe(
      rx.concatMap((err, index) => index === 0 ? rx.timer(3) : rx.throwError(() => err))
    ))) : source.pipe(rx.timeout({ each: 2, with: failure }), rx.retry({ count: 1, delay: 3 }));
    expectObservable(out.pipe(rx.catchError(err => rx.of(err)))).toBe('-------(f|)', { f: '超时' });
    expectSubscriptions(source.subscriptions).toBe(['^-!', '-----^-!']);
  });
}
console.log('两版均在帧 2 超时、帧 5 重试、帧 7 最终降级。');

const virtual = () => new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
for (const mode of ['legacy', 'unlimited', 'serial'] as const) {
  virtual().run(({ cold, expectObservable, expectSubscriptions }) => {
    const archive = cold('---a|');
    const tail = cold('-b|');
    const rows = mode === 'legacy' ? archive.pipe(old.merge(tail, 1))
      : mode === 'serial' ? archive.pipe(rx.concatWith(tail)) : archive.pipe(rx.mergeWith(tail));
    expectObservable(rows).toBe(mode === 'unlimited' ? '-b-a|' : '---a-b|');
    expectSubscriptions(archive.subscriptions).toBe('^---!');
    expectSubscriptions(tail.subscriptions).toBe(mode === 'unlimited' ? '^-!' : '----^-!');
  });
}

virtual().run(({ cold, expectObservable, expectSubscriptions }) => {
  const archive = cold('---a|');
  const tail = cold('-b|');
  expectObservable(archive.pipe(rx.concatWith(tail)), '^-!').toBe('');
  expectSubscriptions(archive.subscriptions).toBe('^-!');
  expectSubscriptions(tail.subscriptions).toBe([]);
});

for (const mode of ['legacy', 'share', 'connect', 'replay'] as const) {
  let reads = 0;
  const archive = rx.defer(() => {
    reads++;
    return rx.of(reads + '.1', reads + '.2');
  });
  const pair = (s: Observable<string>) => rx.zip(s, s);
  const summary = mode === 'legacy' ? archive.pipe(old.publish(pair))
    : mode === 'connect' ? archive.pipe(rx.connect(pair))
    : pair(mode === 'share' ? archive.pipe(rx.share()) : archive.pipe(rx.shareReplay(1)));
  const rows = await collect(summary);
  if (mode === 'replay') assert.deepEqual(rows, [['1.1', '1.2']]);
  else if (mode === 'share') assert.deepEqual(rows, [['1.1', '2.1'], ['1.2', '2.2']]);
  else assert.deepEqual(rows, [['1.1', '1.1'], ['1.2', '1.2']]);
  assert.equal(reads, mode === 'share' ? 2 : 1);
  if (mode === 'legacy' || mode === 'connect') {
    assert.deepEqual(await collect(summary), [['2.1', '2.1'], ['2.2', '2.2']]);
    assert.equal(reads, 2);
  }
}

for (const mode of ['legacy', 'rounds', 'rows'] as const) {
  virtual().run(({ cold, expectObservable, expectSubscriptions }) => {
    const read = cold('--a|');
    const result = mode === 'legacy'
      ? read.pipe(old.repeatWhen(done => done.pipe(rx.take(1), rx.delay(1))))
      : mode === 'rounds' ? read.pipe(rx.repeat({ count: 2, delay: 1 }))
      : read.pipe(rx.repeat({ delay: 1 }), rx.take(2));
    expectObservable(result).toBe(mode === 'rows' ? '--a---(a|)' : '--a---a|');
    expectSubscriptions(read.subscriptions).toBe(['^--!', mode === 'rows' ? '----^-!' : '----^--!']);
  });
}

for (const mode of ['legacy', 'preserve', 'reset'] as const) {
  const clock = virtual();
  clock.run(({ cold, expectObservable, expectSubscriptions }) => {
    const archive = cold<string>('-r|');
    const cache = new rx.ReplaySubject<string>(1, 4, clock);
    const view = mode === 'legacy' ? archive.pipe(old.publishReplay<string>(1, 4, clock), old.refCount())
      : archive.pipe(rx.share({
        connector: () => mode === 'reset' ? new rx.ReplaySubject<string>(1, 4, clock) : cache,
        resetOnComplete: mode === 'reset', resetOnError: false, resetOnRefCountZero: true
      }));
    expectObservable(view).toBe('-r|');
    expectObservable(view, '----^').toBe(mode === 'reset' ? '-----r|' : '----(r|)');
    expectObservable(view, '-----^').toBe(mode === 'reset' ? '-----r|' : '-----|');
    expectSubscriptions(archive.subscriptions).toBe(mode === 'reset' ? ['^-!', '----^-!'] : '^-!');
  });
}
console.log('已核对归档交接、同步配对的读取批号、轮询结束帧与回放过期边界。');

{
  const seconds = (ms: number) => ms / 1000;
  const label = (s: number) => s + ' 秒';
  const durations = rx.of(500, 1200);
  const separate = await collect(durations.pipe(rx.map(seconds), rx.map(label)));
  const fused = await collect(durations.pipe(rx.map(ms => label(seconds(ms)))));
  assert.deepEqual(separate, ['0.5 秒', '1.2 秒']);
  assert.deepEqual(fused, separate);

  const frames = rx.of('ping', 'row:42');
  const isRow = (frame: string) => frame.startsWith('row:');
  const visibleIndices = frames.pipe(rx.filter(isRow), rx.map((_, index) => index));
  const wireIndices = frames.pipe(
    rx.map((frame, index) => ({ frame, index })),
    rx.filter(item => isRow(item.frame)),
    rx.map(item => item.index)
  );
  assert.deepEqual(await collect(visibleIndices), [0]);
  assert.deepEqual(await collect(wireIndices), [1]);

  const decode = (frame: string): unknown => JSON.parse(frame.slice(4));
  assert.deepEqual(await collect(frames.pipe(rx.filter(isRow), rx.map(decode))), [42]);
  await assert.rejects(collect(frames.pipe(
    rx.map(frame => ({ frame, value: decode(frame) })),
    rx.filter(item => isRow(item.frame))
  )), SyntaxError);
  console.log('管道重写：标签相同；行号 0、帧号 1；提前解析在心跳处终止。');
}
