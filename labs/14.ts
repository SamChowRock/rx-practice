import assert from 'node:assert/strict';
import {
  BehaviorSubject, Observable, Subject, Subscription, catchError, combineLatest,
  defer, distinctUntilChanged, filter, from, map, of, retry, scan, shareReplay, tap, timeout, withLatestFrom
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

// at 是相对订阅时刻的毫秒数。重放同一份磁带两次，游标各自独立。
const tape = [{ at: 1, value: '0' }, { at: 3, value: '?' }, { at: 5, value: '2' }];
type Mark = { at: number; attempt: number; kind: 'open' | 'timeout' | 'complete' | 'cancel' }
  | { at: number; attempt: number; kind: 'next'; value: string };
function play(source: Observable<string>, seen: string[]): Subscription {
  return source.subscribe(value => seen.push(value));
}
const immediate: string[] = [];
play(from(tape).pipe(map(row => row.value)), immediate);
assert.deepEqual(immediate, ['0', '?', '2']);
const clock = new TestScheduler((a, b) => assert.deepEqual(a, b));
let deliveries = 0;
const recording = new Observable<string>(sink => {
  const pending = new Subscription();
  for (const row of tape) {
    pending.add(clock.schedule(() => { deliveries++; sink.next(row.value); }, row.at));
  }
  pending.add(clock.schedule(() => sink.complete(), 6));
  return pending;
});
const stopped: string[] = [];
const panel = play(recording, stopped);
clock.schedule(() => panel.unsubscribe(), 4);
clock.flush();
assert.deepEqual(stopped, ['0', '?']);
assert.equal(deliveries, 2);
console.log('同步播放器收到 3 行；定时播放器在第 4 帧停止，生产端只执行了 2 次投递。');

const projected: Array<number | null> = [], valid: number[] = [], totals: number[] = [];
const decoded = from(tape).pipe(map(row => /^\d+$/.test(row.value) ? Number(row.value) : null));
decoded.subscribe(n => projected.push(n));
const deltas = decoded.pipe(filter((n): n is number => n !== null));
const numeric: Observable<number> = deltas;
// @ts-expect-error 尚未筛选的记录允许 null，不能交给仅接受数字的消费者。
const premature: Observable<number> = decoded;
numeric.subscribe(n => valid.push(n));
deltas.pipe(scan((total, n) => total + n, 0)).subscribe(n => totals.push(n));
assert.deepEqual(projected, [0, null, 2]);
assert.deepEqual(valid, [0, 2]);
assert.deepEqual(totals, [0, 2]);
console.log('错误增量投影 [0,null,2] → 筛选 [0,2] → 运行总数 [0,2]。');

const channel = new BehaviorSubject('A'), clear = new Subject<void>();
const first: string[] = [], late: string[] = [];
channel.pipe(distinctUntilChanged()).subscribe(v => first.push(v));
channel.next('A');
clear.next();
channel.pipe(distinctUntilChanged()).subscribe(v => late.push(v));
let clears = 0;
clear.subscribe(() => clears++);
assert.deepEqual(late, ['A']);
channel.next('B');
assert.deepEqual(first, ['A', 'B']);
assert.deepEqual(late, ['A', 'B']);
assert.equal(clears, 0);
console.log('后来面板首值 A，随后 B；旧清空命令收到 0 次。');

const incidentClock = new TestScheduler((a, b) => assert.deepEqual(a, b));
const marks: Mark[] = [];
incidentClock.run(({ cold, expectObservable, expectSubscriptions }) => {
  const stalled = cold('-----x|'), restored = cold('-r|');
  let attempts = 0;
  const replay = defer(() => {
    const attempt = ++attempts;
    marks.push({ at: incidentClock.frame, attempt, kind: 'open' });
    return (attempt === 1 ? stalled : restored).pipe(timeout({ first: 3 }), tap({
      next: value => marks.push({ at: incidentClock.frame, attempt, kind: 'next', value }),
      error: () => marks.push({ at: incidentClock.frame, attempt, kind: 'timeout' }),
      complete: () => marks.push({ at: incidentClock.frame, attempt, kind: 'complete' }),
      unsubscribe: () => marks.push({ at: incidentClock.frame, attempt, kind: 'cancel' })
    }));
  }).pipe(retry(1), catchError(() => of('F')));
  expectObservable(replay).toBe('----r|');
  expectSubscriptions(stalled.subscriptions).toBe('^--!');
  expectSubscriptions(restored.subscriptions).toBe('---^-!');
});
assert.deepEqual(marks.map(m => [m.at, m.attempt, m.kind]), [
  [0, 1, 'open'], [3, 1, 'timeout'], [3, 2, 'open'], [4, 2, 'next'], [5, 2, 'complete']
]);
// 只把对端的响应/完成投给被测播放器。超时是本地策略，必须重新算出来。
const replayClock = new TestScheduler((a, b) => assert.deepEqual(a, b));
replayClock.run(({ expectObservable, flush }) => {
  let attempt = 0;
  const replayed = new Observable<string>(sink => {
    const id = ++attempt;
    const events = marks.filter(m => m.attempt === id);
    const open = events.find(m => m.kind === 'open');
    assert.ok(open, '录像必须包含本次连接的时间原点');
    const pending = new Subscription();
    for (const event of events) {
      if (event.kind === 'next' || event.kind === 'complete') {
        pending.add(replayClock.schedule(() => {
          if (event.kind === 'next') sink.next(event.value);
          else sink.complete();
        }, event.at - open.at));
      }
    }
    return pending;
  }).pipe(timeout({ first: 3 }), retry(1));
  expectObservable(replayed).toBe('----r|');
  flush();
  assert.equal(attempt, 2);
});
console.log('首次连接 [0,3) 超时，第二次 [3,5) 成功，第 4 帧收到 r。');

for (const stateFirst of [false, true]) {
  const click = new Subject<string>(), net = new Subject<string>();
  const previews: string[] = [], sends: string[] = [];
  combineLatest([click, net]).subscribe(pair => previews.push(pair.join('/')));
  click.pipe(withLatestFrom(net)).subscribe(pair => sends.push(pair.join('/')));
  if (stateFirst) net.next('wifi');
  click.next('c1');
  if (!stateFirst) net.next('wifi');
  net.next('4G');
  click.next('c2');
  assert.deepEqual(previews, ['c1/wifi', 'c1/4G', 'c2/4G']);
  assert.deepEqual(sends, stateFirst ? ['c1/wifi', 'c2/4G'] : ['c2/4G']);
  console.log(stateFirst ? '状态先到' : '点击先到', '组合视图', previews, '发送记录', sends);
}

new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ cold, hot, expectObservable, expectSubscriptions }) => {
  const own = cold('-0-?-2|'), live = hot('-0-?-2|');
  expectObservable(own).toBe('-0-?-2|');
  expectObservable(own, '--^').toBe('---0-?-2|');
  expectSubscriptions(own.subscriptions).toBe(['^-----!', '--^-----!']);
  expectObservable(live, '--^').toBe('---?-2|');
});

new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ cold, expectObservable, expectSubscriptions }) => {
  const remote = cold('-a------b|');
  expectObservable(remote.pipe(timeout({ first: 3 }))).toBe('-a------b|');
  expectObservable(remote.pipe(timeout({ each: 3, with: () => of('T') }))).toBe('-a--(T|)');
  expectSubscriptions(remote.subscriptions).toBe(['^--------!', '^---!']);
});

// 两遍各有自己的接纳记录；同一遍中的重连沿用它。
const replayTotals = defer(() => {
  const accepted = new Set<number>();
  let attempts = 0;
  return defer(() => ++attempts === 1
    ? new Observable<number>(sink => { sink.next(41); sink.next(42); sink.error(Error('断线')); })
    : of(41)).pipe(
    retry(1),
    filter(id => !accepted.has(id)),
    tap(id => accepted.add(id)),
    scan((ids, id) => [...ids, id], [] as number[])
  );
});
for (let pass = 0; pass < 2; pass++) {
  const states: number[][] = [];
  replayTotals.subscribe(ids => states.push(ids));
  assert.deepEqual(states, [[41],[41,42]]);
}
console.log('独立详情第 3 帧收到零，跟播详情只见后两行；续包超时在帧 4。两遍重放都只接纳 41、42。');

{
  const liveErrors = new Subject<string>();
  const atSource: string[] = [], atDisplay: string[] = [];
  const mainPanel: string[] = [], detailPanel: string[] = [];
  const view = liveErrors.pipe(
    tap(id => atSource.push(id)),
    shareReplay({ bufferSize: 1, refCount: true }),
    tap(id => atDisplay.push(id))
  );
  const main = view.subscribe(id => mainPanel.push(id));
  liveErrors.next('E1');
  const detail = view.subscribe(id => detailPanel.push(id));
  liveErrors.next('E2');
  main.unsubscribe();
  detail.unsubscribe();
  assert.deepEqual(mainPanel, ['E1', 'E2']);
  assert.deepEqual(detailPanel, mainPanel);
  assert.deepEqual(atSource, ['E1', 'E2']);
  assert.deepEqual(atDisplay, ['E1', 'E1', 'E2', 'E2']);
  console.log('响铃记录：直播来源', atSource, '画面交付', atDisplay);
}
