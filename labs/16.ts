import assert from 'node:assert/strict';
import {
  BehaviorSubject, NEVER, Observable, Subject, Subscription, catchError, debounceTime,
  defer, distinctUntilChanged, exhaustMap, filter, finalize, from, lastValueFrom,
  merge, mergeMap, of, retry, scan, share, shareReplay, takeUntil, timeout,
  type ShareReplayConfig
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

for (const shared of [false, true]) {
  let starts = 0, stops = 0;
  const raw = new Observable<void>(() => { starts++; return () => { stops++; }; });
  const feed = shared ? raw.pipe(share()) : raw;
  const group = new Subscription();
  group.add(feed.subscribe());
  group.add(feed.subscribe());
  const invariant = () => assert.equal(starts, 1);
  if (shared) invariant(); else assert.throws(invariant, assert.AssertionError);
  group.unsubscribe();
  assert.equal(stops, starts);
  console.log(shared ? '修复后' : '修复前', '两个读者，上游启动/关闭', starts, stops);
}
const keepLast: ShareReplayConfig = { bufferSize: 1, refCount: true };
// @ts-expect-error 对象重载要求明确 refCount。
const incomplete: ShareReplayConfig = { bufferSize: 1 };
for (const cached of [true, false]) {
  const events = new Subject<string>();
  const feed = cached ? events.pipe(shareReplay(keepLast)) : events.pipe(share());
  const first = feed.subscribe();
  events.next('old');
  const seen: string[] = [];
  feed.subscribe(v => seen.push(v));
  events.next('new');
  assert.deepEqual(seen, cached ? ['old', 'new'] : ['new']);
  first.unsubscribe(); events.complete();
  console.log('迟到的事件面板', cached ? '缓存开启' : '仅广播', '首值', seen[0]);
}

for (const mode of ['reuse', 'fresh', 'always-fail'] as const) {
  let requests = 0;
  function request(): Promise<string> {
    requests++;
    return requests === 1 || mode === 'always-fail' ? Promise.reject(Error('offline')) : Promise.resolve('live');
  }
  const source = mode === 'reuse' ? from(request()) : defer(request);
  const result = await lastValueFrom(source.pipe(retry(1), catchError(() => of('cached'))));
  assert.equal(requests, mode === 'reuse' ? 1 : 2);
  assert.equal(result, mode === 'fresh' ? 'live' : 'cached');
  console.log(mode, '实际创建请求', requests, '面板结果', result);
}
const clock = () => new TestScheduler((a, b) => assert.deepEqual(a, b));
clock().run(({ cold, expectObservable, expectSubscriptions }) => {
  const hung = cold('-------x');
  expectObservable(hung.pipe(timeout({ first: 2 }), retry({ count: 1, delay: 3 }),
    catchError(() => of('C')))).toBe('-------(C|)');
  expectSubscriptions(hung.subscriptions).toBe(['^-!', '-----^-!']);
});
console.log('两次等待分别占 [0,2)、[5,7)，第 7 帧降级。');

clock().run(({ hot, expectObservable }) => {
  const health = hot('e---r---e----|');
  const fault = (value: string) => value === 'e';
  expectObservable(health.pipe(filter(fault), distinctUntilChanged(), debounceTime(2))).toBe('--e----------|');
  expectObservable(health.pipe(distinctUntilChanged(), debounceTime(2), filter(fault))).toBe('--e-------e--|');
  expectObservable(hot('er---|').pipe(distinctUntilChanged(), debounceTime(2), filter(fault))).toBe('-----|');
});
console.log('恢复前后两次稳定故障均告警，持续不足 2 帧的毛刺被恢复信号撤下。');

function session() {
  const errors = new Subject<1>(), clear = new Subject<0>(), state = new BehaviorSubject(0);
  const reducer = merge(errors, clear).pipe(scan((n, cmd) => cmd === 0 ? 0 : n + 1, 0)).subscribe(n => state.next(n));
  return { errors, clear, state, close() { reducer.unsubscribe(); state.complete(); } };
}
const old = session();
old.errors.next(1); old.clear.next(0); old.errors.next(1);
const late: number[] = [], commands: number[] = [];
old.clear.subscribe(v => commands.push(v));
old.state.subscribe(v => late.push(v));
assert.deepEqual([late, commands], [[1], []]);
old.close();
const next = session();
next.state.subscribe(v => late.push(v));
next.errors.next(1);
assert.deepEqual(late, [1, 0, 1]);
next.close();
console.log('晚加入读到 1，无旧清空命令；新会话先读 0，再计为 1。');

const scheduler = clock();
scheduler.run(({ hot, expectSubscriptions }) => {
  const refresh = hot('-ab------c'), close = hot('----x');
  const cleanups: Array<[string, number]> = [];
  const started: string[] = [];
  refresh.pipe(exhaustMap(batch => from(['L', 'R']).pipe(mergeMap(id => defer(() => {
    started.push(batch + id);
    return NEVER.pipe(finalize(() => cleanups.push([batch + id, scheduler.frame])));
  }), 2))), takeUntil(close)).subscribe();
  expectSubscriptions(refresh.subscriptions).toBe('^---!');
  scheduler.schedule(() => {
    assert.deepEqual(started, ['aL', 'aR']);
    assert.deepEqual(cleanups, [['aL', 4], ['aR', 4]]);
  }, 10);
});
console.log('刷新 b 忙时忽略；aL、aR 在关面板的第 4 帧都清理，c 没有启动。');

for (const finished of [true, false]) {
  clock().run(({ cold, expectObservable, expectSubscriptions, flush }) => {
    const first = cold(finished ? '-a|' : '-a------|'), second = cold('-b|');
    let starts = 0;
    const config = defer(() => ++starts === 1 ? first : second).pipe(shareReplay(keepLast));
    expectObservable(config, '^--!').toBe(finished ? '-a|' : '-a');
    expectObservable(config, '-----^').toBe(finished ? '-----(a|)' : '------b|');
    expectSubscriptions(first.subscriptions).toBe(finished ? '^-!' : '^--!');
    expectSubscriptions(second.subscriptions).toBe(finished ? [] : '-----^-!');
    flush();
    assert.equal(starts, finished ? 1 : 2);
  });
}

clock().run(({ hot, expectObservable }) => {
  const faults = hot('-E------'), fold = hot('--x');
  expectObservable(faults.pipe(takeUntil(fold), debounceTime(3))).toBe('--(E|)');
  expectObservable(faults.pipe(debounceTime(3), takeUntil(fold))).toBe('--|');
});

for (const rightStays of [false, true]) {
  clock().run(({ cold, expectObservable, expectSubscriptions }) => {
    const failed = cold('--#'), restored = cold('-v|');
    let attempts = 0;
    const feed = defer(() => ++attempts === 1 ? failed : restored).pipe(
      retry({ count: 1, delay: 3 }), share()
    );
    expectObservable(feed, '^--!').toBe('');
    expectObservable(feed, rightStays ? '^' : '^---!').toBe(rightStays ? '------v|' : '');
    expectSubscriptions(failed.subscriptions).toBe('^-!');
    expectSubscriptions(restored.subscriptions).toBe(rightStays ? '-----^-!' : []);
  });
}

for (const recoverBeforeCache of [true, false]) {
  clock().run(({ cold, expectObservable, expectSubscriptions }) => {
    const offline = cold('-#'), online = cold('-v|');
    let reads = 0;
    const raw = defer(() => ++reads === 1 ? offline : online);
    const fallback = () => of('C');
    const config = recoverBeforeCache
      ? raw.pipe(catchError(fallback), shareReplay(keepLast))
      : raw.pipe(shareReplay(keepLast), catchError(fallback));
    expectObservable(config).toBe('-(C|)');
    expectObservable(config, '---^').toBe(recoverBeforeCache ? '---(C|)' : '----v|');
    expectSubscriptions(online.subscriptions).toBe(recoverBeforeCache ? [] : '---^-!');
  });
}
console.log('完成的配置会重放；关闭放在防抖之后时静默收尾；最后一位读者离开会撤销等待中的共享重试。');
