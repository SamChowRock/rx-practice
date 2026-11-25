import assert from 'node:assert/strict';
import { Scheduler, VirtualAction, VirtualTimeScheduler, animationFrames, animationFrameScheduler, asapScheduler, asyncScheduler, catchError, debounceTime, defer, distinctUntilChanged, map, observeOn, of, queueScheduler, retry, scheduled, share, shareReplay, subscribeOn, switchMap, take, takeUntil, timeout } from 'rxjs';
import type { SchedulerAction, SchedulerLike } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import type { RunHelpers } from 'rxjs/testing';

const order: string[] = [];
queueScheduler.schedule(function () {
  order.push('外层开始');
  queueScheduler.schedule(() => order.push('嵌套'));
  order.push('外层结束');
});
await new Promise<void>(resolve => {
  asyncScheduler.schedule(() => { order.push('计时器'); resolve(); });
  asapScheduler.schedule(() => order.push('asap'));
  order.push('同步');
});
assert.deepEqual(order, ['外层开始', '外层结束', '嵌套', '同步', 'asap', '计时器']);
console.log('同一轮零延迟调度', order);

const clock = new VirtualTimeScheduler(VirtualAction, 4);
const ticks: number[] = [];
const like: SchedulerLike = clock;
like.schedule(function(this: SchedulerAction<number>, left: number) {
  ticks.push(clock.now());
  if (left > 1) this.schedule(left - 1, 2);
}, 2, 3);
clock.flush();
assert.deepEqual(ticks, [2, 4]);
clock.maxFrames = 6;
clock.flush();
assert.deepEqual(ticks, [2, 4, 6]);
assert.ok(typeof Scheduler.now() === 'number');
// @ts-expect-error a timestamp provider alone cannot schedule work
const invalid: SchedulerLike = { now: Date.now };
// @ts-expect-error scheduling alone does not implement Subscription teardown
const badAction: SchedulerAction<number> = { schedule: () => clock.schedule(() => {}) };
// @ts-expect-error a RunHelpers fixture must also support expectations and sources
const badHelpers: RunHelpers = { flush: () => {} };
console.log('提高虚拟时间上限后执行剩余任务', ticks);

const make = () => new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
make().run(({ cold, hot, expectObservable, expectSubscriptions, time }: RunHelpers) => {
  const tasks = hot('a---b------');
  const close = hot('-------x');
  const a = cold('--x--y|'), b = cold('--z--w|');
  assert.equal(time('---|'), 3);
  expectObservable(tasks.pipe(switchMap(k => k === 'a' ? a : b), takeUntil(close))).toBe('--x---z|');
  expectSubscriptions(a.subscriptions).toBe('^---!');
  expectSubscriptions(b.subscriptions).toBe('----^--!');
  const keys = hot('a-a----|');
  expectObservable(keys.pipe(debounceTime(3), distinctUntilChanged())).toBe('-----a-|');
  expectObservable(keys.pipe(distinctUntilChanged(), debounceTime(3))).toBe('---a---|');
});
console.log('切任务在帧 4 断开 A，关面板在帧 7 断开 B；相同按键的防抖结果分别在帧 5、3。');
for (const replay of [false, true]) {
  make().run(({ cold, expectObservable, expectSubscriptions }) => {
    const source = cold('-a--b----');
    const view = replay ? source.pipe(shareReplay({ bufferSize: 1, refCount: true })) : source.pipe(share());
    expectObservable(view, '^----!').toBe('-a--b');
    expectObservable(view, '--^---!').toBe(replay ? '--a-b' : '----b');
    expectObservable(view, '--------^--!').toBe('---------a');
    expectSubscriptions(source.subscriptions).toBe(['^-----!', '--------^--!']);
  });
}
console.log('两块面板重叠时共用连接，帧 6 到 8 无订阅者；新连接重新从 a 开始。');
make().run(({ hot, cold, expectObservable, expectSubscriptions }) => {
  const request = cold('------x|');
  const input = hot('a-------b|');
  expectObservable(input.pipe(switchMap(k => k === 'b' ? of('b') : request.pipe(
    timeout({ first: 2 }), retry(1), catchError(() => of('f'))
  )))).toBe('----f---b|');
  expectSubscriptions(request.subscriptions).toBe(['^-!', '--^-!']);
});
console.log('帧 2 和 4 两次超时，帧 4 降级，帧 8 新任务仍能运行。');
make().run(({ animate, expectObservable }) => {
  animate('--x--x');
  expectObservable(animationFrames().pipe(map(f => f.elapsed), take(2))).toBe('--a--(b|)', { a: 2, b: 5 });
  expectObservable(scheduled(['绘制'], animationFrameScheduler).pipe(take(1))).toBe('--(a|)', { a: '绘制' });
});
const timing = make();
timing.run(({ flush }) => {
  const log: string[] = [];
  const source = defer(() => { log.push('开工@' + timing.frame); return of(1); });
  source.pipe(observeOn(asyncScheduler, 2)).subscribe(() => log.push('收到@' + timing.frame));
  source.pipe(subscribeOn(asyncScheduler, 2)).subscribe(() => log.push('收到@' + timing.frame));
  assert.deepEqual(log, ['开工@0']);
  flush();
  assert.deepEqual(log, ['开工@0', '收到@2', '开工@2', '收到@2']);
  console.log('推迟通知与推迟订阅', log, '；手动指定绘制帧 2、5。');
});

// 完成会交付尾字；撤掉等待不会。
make().run(({ hot, cold, expectObservable, expectSubscriptions }) => {
  const close = hot('---c');
  const before = hot('-x------');
  const after = hot('-x------');
  expectObservable(before.pipe(takeUntil(close), debounceTime(4))).toBe('---(x|)');
  expectObservable(after.pipe(debounceTime(4), takeUntil(close))).toBe('---|');
  expectSubscriptions(before.subscriptions).toBe('^--!');
  expectSubscriptions(after.subscriptions).toBe('^--!');
  expectObservable(cold('-x-|').pipe(debounceTime(4))).toBe('---(x|)');
});

make().run(({ hot, cold, expectObservable, expectSubscriptions }) => {
  const close = hot('----c');
  const leaky = cold('--#');
  const owned = cold('--#');
  const detachedWait = cold('-----g|');
  const wait = cold('-----g|');
  expectObservable(leaky.pipe(takeUntil(close), retry({ count: 1, delay: () => detachedWait }))).toBe('---------#');
  expectObservable(owned.pipe(retry({ count: 1, delay: () => wait }), takeUntil(close))).toBe('----|');
  expectSubscriptions(leaky.subscriptions).toBe(['^-!', '-------^-!']);
  expectSubscriptions(owned.subscriptions).toBe('^-!');
  expectSubscriptions(detachedWait.subscriptions).toBe('--^----!');
  expectSubscriptions(wait.subscriptions).toBe('--^-!');
});

make().run(({ cold, hot, expectObservable, expectSubscriptions }) => {
  const slow = cold('------x|');
  const tasks = hot('a-------b|');
  const view = tasks.pipe(
    switchMap(task => task === 'b' ? of('b') : slow.pipe(timeout({ first: 2 }), retry(1))),
    catchError(() => of('f'))
  );
  expectObservable(view).toBe('----(f|)');
  expectSubscriptions(slow.subscriptions).toBe(['^-!', '--^-!']);
  expectSubscriptions(tasks.subscriptions).toBe('^---!');
});

for (const immutable of [false, true]) {
  const clock = make();
  clock.run(({ hot, flush }) => {
    let state = { count: 0 };
    const immediate: number[][] = [];
    const history: { count: number }[] = [];
    hot('-a-a|').pipe(map(() => {
      if (immutable) state = { count: state.count + 1 };
      else state.count++;
      return state;
    })).subscribe(value => {
      immediate.push([clock.frame, value.count]);
      history.push(value);
    });
    flush();
    assert.deepEqual(immediate, [[1, 1], [3, 2]]);
    assert.deepEqual(history.map(value => value.count), immutable ? [1, 2] : [2, 2]);
  });
}
console.log('已核对关闭时的尾字、等待重试的订阅区间和两帧计数快照。');
