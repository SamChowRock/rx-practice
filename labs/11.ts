import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { BehaviorSubject, EMPTY, NEVER, Notification, NotificationKind, Subject, TimeoutError, catchError, concat, concatMap, config, debounceTime, defer, dematerialize, exhaustMap, finalize, firstValueFrom, ignoreElements, lastValueFrom, materialize, mergeMap, of, onErrorResumeNextWith, repeat, retry, switchMap, throwError, timeout, toArray } from 'rxjs';
import type { CompleteNotification, ErrorNotification, GlobalConfig, NextNotification, ObservableNotification, RepeatConfig, RetryConfig, TimeoutConfig, TimeoutInfo, ValueFromNotification } from 'rxjs';
import { Subscriber } from 'rxjs';

const search = new Subject<string>();
const read = (q: string) => q === 'bad' ? throwError(() => Error(q)) : of(q);
for (const local of [false, true]) {
  const result: string[] = [];
  const input = search.pipe(debounceTime(1));
  const queries = local ? input.pipe(switchMap(q => read(q).pipe(catchError(() => of('离线')))))
    : input.pipe(switchMap(read), catchError(() => of('离线')));
  const sub = queries.subscribe(v => result.push(v));
  search.next('bad');
  await sleep(5);
  search.next('good');
  await sleep(5);
  assert.deepEqual(result, local ? ['离线', 'good'] : ['离线']);
  sub.unsubscribe();
  console.log(local ? '查询内部恢复' : '输入链末端恢复', result);
}
for (const flatten of [concatMap, mergeMap, exhaustMap]) {
  const commands = new Subject<number>(), first = new Subject<number>();
  const started: number[] = [], results: number[] = [];
  const sub = commands.pipe(flatten((id: number) => {
    started.push(id);
    return (id === 1 ? first : of(id)).pipe(catchError(() => EMPTY));
  })).subscribe(id => results.push(id));
  commands.next(1); commands.next(2);
  const before = started.slice();
  first.error(Error('连接断开'));
  commands.next(3);
  assert.deepEqual(before, flatten === mergeMap ? [1, 2] : [1]);
  assert.deepEqual(results, flatten === exhaustMap ? [3] : [2, 3]);
  sub.unsubscribe();
  console.log(flatten.name, '失败前已启动', before, '最终成功', results);
}
const lost = new Subject<string>(), state = new BehaviorSubject('在线');
lost.next('断线'); state.next('重连中');
const late: string[] = [];
lost.subscribe(v => late.push(v)); state.subscribe(v => late.push(v));
assert.deepEqual(late, ['重连中']);
console.log('中途挂载的状态栏', late);

let attempts = 0, single = 0, round = 0;
const policy: RetryConfig = { count: 2, delay: (_, attempt) => of(attempt), resetOnSuccess: false };
const successful = await firstValueFrom(defer(() => ++attempts < 3 ? read('bad') : read('good')).pipe(
  finalize(() => single++), retry(policy), finalize(() => round++)
));
assert.deepEqual([successful, attempts, single, round], ['good', 3, 3, 1]);
const again: RepeatConfig = { count: 2, delay: () => of(0) };
assert.deepEqual(await lastValueFrom(of(1).pipe(repeat(again), toArray())), [1, 1]);
console.log('两次失败后成功：请求工厂 3 次，单次清理 3 次，整轮清理 1 次；repeat(2) 共两轮。');

let factories = 0, subscriptions = 0;
await firstValueFrom(defer(() => {
  factories++;
  return defer(() => ++subscriptions < 3 ? read('bad') : read('good')).pipe(retry(2));
}));
assert.deepEqual([factories, subscriptions], [1, 3]);
console.log('将请求工厂移到重试外：创建一次可重订阅的源，内部订阅三次。');

const cutoff = {
  first: 1, meta: '历史', with: (info: TimeoutInfo<number, string>) => of(info.meta + '/' + info.seen)
} satisfies TimeoutConfig<number, import('rxjs').Observable<string>, string>;
assert.equal(await firstValueFrom(NEVER.pipe(timeout(cutoff))), '历史/0');
await assert.rejects(firstValueFrom(NEVER.pipe(timeout(1))), TimeoutError);
const n: NextNotification<number> = { kind: 'N', value: 0 };
const e: ErrorNotification = { kind: 'E', error: '断线' };
const c: CompleteNotification = { kind: 'C' };
const recording: ObservableNotification<number>[] = [n, e];
const zero: ValueFromNotification<typeof n> = 0;
// @ts-expect-error complete has no emitted value
const noValue: ValueFromNotification<typeof c> = 0;
// @ts-expect-error N requires value
const invalidNote: ObservableNotification<number> = { kind: 'N' };
// @ts-expect-error an error record needs its error payload
const invalidError: ErrorNotification = { kind: 'E' };
// @ts-expect-error completion has no value field
const invalidComplete: CompleteNotification = { kind: 'C', value: 1 };
// @ts-expect-error seen counts notifications
const invalidInfo: TimeoutInfo<number, string> = { seen: '0', lastValue: null, meta: '历史' };
// @ts-expect-error retry delays must emit via an ObservableInput
const invalidRetry: RetryConfig = { delay: () => 42 };
// @ts-expect-error repeat count is numeric
const invalidRepeat: RepeatConfig = { count: '2' };
// @ts-expect-error each measures a duration
const invalidTimeout: TimeoutConfig<number> = { each: new Date() };
const notes = await lastValueFrom(concat(of(zero), throwError(() => '断线')).pipe(materialize(), toArray()));
assert.deepEqual(notes.map(x => x.kind), ['N', 'E']);
const replayed: unknown[] = [];
of(...recording, c).pipe(dematerialize()).subscribe({ next: v => replayed.push(v), error: err => replayed.push(err) });
assert.deepEqual(replayed, [0, '断线']);
assert.equal(Notification.createComplete().kind, NotificationKind.COMPLETE);
assert.deepEqual(await lastValueFrom(of(1).pipe(ignoreElements(), onErrorResumeNextWith([read('bad'), of(2)]), toArray())), [2]);
const globals: GlobalConfig = config;
// @ts-expect-error the hook receives a notification, not a number
const badHook: GlobalConfig['onStoppedNotification'] = (v: number) => {};
const previous = globals.onStoppedNotification;
const stopped: string[] = [];
try {
  globals.onStoppedNotification = note => stopped.push(note.kind);
  const ended = new Subscriber<number>();
  ended.complete();
  ended.next(42);
  await sleep(5);
  assert.deepEqual(stopped, ['N']);
} finally {
  globals.onStoppedNotification = previous;
}
console.log('首包超时记录 历史/0；通知序列 N,E 回放为', replayed, '；订阅完成后的值触发诊断', stopped);

for (const releaseBeforeSwitch of [false, true]) {
  const tasks = new Subject<string>(), releaseRetry = new Subject<void>();
  const calls: string[] = [], delivered: string[] = [];
  const sub = tasks.pipe(switchMap(task => defer(() => {
    calls.push(task);
    return task === 'A' && calls.length === 1 ? read('bad') : of(task);
  }).pipe(retry({ count: 1, delay: () => releaseRetry })))).subscribe(v => delivered.push(v));
  tasks.next('A');
  if (releaseBeforeSwitch) releaseRetry.next();
  tasks.next('B');
  if (!releaseBeforeSwitch) releaseRetry.next();
  assert.deepEqual(calls, releaseBeforeSwitch ? ['A', 'A', 'B'] : ['A', 'B']);
  assert.deepEqual(delivered, releaseBeforeSwitch ? ['A', 'B'] : ['B']);
  sub.unsubscribe();
}
console.log('等待期间切到 B 会取消 A 的重试；先放行再切换则确实尝试两次 A。');

for (const reuseKey of [false, true]) {
  // A synchronous append-only service model drops only the first acknowledgement.
  const receipts = new Map<string, number>();
  const notes: string[] = [];
  let sends = 0;
  const version = await lastValueFrom(defer(() => {
    sends++;
    const key = reuseKey ? 'click-1' : 'send-' + sends;
    if (!receipts.has(key)) {
      notes.push('已复核');
      receipts.set(key, 7 + notes.length);
    }
    return sends === 1 ? throwError(() => Error('回执丢失')) : of(receipts.get(key)!);
  }).pipe(retry(1)));
  assert.equal(sends, 2);
  assert.equal(notes.length, reuseKey ? 1 : 2);
  assert.equal(version, reuseKey ? 8 : 9);
}
console.log('提交后丢回执，两次传输共用点击编号只追加一次；每次换编号会追加两次。');
