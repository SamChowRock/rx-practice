import assert from 'node:assert/strict';
import {
  EMPTY, Observable, Subject, Subscription, VirtualTimeScheduler, concatAll, concatMap,
  debounceTime, defer, distinctUntilChanged, exhaustAll, exhaustMap, expand, finalize,
  fromEvent, map, mergeAll, mergeMap, mergeScan, of, subscribeOn, switchAll, switchMap,
  switchScan, tap, timer,
} from 'rxjs';
import type { OperatorFunction } from 'rxjs';

type Factory = (key: string) => Observable<string>;
type Policy = (make: Factory) => OperatorFunction<string, string>;
const durations: Record<string, number> = { A: 40, B: 10, C: 5 };
function run(label: string, policy: Policy, expected: string[], fails = false) {
  const clock = new VirtualTimeScheduler();
  const outer = new Subject<string>();
  const events: string[] = [], output: string[] = [];
  const mark = (action: string) => events.push(clock.now() + ' ' + action);
  const make: Factory = key => {
    mark('创建 ' + key);
    return defer(() => {
      mark('订阅 ' + key);
      return timer(durations[key], clock).pipe(map(() => {
        if (fails && key === 'B') throw Error('B 失败');
        return key;
      }), tap({ complete: () => mark('完成 ' + key) }), finalize(() => mark('清理 ' + key)));
    });
  };
  outer.pipe(policy(make)).subscribe({
    next: key => output.push(clock.now() + ' ' + key),
    error: (e: Error) => output.push(clock.now() + ' ' + e.message),
    complete: () => output.push(clock.now() + ' 结束'),
  });
  for (const [i, key] of ['A', 'B', 'C'].entries()) {
    clock.schedule(() => outer.next(key), [0, 5, 6][i]);
  }
  clock.schedule(() => outer.complete(), 7);
  clock.flush();
  assert.deepEqual(output, expected, label);
  console.log(label, '\n  请求', events.join(' → '), '\n  下游', output.join(' → '));
  return events;
}
const cases: [string, Policy, Policy, string[]][] = [
  ['切换', f => switchMap(f), f => source => source.pipe(map(f), switchAll()), ['11 C', '11 结束']],
  ['保存', f => concatMap(f), f => source => source.pipe(map(f), concatAll()), ['40 A', '50 B', '55 C', '55 结束']],
  ['并行', f => mergeMap(f, 2), f => source => source.pipe(map(f), mergeAll(2)), ['15 B', '20 C', '40 A', '40 结束']],
  ['防连点', f => exhaustMap(f), f => source => source.pipe(map(f), exhaustAll()), ['40 A', '40 结束']],
];
for (const [name, direct, nested, expected] of cases) {
  const log = run(name, direct, expected);
  const split = run(name + '（先造流再展开）', nested, expected);
  if (name === '切换') assert.ok(log.includes('5 清理 A') && !log.includes('40 完成 A'));
  if (name === '防连点') {
    assert.ok(!log.includes('5 创建 B'));
    assert.ok(split.includes('5 创建 B') && !split.includes('5 订阅 B'));
  }
}
const failure = run('详情 B 出错', f => mergeMap(f, 2), ['15 B 失败'], true);
assert.ok(failure.includes('15 清理 A') && failure.includes('15 清理 B'));
assert.ok(!failure.some(e => e.endsWith('订阅 C')));

function searchWhileTyping(aDuration: number) {
  const clock = new VirtualTimeScheduler();
  const keys = new Subject<string>();
  const query = (key: string) => timer(key === 'A' ? aDuration : 5, clock).pipe(map(() => key));
  const confirmed = keys.pipe(debounceTime(5, clock), distinctUntilChanged());

  function record(results: Observable<string>) {
    const seen: string[] = [];
    results.subscribe(key => seen.push(clock.now() + ' ' + key));
    return seen;
  }
  const parallel = record(confirmed.pipe(mergeMap(query)));
  const switchOnConfirmation = record(confirmed.pipe(switchMap(query)));
  const switchOnEdit = record(keys.pipe(
    distinctUntilChanged(),
    switchMap(key => timer(5, clock).pipe(switchMap(() => query(key)))),
  ));

  clock.schedule(() => keys.next('A'), 0);
  clock.schedule(() => keys.next('B'), 20);
  clock.schedule(() => keys.complete(), 60);
  clock.flush();
  return { parallel, switchOnConfirmation, switchOnEdit };
}

const lateReply = searchWhileTyping(40);
assert.deepEqual(lateReply.parallel, ['30 B', '45 A']);
assert.deepEqual(lateReply.switchOnConfirmation, ['30 B']);
assert.deepEqual(lateReply.switchOnEdit, ['30 B']);
console.log('搜索回归', lateReply.parallel);
console.log('搜索修复', lateReply.switchOnConfirmation);

// A 在第 5 毫秒开始，17 毫秒后返回，正好落在 B 的等待期内。
const duringPause = searchWhileTyping(17);
assert.deepEqual(duringPause.switchOnConfirmation, ['22 A', '30 B']);
assert.deepEqual(duringPause.switchOnEdit, ['30 B']);
console.log('等待新词确认再切', duringPause.switchOnConfirmation, '改词立即切', duringPause.switchOnEdit);

const button = new EventTarget();
const tasks = new Subject<string>();
const group = new Subscription();
const leaked: string[] = [], current: string[] = [];
group.add(tasks.subscribe(key => group.add(fromEvent(button, 'click').subscribe(() => leaked.push(key)))));
group.add(tasks.pipe(switchMap(key => fromEvent(button, 'click').pipe(map(() => key))))
  .subscribe(key => current.push(key)));
for (const key of ['A', 'B', 'C']) tasks.next(key);
button.dispatchEvent(new Event('click'));
group.unsubscribe();
button.dispatchEvent(new Event('click'));
assert.deepEqual(leaked, ['A', 'B', 'C']);
assert.deepEqual(current, ['C']);
console.log('一次刷新，嵌套监听与切换监听', leaked, current);

for (const mode of ['并发', '串行', '切换']) {
  const clock = new VirtualTimeScheduler();
  const add = (acc: number, delta: number) => timer(delta === 1 ? 10 : 2, clock).pipe(map(() => acc + delta));
  const values: number[] = [];
  of(1, 2).pipe(mode === '切换' ? switchScan(add, 0) : mergeScan(add, 0, mode === '并发' ? 2 : 1))
    .subscribe(n => values.push(n));
  clock.flush();
  assert.deepEqual(values, mode === '并发' ? [2, 1] : mode === '串行' ? [1, 3] : [2]);
  console.log(mode + '累计状态', values);
}

const clock = new VirtualTimeScheduler();
const page = (n: number) => n < 3 ? of(n + 1) : EMPTY;
for (const legacy of [true, false]) {
  const pages: number[] = [];
  of(1).pipe(legacy ? expand(page, Infinity, clock) : expand(n => page(n).pipe(subscribeOn(clock))))
    .subscribe(n => pages.push(n));
  clock.flush();
  assert.deepEqual(pages, [1, 2, 3]);
  console.log(legacy ? '旧分页值序列' : '逐页调度后的值序列', pages);
}

const old: string[] = [], modern: string[] = [];
const keys = of('A', 'B');
keys.pipe(mergeMap(() => of(4, 5), (key, n, i, j) => [key, n, i, j].join('/'), 1))
  .subscribe(v => old.push(v));
keys.pipe(mergeMap((key, i) => of(4, 5).pipe(map((n, j) => [key, n, i, j].join('/'))), 1))
  .subscribe(v => modern.push(v));
assert.deepEqual(modern, ['A/4/0/0', 'A/5/0/1', 'B/4/1/0', 'B/5/1/1']);
assert.deepEqual(old, modern);
console.log('迁移前后保留外值、内值与两层索引', modern);

// @ts-expect-error 投影应返回可订阅或可转换的输入
const bad = keys.pipe(switchMap(key => key.length));
const good: Observable<number> = keys.pipe(switchMap(key => of(key.length)));
void [bad, good];

for (const captureAtClick of [false, true]) {
  const clock = new VirtualTimeScheduler();
  const clicks = new Subject<void>();
  const form = { task: '', text: '' };
  const starts: [number, string, string][] = [];
  const save = (item: typeof form) => {
    starts.push([clock.now(), item.task, item.text]);
    return timer(20, clock);
  };
  const saves = captureAtClick
    ? clicks.pipe(map(() => ({ ...form })), concatMap(save))
    : clicks.pipe(concatMap(() => save(form)));
  const sub = saves.subscribe();
  for (const [at, task, text] of [[0, 'A', '已接单'], [4, 'A', '已复核'], [6, 'B', '待补图']] as const) {
    clock.schedule(() => {
      Object.assign(form, { task, text });
      clicks.next();
    }, at);
  }
  clock.schedule(() => clicks.complete(), 7);
  clock.flush();
  assert.equal(sub.closed, true);
  assert.deepEqual(starts, [[0, 'A', '已接单'], captureAtClick ? [20, 'A', '已复核'] : [20, 'B', '待补图'], [40, 'B', '待补图']]);
  console.log(captureAtClick ? '接单时取值' : '开工时取值', starts);
}

for (const markAtStart of [false, true]) {
  const clock = new VirtualTimeScheduler();
  const selection = new Subject<string>();
  let busy = false;
  const detail = (key: string) => defer(() => {
    if (markAtStart) busy = true;
    return timer(key === 'A' ? 40 : 10, clock).pipe(finalize(() => { busy = false; }));
  });
  const replies: number[] = [];
  selection.pipe(tap(() => { if (!markAtStart) busy = true; }), switchMap(detail))
    .subscribe(() => replies.push(clock.now()));
  clock.schedule(() => selection.next('A'), 0);
  clock.schedule(() => selection.next('B'), 5);
  clock.schedule(() => {
    assert.equal(busy, markAtStart);
    console.log('切换后的忙状态', markAtStart ? '内部订阅置忙' : '外层置忙', busy);
  }, 6);
  clock.schedule(() => selection.complete(), 7);
  clock.flush();
  assert.deepEqual(replies, [15]);
  assert.equal(busy, false);
}

