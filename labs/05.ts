import assert from 'node:assert/strict';
import {
  EMPTY, VirtualTimeScheduler, audit, auditTime, concat, debounce, debounceTime, defer,
  delay, delayWhen, distinctUntilChanged, finalize, fromEvent, ignoreElements, interval,
  map, of, sample, sampleTime, take, takeUntil, tap, throttle, throttleTime, timeInterval,
  timer, timestamp,
} from 'rxjs';
import type { Observable, ThrottleConfig, TimeInterval, Timestamp, TimestampProvider } from 'rxjs';

type Point<T> = [number, T];
function trace<T>(label: string, build: (clock: VirtualTimeScheduler) => Observable<T>, expected: Point<T>[]) {
  const clock = new VirtualTimeScheduler();
  const log: Point<T>[] = [];
  const sub = build(clock).subscribe(value => log.push([clock.now(), value]));
  clock.flush();
  assert.equal(sub.closed, true, label);
  assert.deepEqual(log, expected, label);
  console.log(label, '→ [毫秒, 值]', log);
}

function inputs(clock: VirtualTimeScheduler, points: Point<string>[], end: number) {
  const target = new EventTarget();
  for (const [at, value] of points) {
    clock.schedule(() => target.dispatchEvent(new CustomEvent('input', { detail: value })), at);
  }
  return fromEvent<CustomEvent<string>>(target, 'input').pipe(
    map(event => event.detail), takeUntil(timer(end, clock)),
  );
}
const keys = (s: VirtualTimeScheduler) => inputs(s, [[0, 'A'], [80, 'B'], [190, 'C']], 460);
const quiet: Point<string>[] = [[340, 'C']];
const head: Point<string>[] = [[0, 'A'], [190, 'C']];
const tail: Point<string>[] = [[150, 'B'], [340, 'C']];
const ticks: Point<string>[] = [[150, 'B'], [300, 'C']];
trace('停手搜索', s => keys(s).pipe(debounceTime(150, s)), quiet);
trace('每次重设释放信号', s => keys(s).pipe(debounce(() => timer(150, s))), quiet);
trace('首次点击立即刷新', s => keys(s).pipe(throttleTime(150, s)), head);
trace('首条打开禁止窗口', s => keys(s).pipe(throttle(() => timer(150, s))), head);
trace('窗口到期绘制最新值', s => keys(s).pipe(auditTime(150, s)), tail);
trace('首条决定绘制时刻', s => keys(s).pipe(audit(() => timer(150, s))), tail);
trace('定时读数', s => keys(s).pipe(sampleTime(150, s)), ticks);
trace('外部刷新时钟', s => keys(s).pipe(sample(interval(150, s))), ticks);
const both: ThrottleConfig = { leading: true, trailing: true };
trace('前沿立即发，尾沿再开窗口', s => keys(s).pipe(throttleTime(150, s, both)),
  [[0, 'A'], [150, 'B'], [300, 'C']]);
trace('每行都推迟', s => keys(s).pipe(delay(150, s)), [[150, 'A'], [230, 'B'], [340, 'C']]);
trace('逐行等待，允许乱序', s => keys(s).pipe(delayWhen(k => timer(k === 'A' ? 300 : 10, s))),
  [[90, 'B'], [200, 'C'], [300, 'A']]);
trace('空释放流不放行', () => of('A').pipe(delayWhen(() => EMPTY)), []);
trace('完成立即交防抖末值', s => inputs(s, [[0, 'A']], 10).pipe(debounceTime(150, s)), [[10, 'A']]);
trace('完成后等审核窗口', s => inputs(s, [[0, 'A']], 10).pipe(auditTime(150, s)), [[150, 'A']]);
trace('完成前未采样', s => inputs(s, [[0, 'A']], 10).pipe(sampleTime(150, s)), []);

const repeats = (s: VirtualTimeScheduler) => inputs(s, [[0, 'A'], [80, 'A'], [190, 'B'], [400, 'A']], 600);
trace('相邻去重在前', s => repeats(s).pipe(distinctUntilChanged(), debounceTime(150, s)),
  [[150, 'A'], [340, 'B'], [550, 'A']]);
trace('停手后再去重', s => repeats(s).pipe(debounceTime(150, s), distinctUntilChanged()),
  [[340, 'B'], [550, 'A']]);

let work = 0, cleaned = 0;
trace('关闭按钮停止心跳', s => {
  const close = new EventTarget();
  s.schedule(() => close.dispatchEvent(new Event('click')), 25);
  s.schedule(() => assert.equal(work, 2), 100);
  return interval(10, s).pipe(tap(() => work++), finalize(() => cleaned++),
    takeUntil(fromEvent(close, 'click')));
}, [[10, 0], [20, 1]]);
assert.equal(cleaned, 1);
trace('指定首拍，再按周期', s => timer(3, 10, s).pipe(take(3)), [[3, 0], [13, 1], [23, 2]]);
trace('旧单次计时器', s => timer(3, undefined, s), [[3, 0]]);
trace('单次计时器', s => timer(3, s), [[3, 0]]);
for (const legacy of [true, false]) {
  trace(legacy ? '旧订阅门' : '显式订阅门', s => {
    const gate = timer(50, s);
    const source = defer(() => of(s.now()));
    const wait = () => timer(10, s);
    return legacy ? source.pipe(delayWhen(wait, gate))
      : concat(gate.pipe(take(1), ignoreElements()), source.pipe(delayWhen(wait)));
  }, [[60, 50]]);
}
trace('相对订阅和前一项的间隔', s => keys(s).pipe(timeInterval(s), map((t: TimeInterval<string>) => t.interval)),
  [[0, 0], [80, 80], [190, 110]]);
trace('自定义时钟的绝对读数', s => {
  const provider: TimestampProvider = { now: () => 1000 + s.now() };
  return keys(s).pipe(timestamp(provider), map((t: Timestamp<string>) => [t.value, t.timestamp] as const));
}, [[0, ['A', 1000]], [80, ['B', 1080]], [190, ['C', 1190]]]);
// @ts-expect-error 时钟必须返回数值
const badClock: TimestampProvider = { now: () => '12:00' };
// @ts-expect-error 边沿是布尔开关
const badConfig: ThrottleConfig = { trailing: 150 };
// @ts-expect-error 间隔记录没有 timestamp 成员
const badInterval: TimeInterval<number> = { value: 0, timestamp: 10 };
// @ts-expect-error 时刻记录没有 interval 成员
const badStamp: Timestamp<number> = { value: 0, interval: 10 };
void [badClock, badConfig, badInterval, badStamp];

const clearSearch = (s: VirtualTimeScheduler) => inputs(s, [[10, 'error'], [70, '']], 300);
trace('清空与普通词一起等待', s => clearSearch(s).pipe(debounceTime(150, s)), [[220, '']]);
trace('清空立即生效并撤销旧候选', s => clearSearch(s).pipe(
  debounce(word => word === '' ? of(0) : timer(150, s)),
), [[70, '']]);
trace('空等待流只能等到源完成才交末词', s => clearSearch(s).pipe(
  debounce(word => word === '' ? EMPTY : timer(150, s)),
), [[300, '']]);

{
  const clock = new VirtualTimeScheduler();
  const target = new EventTarget();
  const levels = fromEvent<CustomEvent<string>>(target, 'level').pipe(map(event => event.detail));
  const paints = fromEvent(target, 'paint');
  function openPanel() {
    const local: Point<string>[] = [], common: Point<string>[] = [];
    const sub = levels.pipe(sampleTime(100, clock)).subscribe(value => local.push([clock.now(), value]));
    sub.add(levels.pipe(sample(paints)).subscribe(value => common.push([clock.now(), value])));
    return { local, common, sub };
  }

  const firstPanel = openPanel();
  clock.schedule(() => {
    const secondPanel = openPanel();
    firstPanel.sub.add(secondPanel.sub);
    clock.schedule(() => {
      assert.deepEqual(secondPanel.local, [[140, '高'], [240, '低']]);
      assert.deepEqual(secondPanel.common, [[100, '低'], [200, '低']]);
      console.log('晚开的图，本地节拍与公共节拍', secondPanel.local, secondPanel.common);
    }, 310);
  }, 40);
  for (const [at, value] of [[60, '低'], [110, '高'], [155, '低']] as const) {
    clock.schedule(() => target.dispatchEvent(new CustomEvent('level', { detail: value })), at);
  }
  for (const at of [100, 200, 300]) {
    clock.schedule(() => target.dispatchEvent(new Event('paint')), at);
  }
  clock.schedule(() => firstPanel.sub.unsubscribe(), 400);
  clock.flush();
  assert.deepEqual(firstPanel.local, [[100, '低'], [200, '低']]);
  assert.deepEqual(firstPanel.common, [[100, '低'], [200, '低']]);
}

