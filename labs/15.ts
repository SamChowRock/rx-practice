import assert from 'node:assert/strict';
import {
  BehaviorSubject, combineLatest, concatMap, defer, finalize, from, fromEvent,
  map, mergeMap, takeUntil, tap, timer, toArray, withLatestFrom, catchError, of, take
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

const clock = new TestScheduler((a, b) => assert.deepEqual(a, b));
const button = new EventTarget();
const rows = new BehaviorSubject<readonly string[]>(['slow', 'medium', 'fast']);
const format = new BehaviorSubject<'csv' | 'json'>('csv');
// 此函数只由 tsc 检查，用来锁定表单与导出模块的参数边界。
function checkFormat(value: typeof format) {
  value.next('json');
  // @ts-expect-error 界面声明的导出格式不含 xml。
  value.next('xml');
}
const params = combineLatest({ rows, format });
const sizes: number[] = [];
// 演示用估算器：每条 CSV 占 2 单位、JSON 占 5 单位。
const preview = params.pipe(map(p => p.rows.length * (p.format === 'csv' ? 2 : 5)))
  .subscribe(n => sizes.push(n));
const clicks = fromEvent(button, 'export').pipe(withLatestFrom(params),
  map(([, p]) => ({ format: p.format, rows: [...p.rows] })));
const generated: string[] = [], written: string[] = [], reads: string[] = [];
let active = 0, peak = 0;
const writer = clicks.pipe(concatMap(job => defer(() => {
  generated.push(job.format);
  return from(job.rows).pipe(
    mergeMap((id, index) => defer(() => {
      peak = Math.max(peak, ++active);
      return timer(id === 'slow' ? 6 : id === 'medium' ? 3 : 1, clock).pipe(
        map(() => ({ id, index })), tap(() => reads.push(id)), finalize(() => active--)
      );
    }), 2),
    toArray(), map(parts => parts.sort((a, b) => a.index - b.index).map(p => p.id)),
    concatMap(ids => timer(2, clock).pipe(map(() => job.format + ':' + ids.join(','))))
  );
}))).subscribe(file => written.push(file));
const click = () => button.dispatchEvent(new Event('export'));
click();
format.next('json');
rows.next(['fast']);
click();
assert.deepEqual(generated, ['csv']);
clock.flush();
assert.deepEqual(sizes, [6, 15, 5]);
assert.equal(peak, 2);
assert.deepEqual(reads, ['medium', 'fast', 'slow', 'fast']);
assert.deepEqual(written, ['csv:slow,medium,fast', 'json:fast']);
console.log('预估大小', sizes, '附件峰值', peak, '读完顺序', reads, '落盘顺序', written);

click(); // 第三份开始
format.next('csv');
click(); // 第四份排队
writer.unsubscribe();
clock.flush();
assert.deepEqual(generated, ['csv', 'json', 'json']);
assert.equal(active, 0);
assert.equal(written.length, 2);
preview.unsubscribe();
console.log('显式取消后第三份停止，第四份没有开始生成，磁盘仍只有前两份。');

for (const early of [false, true]) {
  new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ hot, expectObservable, expectSubscriptions }) => {
    // 两个热源按创建顺序把各自的绝对帧动作放入虚拟队列。
    const batch = hot('---b---c|');
    const cancel = hot(early ? '--x' : '---x');
    expectObservable(batch.pipe(takeUntil(cancel))).toBe(early ? '--|' : '---(b|)');
    expectSubscriptions(batch.subscriptions).toBe(early ? '^-!' : '^--!');
    expectSubscriptions(cancel.subscriptions).toBe(early ? '^-!' : '^--!');
  });
  console.log(early ? '取消提前至帧 2：没有批次。' : '同在帧 3：已排在前面的批次 b 先交付，随即取消。');
}

{
  const disk = new TestScheduler((a, b) => assert.deepEqual(a, b));
  const selected = [{ id: 1, text: '连接中' }];
  const shallow = [...selected];
  const projected = selected.map(({ id, text }) => ({ id, text }));
  disk.schedule(() => { selected[0].text = '已恢复'; }, 1);
  const files: string[] = [];
  disk.schedule(() => files.push(shallow[0].text, projected[0].text), 3);
  disk.flush();
  assert.deepEqual(files, ['已恢复', '连接中']);
}

for (const localRecovery of [false, true]) {
  new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ hot, cold, expectObservable, expectSubscriptions }) => {
    const jobs = hot('ab----|'), broken = cold('--#'), good = cold('-s|');
    const failure = () => of('F');
    const queue = jobs.pipe(concatMap(id => {
      const file = id === 'a' ? broken : good;
      return localRecovery ? file.pipe(catchError(failure)) : file;
    }));
    expectObservable(queue.pipe(catchError(failure))).toBe(localRecovery ? '--Fs--|' : '--(F|)');
    expectSubscriptions(broken.subscriptions).toBe('^-!');
    expectSubscriptions(good.subscriptions).toBe(localRecovery ? '--^-!' : []);
  });
}

new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ cold, expectObservable, expectSubscriptions }) => {
  const read = cold('-x|');
  const reports = from([[], ['x']]).pipe(concatMap(rows => from(rows).pipe(
    mergeMap(() => read.pipe(take(1))), toArray(),
    concatMap(parts => timer(2).pipe(map(() => parts.length)))
  )));
  expectObservable(reports).toBe('--a--(b|)', { a: 0, b: 1 });
  expectSubscriptions(read.subscriptions).toBe('--^!');
});

new TestScheduler((a, b) => assert.deepEqual(a, b)).run(({ hot, expectObservable }) => {
  const parts = hot('-a---b|'), cancel = hot('---x');
  expectObservable(parts.pipe(takeUntil(cancel), toArray())).toBe('---(p|)', { p: ['a'] });
  expectObservable(parts.pipe(toArray(), takeUntil(cancel))).toBe('---|');
});
console.log('旧字随点击保存，坏附件只结束本份任务；空表在帧 2 提交，取消时不会交出半份数组。');
