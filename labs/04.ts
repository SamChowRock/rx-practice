import assert from 'node:assert/strict';
import {
  ArgumentOutOfRangeError, EmptyError, NotFoundError, Observable, SequenceError,
  Subject, Subscription, VirtualTimeScheduler, concat, concatAll, count, defaultIfEmpty,
  elementAt, endWith, every, filter, find, findIndex, first, firstValueFrom, from,
  isEmpty, last, lastValueFrom, map, max, min, of, reduce, scan, scheduled, single,
  skip, skipLast, skipUntil, skipWhile, startWith, take, takeLast, takeUntil,
  takeWhile, throwIfEmpty, toArray,
} from 'rxjs';

async function check<T>(label: string, source: Observable<T>, expected: T[]) {
  const actual = await lastValueFrom(source.pipe(toArray()));
  assert.deepEqual(actual, expected, label);
  console.log(label, '→', actual);
}

const levels = from(['info', 'error', 'error']);
const bits = levels.pipe(map(level => Number(level === 'error')));
const sum = (total: number, bit: number) => total + bit;
await check('面板的累计错误数（含初始值）', bits.pipe(scan(sum, 0), startWith(0)), [0, 0, 1, 2]);
await check('运行报告只交一次', bits.pipe(reduce(sum, 0)), [2]);
const lastError = (lines: string[]) => from(lines).pipe(
  last(line => line.startsWith('error'), '本次无错误'),
);
await check('最后一条错误要等运行结束', lastError(['info', 'error #2', 'error #3', 'info']), ['error #3']);
await check('正常运行结束也有报告', lastError(['info']), ['本次无错误']);
await check('错误行数', levels.pipe(count(level => level === 'error')), [2]);
await check('错误位最小值', bits.pipe(min()), [0]);
await check('错误位最大值', bits.pipe(max((a, b) => a - b)), [1]);
await check('归档保留原顺序', bits.pipe(toArray()), [[0, 1, 1]]);
const empty = from<number[]>([]);
await check('空档案', empty.pipe(toArray()), [[]]);
await check('空档带种子报告', empty.pipe(reduce(sum, 0)), [0]);
await check('无种子无报告', empty.pipe(reduce(sum)), []);
await check('空档是否全合格', empty.pipe(every(n => n > 0)), [true]);
await check('空档检测', empty.pipe(isEmpty()), [true]);
await check('单行即非空', from([0]).pipe(isEmpty()), [false]);
await check('空档占位', empty.pipe(defaultIfEmpty('无记录')), ['无记录']);
await check('头尾状态', empty.pipe(startWith('加载'), endWith('结束')), ['加载', '结束']);
const nums = of(2, 0, 3, 0);
await check('首项、末次命中', concat(nums.pipe(first()), nums.pipe(last(Boolean))), [2, 3]);
await check('恰有一次 3', nums.pipe(single(n => n === 3)), [3]);
await check('缺失位置可回退', nums.pipe(elementAt(4, -1)), [-1]);
await check('从零编号，第 2 个位置', nums.pipe(elementAt(2)), [3]);
await check('查找 9 缺席', nums.pipe(find(n => n === 9)), [undefined]);
await check('查找首个零的位置', nums.pipe(findIndex(n => n === 0)), [1]);
await check('先筛再取', levels.pipe(filter(x => x === 'error'), take(1), map(x => x.toUpperCase())), ['ERROR']);
await check('先取再筛', levels.pipe(take(1), filter(x => x === 'error'), map(x => x.toUpperCase())), []);
await check('略过前两条', nums.pipe(skip(2)), [3, 0]);
await check('扣留最后两条', nums.pipe(skipLast(2)), [2, 0]);
await check('结束交最后两条', nums.pipe(takeLast(2)), [3, 0]);
await check('遇零开闸', nums.pipe(skipWhile(Boolean)), [0, 3, 0]);
await check('遇零收尾，含边界', nums.pipe(takeWhile(Boolean, true)), [2, 0]);
const gate = new Subject<void>();
const live = new Subject<number>();
const before: number[] = [], after: number[] = [];
live.pipe(takeUntil(gate)).subscribe(n => before.push(n));
live.pipe(skipUntil(gate)).subscribe(n => after.push(n));
live.next(1); gate.next(); live.next(2); live.complete();
assert.deepEqual([before, after], [[1], [2]]);
console.log('按钮分界，关闭前与开启后', before, after);

for (const [source, kind] of [
  [empty.pipe(single()), EmptyError],
  [nums.pipe(single(n => n > 9)), NotFoundError],
  [nums.pipe(single(n => n === 0)), SequenceError],
  [nums.pipe(elementAt(9)), ArgumentOutOfRangeError],
  [empty.pipe(throwIfEmpty()), EmptyError],
] as const) {
  await assert.rejects(firstValueFrom(source), (reason: unknown) => {
    assert.ok(reason instanceof kind);
    console.log('拒绝无效查询 →', reason.name, reason.message);
    return true;
  });
}
assert.throws(() => elementAt(-1), ArgumentOutOfRangeError);
await assert.rejects(lastValueFrom(empty.pipe(throwIfEmpty(() => Error('缺少审计行')))), /缺少审计行/);
assert.equal(await firstValueFrom(nums), 2);
assert.equal(await lastValueFrom(nums), 0);
await assert.rejects(firstValueFrom(empty), EmptyError);
await assert.rejects(lastValueFrom(empty), EmptyError);
assert.equal(await firstValueFrom(empty, { defaultValue: null }), null);
assert.equal(await lastValueFrom(empty, { defaultValue: null }), null);

let cleaned = 0;
const ongoing = new Observable<number>(s => {
  s.next(1);
  return () => { cleaned++; };
});
const reports: number[] = [];
const panel: Subscription = ongoing.pipe(count()).subscribe(n => reports.push(n));
assert.deepEqual(reports, []);
assert.equal(panel.closed, false);
panel.unsubscribe();
assert.deepEqual(reports, []);
assert.equal(cleaned, 1);
await check('同样一行，有完成边界', from([1]).pipe(count()), [1]);
console.log('持续源关闭后报告数', reports.length, '清理次数', cleaned);

const policy = { limit: 4 };
await check('旧 thisArg', nums.pipe(every(function (n) { return n < this.limit; }, policy)), [true]);
await check('闭包迁移', nums.pipe(every(n => n < policy.limit)), [true]);
const rule = { threshold: 2 };
const oldPredicate = function (this: typeof rule, n: number) { return n > this.threshold; };
const predicate = (n: number) => n > rule.threshold;
await check('旧查找及闭包版返回相同值', concat(nums.pipe(find(oldPredicate, rule)), nums.pipe(find(predicate))), [3, 3]);
await check('旧查找及闭包版返回相同位置', concat(nums.pipe(findIndex(oldPredicate, rule)), nums.pipe(findIndex(predicate))), [2, 2]);
const clock = new VirtualTimeScheduler();
const base = of(2);
const frames = (stream: Observable<number>) => {
  const events: [number, number][] = [];
  stream.subscribe(value => events.push([clock.now(), value]));
  return events;
};
const oldHead = frames(base.pipe(startWith(1, clock)));
const newHead = frames(scheduled([of(1), base], clock).pipe(concatAll()));
const oldTail = frames(base.pipe(endWith(3, clock)));
const newTail = frames(concat(base, scheduled([3], clock)));
clock.flush();
assert.deepEqual(oldHead, newHead);
assert.deepEqual(oldTail, newTail);
assert.deepEqual(oldHead.map(x => x[1]), [1, 2]);
assert.deepEqual(oldTail.map(x => x[1]), [2, 3]);

const mixed = of('ok', 0);
const strings: Observable<string> = mixed.pipe(first((x): x is string => typeof x === 'string'));
// @ts-expect-error 普通查找可能没有命中
const missing: Observable<string> = mixed.pipe(find((x): x is string => typeof x === 'string'));
void [strings, missing];

// 每次 next 都是完整保留表。只留两行，使第三张表会淘汰第一条红行。
const retained = of(['红'], ['红', '灰'], ['灰', '红']);
const retainedErrors = retained.pipe(map(rows => rows.filter(row => row === '红').length));
await check('保留表中红行数', retainedErrors, [1, 1, 1]);
await check('错误实现重复累计快照', retainedErrors.pipe(scan(sum, 0)), [1, 2, 3]);
await check('接纳处累计红行', of('红', '灰', '红').pipe(
  scan((total, level) => total + Number(level === '红'), 0),
), [1, 1, 2]);

const redIds = of(41, 43);
const aliased = redIds.pipe(scan((ids, id) => {
  ids.push(id);
  return ids;
}, [] as number[]));
await check('旧状态指向同一数组', aliased, [[41, 43], [41, 43]]);
await check('第二次订阅继承被修改的种子', aliased, [[41, 43, 41, 43], [41, 43, 41, 43]]);
const snapshots = redIds.pipe(scan((ids, id) => [...ids, id], [] as number[]));
await check('能回看的两份快照', snapshots, [[41], [41, 43]]);
await check('新订阅从空种子开始', snapshots, [[41], [41, 43]]);

const positions = of('普通', '普通', '错误');
await check('原档链接位置', positions.pipe(findIndex(level => level === '错误')), [2]);
await check('过滤后的链接位置', positions.pipe(filter(level => level === '错误'), findIndex(() => true)), [0]);

