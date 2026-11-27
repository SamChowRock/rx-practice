import assert from 'node:assert/strict';
import { EMPTY, Subject, catchError, forkJoin, map, of, throwError } from 'rxjs';
import type { Observable } from 'rxjs';
import { archiveA, archiveReader } from './archive.ts';

function column(rows: Observable<string[]>) {
  return rows.pipe(
    map(list => ({ status: '已读完' as const, count: list.length })),
    catchError(() => of({ status: '读取失败' as const })),
  );
}

const rowsFor = archiveReader((build, cursor) => {
  if (build === 'C') return throwError(() => Error('归档离线'));
  if (build === 'B') return of({ rows: [], next: null });
  return of(archiveA[cursor]);
});
for (const right of ['B', 'C']) {
  const reports: unknown[] = [];
  forkJoin({ left: column(rowsFor('A')), right: column(rowsFor(right)) })
    .subscribe(report => reports.push(report));
  assert.deepEqual(reports, [{ left: { status: '已读完', count: 2 }, right: right === 'B'
    ? { status: '已读完', count: 0 } : { status: '读取失败' } }]);
  console.log('A 对照 ' + right, reports[0]);
}

const signals: string[] = [];
forkJoin({ left: rowsFor('A'), right: EMPTY })
  .subscribe({ next: () => signals.push('生成报告'), complete: () => signals.push('完成') });
assert.deepEqual(signals, ['完成']);
console.log('把空归档换成 EMPTY', signals);

const pending = new Subject<string[]>();
const counts: number[] = [];
forkJoin({ left: rowsFor('A'), right: pending }).subscribe(report => counts.push(report.right.length));
pending.next(['甲']);
pending.next(['甲', '乙']);
assert.deepEqual(counts, []);
pending.complete();
assert.deepEqual(counts, [2]);
console.log('右栏发过两版，完成时才采用末版', counts);

const slow = new Subject<string[]>();
const broken = throwError(() => Error('归档离线'));
const raw = forkJoin({ left: slow, right: broken }).subscribe({ error: () => {} });
assert.ok(raw.closed);
assert.equal(slow.observed, false);
const partial: unknown[] = [];
forkJoin({ left: column(slow), right: column(broken) }).subscribe(report => partial.push(report));
assert.ok(slow.observed);
assert.deepEqual(partial, []);
slow.next(['开始编译', '缺少入口文件']);
slow.complete();
assert.deepEqual(partial, [{ left: { status: '已读完', count: 2 }, right: { status: '读取失败' } }]);
