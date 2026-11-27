import assert from 'node:assert/strict';
import { EMPTY, catchError, filter, map, merge, race, take, throwIfEmpty } from 'rxjs';
import type { Observable } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

type ReportFile = { kind: 'file'; reportId: string; text: string };
type Transfer = { kind: 'headers' } | ReportFile;
const headers: Transfer = { kind: 'headers' };
const good: Transfer = { kind: 'file', reportId: 'R7', text: 'A:2 B:0' };
const stale: Transfer = { kind: 'file', reportId: 'R6', text: '旧报告' };

function filesFrom(source: Observable<Transfer>, reportId: string) {
  return source.pipe(
    filter((event): event is ReportFile =>
      event.kind === 'file' && event.reportId === reportId),
    map(file => file.text),
    catchError(() => EMPTY),
  );
}

function download(a: Observable<Transfer>, b: Observable<Transfer>) {
  return merge(filesFrom(a, 'R7'), filesFrom(b, 'R7')).pipe(
    take(1), throwIfEmpty(() => Error('两个下载点都没交付 R7')),
  );
}

const clock = new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
clock.run(({ cold, expectObservable, expectSubscriptions }) => {
  const values = { h: headers, f: good, s: stale };
  const a = cold<Transfer>('-h------f|', values);
  const b = cold<Transfer>('---h-f|', values);
  expectObservable(race(a, b)).toBe('-h------f|', values);
  expectSubscriptions(b.subscriptions).toBe('^!');

  const slow = cold<Transfer>('-h------f|', values);
  const fast = cold<Transfer>('---h-f|', values);
  expectObservable(download(slow, fast)).toBe('-----(x|)', { x: good.text });
  expectSubscriptions(slow.subscriptions).toBe('^----!');
  expectSubscriptions(fast.subscriptions).toBe('^----!');

  const fallback = cold<Transfer>('---h-f|', values);
  const broken = cold<Transfer>('--#');
  expectObservable(race(filesFrom(broken, 'R7'), filesFrom(fallback, 'R7')))
    .toBe('--|');
  expectObservable(download(broken, fallback)).toBe('-----(x|)', { x: good.text });
  expectObservable(download(cold('-s|', values), fallback)).toBe('-----(x|)', { x: good.text });
  expectObservable(download(broken, cold('---s|', values)))
    .toBe('----#', undefined, Error('两个下载点都没交付 R7'));
});
console.log('抢到响应头：第 8 帧拿到文件；先收 R7 正文：第 5 帧拿到并退出两路。');
console.log('单路失败或返回 R6 时保留另一路；两路都无 R7 时报告失败。');
