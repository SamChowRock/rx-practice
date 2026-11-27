import assert from 'node:assert/strict';
import { Subject, concatMap, defer, finalize, from, map, of, takeWhile, toArray } from 'rxjs';
import { archiveA, pagesOf, type Page } from './archive.ts';

const requests: string[] = [];
const pages = pagesOf(cursor => {
  requests.push(cursor);
  return of(archiveA[cursor]);
});
assert.deepEqual(requests, []);
const sizes: number[] = [];
pages.pipe(map(page => page.rows.length)).subscribe(size => sizes.push(size));
assert.deepEqual(sizes, [1, 0, 1]);
assert.deepEqual(requests, ['start', 'k7', 'm2']);
console.log('请求顺序', requests, '每页行数', sizes);

const rows: string[] = [];
pages.pipe(concatMap(page => from(page.rows))).subscribe(row => rows.push(row));
assert.deepEqual(rows, ['开始编译', '缺少入口文件']);
console.log('逐行交付', rows);

const truncated: string[][] = [];
pages.pipe(takeWhile(page => page.next !== null), concatMap(page => from(page.rows)), toArray())
  .subscribe(list => truncated.push(list));
assert.deepEqual(truncated, [['开始编译']]);
console.log('用 takeWhile 挡末页', truncated[0]);

const waiting = new Subject<Page>();
const opened: string[] = [], closed: string[] = [];
const panel = pagesOf(cursor => defer(() => {
  opened.push(cursor);
  return (cursor === 'start' ? of(archiveA.start) : waiting).pipe(finalize(() => closed.push(cursor)));
})).subscribe();
panel.unsubscribe();
waiting.next(archiveA.k7);
assert.deepEqual(opened, ['start', 'k7']);
assert.ok(closed.includes('k7'));
console.log('关窗时已打开', opened, '未请求 m2');
