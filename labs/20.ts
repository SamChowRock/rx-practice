import assert from 'node:assert/strict';
import { Subject, defer, finalize, of, shareReplay } from 'rxjs';
import type { Observable } from 'rxjs';
import { archiveA, archiveReader, pagesOf, type Page } from './archive.ts';

const lastPage: Page[] = [];
const pages = pagesOf(cursor => of(archiveA[cursor]))
  .pipe(shareReplay({ bufferSize: 1, refCount: true }));
pages.subscribe();
pages.subscribe(page => lastPage.push(page));
assert.deepEqual(lastPage.map(page => page.rows), [['缺少入口文件']]);
console.log('只回放一页', lastPage[0].rows);

const tail = new Subject<Page>();
const requests: string[] = [], released: string[] = [];
const rowsFor = archiveReader((build, cursor) => defer(() => {
  const key = build + '/' + cursor;
  requests.push(key);
  let response: Observable<Page>;
  if (build === 'B') response = of({ rows: [], next: null });
  else if (cursor === 'm2') response = tail;
  else response = of(archiveA[cursor]);
  return response.pipe(finalize(() => released.push(key)));
}));
const lists: string[][] = [], files: string[][] = [], late: string[][] = [];
const list = rowsFor('A').subscribe(rows => lists.push(rows));
rowsFor('A').subscribe(rows => files.push(rows));
list.unsubscribe();
assert.ok(!released.includes('A/m2'));
tail.next(archiveA.m2);
tail.complete();
rowsFor('A').subscribe(rows => late.push(rows));
assert.deepEqual(lists, []);
assert.deepEqual(files, [['开始编译', '缺少入口文件']]);
assert.deepEqual(late, files);
assert.deepEqual(requests, ['A/start', 'A/k7', 'A/m2']);
console.log('导出和晚开的列表', files[0], '请求', [...requests]);

rowsFor('B').subscribe(rows => assert.deepEqual(rows, []));
assert.equal(requests.at(-1), 'B/start');

const opened: string[] = [], stopped: string[] = [];
const pending = archiveReader((build, cursor) => defer(() => {
  opened.push(build + '/' + cursor);
  return new Subject<Page>().pipe(finalize(() => stopped.push(cursor)));
}));
const first = pending('A').subscribe(), second = pending('A').subscribe();
first.unsubscribe();
assert.deepEqual(stopped, []);
second.unsubscribe();
assert.deepEqual(stopped, ['start']);
pending('A').subscribe().unsubscribe();
assert.deepEqual(opened, ['A/start', 'A/start']);
console.log('读到一半全部离开，再打开', opened);
