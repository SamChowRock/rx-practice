import { EMPTY, concatMap, defer, expand, from, shareReplay, toArray } from 'rxjs';
import type { Observable } from 'rxjs';

export interface Page {
  rows: string[];
  next: string | null;
}

export const archiveA: Record<string, Page> = {
  start: { rows: ['开始编译'], next: 'k7' },
  k7:    { rows: [], next: 'm2' },
  m2:    { rows: ['缺少入口文件'], next: null },
};

export function pagesOf(readPage: (cursor: string) => Observable<Page>): Observable<Page> {
  return defer(() => readPage('start')).pipe(
    expand(page => page.next === null ? EMPTY : readPage(page.next), 1),
  );
}

export function archiveReader(readPage: (build: string, cursor: string) => Observable<Page>) {
  const archives = new Map<string, Observable<string[]>>();
  return function rowsFor(build: string): Observable<string[]> {
    const existing = archives.get(build);
    if (existing) return existing;

    const rows = pagesOf(cursor => readPage(build, cursor)).pipe(
      concatMap(page => from(page.rows)),
      toArray(),
      shareReplay({ bufferSize: 1, refCount: true }),
    );
    archives.set(build, rows);
    return rows;
  };
}
