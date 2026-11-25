import { Observable, catchError, concat, defer, distinctUntilChanged, EMPTY, from, repeat, retry, scan, shareReplay, startWith, switchMap, tap, timer } from 'rxjs';

export interface Row { id: number; task: string; level: 'info' | 'error'; text: string }

export function consoleModel(
  selection: Observable<string>,
  history: (task: string) => Observable<Row[]>,
  live: (task: string, after: number) => Observable<Row>,
  report: (message: string) => void = () => {},
  wait = 600
): Observable<Row[]> {
  return selection.pipe(distinctUntilChanged(), switchMap(task => defer(() => {
    let cursor = 0;
    const resume = () => { report('连接中断 · 等待恢复'); return timer(wait); };
    return defer(() => history(task)).pipe(retry({ count: 2, delay: resume }),
      switchMap(rows => concat(from(rows), defer(() => live(task, cursor)).pipe(
        retry({ delay: resume }), repeat({ delay: resume })
      ))),
      scan((rows: Row[], row) => {
        if (row.task !== task || row.id <= cursor) return rows;
        cursor = row.id;
        return [...rows.slice(-199), row];
      }, [] as Row[]),
      startWith([] as Row[]),
      tap(rows => { if (rows.length) report('已接收 #' + cursor); }),
      catchError(() => { report('历史读取失败 · 切到其他任务再切回，或关闭两个面板后重开'); return EMPTY; })
    );
  })), shareReplay({ bufferSize: 1, refCount: true }));
}
