import {
  Observable, bufferCount, catchError, concatMap, defer, distinctUntilChanged,
  filter, from, groupBy, map, mergeMap, of, pairwise, scan, shareReplay,
  switchMap, take, takeUntil, timeout, withLatestFrom,
  type SchedulerLike,
} from 'rxjs';

// 01 每次开册都有自己的流水号
export function booklet(open: () => Iterable<string>) {
  return defer(() => from(open()).pipe(map((word, index) => (index + 1) + '/' + word)));
}

// 02 空读数也占一次采样的位置
export function rises(samples: Observable<number | null>) {
  return samples.pipe(pairwise(), filter(([a, b]) =>
    a !== null && b !== null && a < 10 && b >= 10
  ), map(([, b]) => b as number));
}

// 03 封蜡那一刻固定颜色
export function seals(press: Observable<void>, ink: Observable<{ color: string }>) {
  return press.pipe(withLatestFrom(ink), map(([, jar]) => jar.color));
}

// 04 收班冲出尾批，撤场丢掉尾批
export function trays(pieces: Observable<string>, abort: Observable<void>) {
  return pieces.pipe(bufferCount(3), takeUntil(abort));
}

// 05 翻页会收回前一页的校音任务
export function pronunciation(pages: Observable<string>, read: (page: string) => Observable<string>) {
  return pages.pipe(switchMap(read));
}

// 06 每一张谱都要得到印刷回执
export function proofs(sheets: Observable<string>, print: (sheet: string) => Observable<string>) {
  return sheets.pipe(concatMap(sheet =>
    defer(() => print(sheet)).pipe(catchError(() => of('废样/' + sheet)))
  ));
}

// 07 一台光谱仪供两块显示屏使用
export function spectrum(source: Observable<number>) {
  return source.pipe(shareReplay({ bufferSize: 1, refCount: true }));
}

// 08 到场和下一次报点有不同的时限
export function beacon(source: Observable<string>, clock: SchedulerLike) {
  return source.pipe(timeout({ first: 5, each: 3, scheduler: clock, with: () => of('失联') }));
}

// 09 左右两声部按曲号配齐
export type Voice = { piece: string; side: 'L' | 'R' };
export function duets(parts: Observable<Voice>) {
  return parts.pipe(
    groupBy(part => part.piece),
    mergeMap(group => group.pipe(
      map(part => part.side === 'L' ? 1 : 2),
      scan((mask, side) => mask | side, 0),
      filter(mask => mask === 3),
      take(1),
      map(() => group.key)
    ))
  );
}

// 10 每个录音棚拥有自己的队列；按钮决定何时取样
export function booths(
  rooms: Observable<string>, pedal: Observable<void>, pitch: Observable<number>,
  record: (room: string, hz: number) => Observable<string>, leave: Observable<void>
) {
  return rooms.pipe(
    distinctUntilChanged(),
    switchMap(room => pedal.pipe(
      withLatestFrom(pitch),
      concatMap(([, hz]) => record(room, hz))
    )),
    takeUntil(leave)
  );
}
