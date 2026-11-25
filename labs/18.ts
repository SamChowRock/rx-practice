import assert from 'node:assert/strict';
import {
  BehaviorSubject, EMPTY, Observable, Subject, defer, finalize, of, throwError,
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import * as reference from './18/answers.ts';
import { practice } from './18/practice.ts';

const [selection = 'all', mode = 'reference', ...extra] = process.argv.slice(2);
if (!/^(all|0[1-9]|10)$/.test(selection) || !['reference', 'mine'].includes(mode) || extra.length) {
  throw Error('用法：node labs/18.ts <01 至 10 或 all> <reference 或 mine>');
}

function exercise<K extends keyof typeof reference>(
  number: string, name: K, verify: (solve: (typeof reference)[K]) => void
) {
  if (selection !== 'all' && number !== selection) return;
  const solve = mode === 'mine' ? practice[name] : reference[name];
  if (!solve) throw Error('请先在 labs/18/practice.ts 实现 ' + name);
  verify(solve);
  console.log(number + ' ' + name + ' 通过（' + mode + '）');
}

function clock() {
  return new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
}

exercise('01', 'booklet', solve => {
  let opened = 0;
  const pages = solve(() => ['甲', String(++opened)]);
  assert.equal(opened, 0);
  for (const edition of ['1', '2']) {
    const lines: string[] = [];
    pages.subscribe(value => lines.push(value));
    assert.deepEqual(lines, ['1/甲', '2/' + edition]);
  }
  assert.equal(opened, 2);
});

exercise('02', 'rises', solve => {
  const edges: number[] = [];
  solve(of(12, 9, null, 11, 8, 10, 12, 7, 13)).subscribe(value => edges.push(value));
  assert.deepEqual(edges, [10, 13]);
});

exercise('03', 'seals', solve => {
  const press = new Subject<void>(), ink = new Subject<{ color: string }>();
  const jar = { color: '青' }, saved: string[] = [];
  solve(press, ink).subscribe(value => saved.push(value));
  press.next();
  ink.next(jar);
  press.next(); press.next();
  jar.color = '朱'; ink.next(jar);
  assert.deepEqual(saved, ['青', '青']);
  press.next();
  assert.deepEqual(saved, ['青', '青', '朱']);
});

exercise('04', 'trays', solve => {
  for (const ending of ['自然', '撤场', '仅关闭撤场按钮']) {
    const pieces = new Subject<string>(), abort = new Subject<void>();
    const trays: string[][] = [];
    let completed = false;
    solve(pieces, abort).subscribe({ next: tray => trays.push(tray), complete: () => { completed = true; } });
    for (const piece of '甲乙丙丁') pieces.next(piece);
    assert.deepEqual(trays, [['甲', '乙', '丙']]);
    if (ending === '撤场') {
      abort.next();
      assert.equal(completed, true);
      assert.deepEqual(trays, [['甲', '乙', '丙']]);
    }
    if (ending === '仅关闭撤场按钮') { abort.complete(); assert.equal(completed, false); }
    pieces.complete();
    assert.equal(completed, true);
    assert.deepEqual(trays, ending === '撤场' ? [['甲', '乙', '丙']] : [['甲', '乙', '丙'], ['丁']]);
  }
});

exercise('05', 'pronunciation', solve => {
  clock().run(({ cold, hot, expectObservable, expectSubscriptions }) => {
    const old = cold('-----o|'), current = cold('--n|');
    const page = hot('-a-b-----|');
    expectObservable(solve(page, id => id === 'a' ? old : current)).toBe('-----n---|');
    expectSubscriptions(old.subscriptions).toBe('-^-!');
    expectSubscriptions(current.subscriptions).toBe('---^--!');
  });
});

exercise('06', 'proofs', solve => {
  const sheets = new Subject<string>(), first = new Subject<string>();
  const started: string[] = [], receipts: string[] = [];
  solve(sheets, name => {
    started.push(name);
    if (name === 'A') return first;
    if (name === 'B') return throwError(() => Error('卡纸'));
    if (name === 'C') throw Error('装版失败');
    return of('样张/' + name);
  }).subscribe(value => receipts.push(value));
  for (const sheet of 'ABCD') sheets.next(sheet);
  sheets.complete();
  first.next('样张/A');
  assert.deepEqual(started, ['A']);
  first.complete();
  assert.deepEqual(started, ['A', 'B', 'C', 'D']);
  assert.deepEqual(receipts, ['样张/A', '废样/B', '废样/C', '样张/D']);
});

exercise('07', 'spectrum', solve => {
  const sensor = new Subject<number>();
  let opened = 0, closed = 0;
  const view = solve(defer(() => { opened++; return sensor.pipe(finalize(() => closed++)); }));
  assert.equal(opened, 0);
  const a: number[] = [], b: number[] = [], c: number[] = [];
  const first = view.subscribe(n => a.push(n));
  sensor.next(6);
  const second = view.subscribe(n => b.push(n));
  assert.deepEqual(b, [6]);
  first.unsubscribe();
  assert.equal(closed, 0);
  sensor.next(7);
  second.unsubscribe();
  assert.deepEqual([opened, closed], [1, 1]);
  const third = view.subscribe(n => c.push(n));
  assert.deepEqual(c, []);
  sensor.next(8);
  third.unsubscribe();
  assert.deepEqual([a, b, c, opened, closed], [[6], [6, 7], [8], 2, 2]);
});

exercise('08', 'beacon', solve => {
  const scheduler = clock();
  scheduler.run(({ cold, expectObservable, expectSubscriptions }) => {
    const silent = cold<string>('--------');
    const late = cold('----a-b----c|');
    expectObservable(solve(silent, scheduler)).toBe('-----(x|)', { x: '失联' });
    expectSubscriptions(silent.subscriptions).toBe('^----!');
    expectObservable(solve(late, scheduler)).toBe('----a-b--(x|)', { a: 'a', b: 'b', x: '失联' });
    expectSubscriptions(late.subscriptions).toBe('^--------!');
    expectObservable(solve(EMPTY, scheduler)).toBe('|');
  });
});

exercise('09', 'duets', solve => {
  const parts = new Subject<reference.Voice>(), ready: string[] = [];
  let done = false;
  solve(parts).subscribe({ next: name => ready.push(name), complete: () => { done = true; } });
  for (const [piece, side, expected] of [
    ['青', 'L', []], ['金', 'R', []], ['青', 'L', []], ['灰', 'R', []],
    ['金', 'L', ['金']], ['青', 'R', ['金', '青']],
    ['金', 'L', ['金', '青']], ['金', 'R', ['金', '青']],
  ] as const) {
    parts.next({ piece, side });
    assert.deepEqual(ready, expected);
  }
  assert.deepEqual(ready, ['金', '青']);
  parts.complete();
  assert.equal(done, true);
});

exercise('10', 'booths', solve => {
  const rooms = new Subject<string>(), pedal = new Subject<void>(), leave = new Subject<void>();
  const pitch = new BehaviorSubject(440);
  const started: string[] = [], stopped: string[] = [], clips: string[] = [];
  const takes: Subject<string>[] = [];
  let ended = false;
  function record(room: string, hz: number): Observable<string> {
    const tag = room + '/' + hz;
    started.push(tag);
    const take = new Subject<string>();
    takes.push(take);
    return take.pipe(finalize(() => stopped.push(tag)));
  }
  solve(rooms, pedal, pitch, record, leave).subscribe({
    next: clip => clips.push(clip), complete: () => { ended = true; },
  });
  pedal.next();
  rooms.next('东'); pedal.next(); pitch.next(442); pedal.next(); pitch.next(444);
  rooms.next('东');
  assert.deepEqual(started, ['东/440']);
  takes[0].next('一'); takes[0].complete();
  assert.deepEqual(started, ['东/440', '东/442']);
  pedal.next();
  rooms.next('西');
  assert.deepEqual(stopped, ['东/440', '东/442']);
  takes[1].next('旧'); pedal.next(); pedal.next();
  assert.deepEqual(started, ['东/440', '东/442', '西/444']);
  takes[2].next('二');
  leave.next();
  rooms.next('北'); pedal.next(); takes[2].next('迟');
  assert.deepEqual(clips, ['一', '二']);
  assert.deepEqual(stopped, started);
  assert.equal(started.length, 3);
  assert.equal(ended, true);
});
