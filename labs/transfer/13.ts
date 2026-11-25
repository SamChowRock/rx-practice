import assert from 'node:assert/strict';
import * as rx from 'rxjs';
import * as old from 'rxjs/operators';
import { TestScheduler } from 'rxjs/testing';

const virtual = () => new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));

// 题 A
function palette() {
  for (const mode of ['旧', '快照', '延迟读取'] as const) {
    const clock = virtual();
    let accesses = 0;
    const ink = { density: 1 };
    const swatches = {
      ink, width: 2,
      get serial() { return ++accesses; }
    };
    const seen: unknown[][] = [];
    clock.run(({ flush }) => {
      const pairs = mode === '旧' ? rx.pairs(swatches)
        : mode === '快照' ? rx.from(Object.entries(swatches))
        : rx.defer(() => rx.from(Object.entries(swatches)));
      assert.equal(accesses, mode === '延迟读取' ? 0 : 1);
      clock.schedule(() => {
        ink.density = 3;
        swatches.ink = { density: 9 };
        swatches.width = 4;
      }, 2);
      for (const at of [4, 7]) clock.schedule(() => pairs.subscribe(([key, value]) => {
        seen.push([clock.now(), key, typeof value === 'object' ? value.density : value]);
      }), at);
      flush();
    });
    const lazy = mode === '延迟读取';
    assert.equal(accesses, lazy ? 2 : 1);
    assert.deepEqual(seen, [
      [4, 'ink', lazy ? 9 : 3], [4, 'width', lazy ? 4 : 2], [4, 'serial', 1],
      [7, 'ink', lazy ? 9 : 3], [7, 'width', lazy ? 4 : 2], [7, 'serial', lazy ? 2 : 1]
    ]);
  }
  const stamp = { shape: '圆' };
  for (const stream of [
    rx.of(1, 2).pipe(old.mapTo(stamp)), rx.of(1, 2).pipe(rx.map(() => stamp))
  ]) {
    const marks: typeof stamp[] = [];
    stream.subscribe(mark => marks.push(mark));
    assert.equal(marks.length, 2);
    assert.equal(marks[0], stamp);
    assert.equal(marks[1], stamp);
  }
}

// 题 B
function audiobook() {
  for (const mode of ['旧', '逐章继续', '整本补救'] as const) {
    for (const damaged of [false, true]) virtual().run(({ cold, expectObservable, expectSubscriptions }) => {
      const first = cold(damaged ? '-u-#' : '-u-|');
      const second = cold('--v|');
      const third = cold('-w|');
      const reading = mode === '旧' ? first.pipe(old.onErrorResumeNext(second, third))
        : mode === '逐章继续' ? first.pipe(rx.onErrorResumeNextWith(second, third))
        : rx.concat(first, second, third).pipe(rx.catchError(() => rx.EMPTY));
      const cut = damaged && mode === '整本补救';
      expectObservable(reading).toBe(cut ? '-u-|' : '-u---v-w|');
      expectSubscriptions(first.subscriptions).toBe('^--!');
      expectSubscriptions(second.subscriptions).toBe(cut ? [] : '---^--!');
      expectSubscriptions(third.subscriptions).toBe(cut ? [] : '------^-!');
    });
  }
  for (const mode of ['旧', '首段期限', '逐段限时'] as const) {
    const clock = virtual();
    clock.run(({ cold, expectObservable, expectSubscriptions }) => {
      const chapter = cold('-p---q|');
      const fallback = rx.of('f');
      const output = mode === '旧' ? chapter.pipe(old.timeoutWith(new Date(6), fallback, clock))
        : mode === '首段期限' ? chapter.pipe(rx.timeout({ first: new Date(6), with: () => fallback }))
        : chapter.pipe(rx.timeout({ each: 2, with: () => fallback }));
      expectObservable(output, '----^').toBe(mode === '逐段限时' ? '-----p-(f|)' : '-----p---q|');
      expectSubscriptions(chapter.subscriptions).toBe(mode === '逐段限时' ? '----^--!' : '----^-----!');
    });
  }
}

// 题 C
function kiln() {
  for (const mode of ['旧', '忙时拒收', '切换', '排队'] as const) {
    const clock = virtual();
    clock.run(({ hot, cold, expectObservable, expectSubscriptions, flush }) => {
      const glaze = cold('---h|');
      const trial = cold('-i|');
      const bisque = cold('--j|');
      const starts: number[] = [];
      const trialOnDemand = rx.defer(() => {
        starts.push(clock.now());
        return trial;
      });
      const buttons = hot('a-b--c|', { a: glaze, b: trialOnDemand, c: bisque });
      const fired = mode === '旧' ? buttons.pipe(old.exhaust())
        : mode === '忙时拒收' ? buttons.pipe(rx.exhaustAll())
        : mode === '切换' ? buttons.pipe(rx.switchAll()) : buttons.pipe(rx.concatAll());
      if (mode === '切换') {
        expectObservable(fired).toBe('---i---j|');
        expectSubscriptions(glaze.subscriptions).toBe('^-!');
        expectSubscriptions(trial.subscriptions).toBe('--^-!');
        expectSubscriptions(bisque.subscriptions).toBe('-----^--!');
      } else if (mode === '排队') {
        expectObservable(fired).toBe('---h-i--j|');
        expectSubscriptions(glaze.subscriptions).toBe('^---!');
        expectSubscriptions(trial.subscriptions).toBe('----^-!');
        expectSubscriptions(bisque.subscriptions).toBe('------^--!');
      } else {
        expectObservable(fired).toBe('---h---j|');
        expectSubscriptions(glaze.subscriptions).toBe('^---!');
        expectSubscriptions(trial.subscriptions).toBe([]);
        expectSubscriptions(bisque.subscriptions).toBe('-----^--!');
      }
      flush();
      assert.deepEqual(starts, mode === '切换' ? [2] : mode === '排队' ? [4] : []);
    });
  }
}

const stations: Record<string, () => void> = { A: palette, B: audiobook, C: kiln };
const chosen = process.argv[2];
for (const name of chosen ? [chosen] : Object.keys(stations)) {
  assert.ok(Object.hasOwn(stations, name), '请选择 A、B 或 C');
  stations[name]();
  console.log('第 13 章工作坊', name, '通过');
}
