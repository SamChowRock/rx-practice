import assert from 'node:assert/strict';
import { EMPTY, catchError, defer, filter, forkJoin, map, of, shareReplay, take, timeout } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

// 题 A
function font() {
  const clock = new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
  clock.run(({ cold, expectObservable, expectSubscriptions }) => {
    const data = cold('---p----r|', { p: 'bytes', r: 'face' });
    const ready = data.pipe(filter(value => value === 'face'), shareReplay({ bufferSize: 1, refCount: true }));
    const choose = () => ready.pipe(timeout({ first: 5, with: () => of('local') }), take(1));
    const owner = ready.subscribe();
    expectObservable(choose()).toBe('-----(l|)', { l: 'local' });
    const progressFirst = cold('---p----r|', { p: 'bytes', r: 'face' });
    const premature = progressFirst.pipe(timeout({ first: 5, with: () => of('local') }));
    expectObservable(premature).toBe('---p----r|', { p: 'bytes', r: 'face' });
    expectObservable(choose(), '----------^').toBe('----------(r|)', { r: 'face' });
    expectSubscriptions(data.subscriptions).toBe('^--------!');
    clock.schedule(() => owner.unsubscribe(), 11);
    const earlyFace = cold('---pr----|', { p: 'bytes', r: 'face' });
    const earlyPage = earlyFace.pipe(
      filter(value => value === 'face'), timeout({ first: 5, with: () => of('local') }), take(1)
    );
    expectObservable(earlyPage).toBe('----(r|)', { r: 'face' });
  });
}

// 题 B
function puzzle() {
  type Piece = 'arch' | 'wheel' | 'rim';
  type Result = { ok: true; teeth: number } | { ok: false; piece: Piece };
  for (const variation of ['拱片坏', '轮片光滑', '边片坏'] as const) {
    const clock = new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
    clock.run(({ cold, expectObservable, expectSubscriptions }) => {
      const arch = variation === '边片坏' ? cold('--a|', { a: 1 })
        : cold<number>('--#', {}, Error('轮廓损坏'));
      const wheel = cold('----w|', { w: variation === '轮片光滑' ? 0 : 3 });
      const rim = variation === '边片坏' ? cold<number>('------#', {}, Error('边片破损'))
        : cold('------r|', { r: 2 });
      const starts: Piece[] = [];
      const inspect = (piece: Piece, source: typeof wheel) => defer(() => {
        starts.push(piece);
        return source.pipe(
          map((teeth): Result => ({ ok: true, teeth })),
          catchError(() => of<Result>({ ok: false, piece }))
        );
      });
      const verdict = forkJoin({
        arch: inspect('arch', arch), wheel: inspect('wheel', wheel), rim: inspect('rim', rim)
      }).pipe(map(({ arch, wheel, rim }) => ({
        bridge: arch.ok && rim.ok ? '可试拼' : '待补片',
        gear: wheel.ok && rim.ok ? wheel.teeth + rim.teeth : '待补片'
      })));
      if (variation === '边片坏') {
        expectObservable(verdict).toBe('------(v|)', { v: { bridge: '待补片', gear: '待补片' } });
        expectSubscriptions(rim.subscriptions).toBe('^-----!');
        expectSubscriptions(arch.subscriptions).toBe('^--!');
      } else {
        expectObservable(verdict).toBe('-------(v|)', { v: { bridge: '待补片', gear: variation === '轮片光滑' ? 2 : 5 } });
        expectSubscriptions(rim.subscriptions).toBe('^------!');
        expectSubscriptions(arch.subscriptions).toBe('^-!');
      }
      expectSubscriptions(wheel.subscriptions).toBe('^----!');
      clock.schedule(() => assert.deepEqual(starts, ['arch', 'wheel', 'rim']), 8);
      if (variation !== '拱片坏') return;
      const lateWheel = cold('----w|', { w: 3 });
      const brokenArch = cold<number>('--#', {}, Error('轮廓损坏'));
      const outside = forkJoin({ arch: brokenArch, wheel: lateWheel }).pipe(catchError(() => of('整桌失败')));
      expectObservable(outside).toBe('--(x|)', { x: '整桌失败' });
      expectSubscriptions(lateWheel.subscriptions).toBe('^-!');
      const silentArch = cold<number>('--#').pipe(catchError(() => EMPTY));
      const spareRim = cold('------r|', { r: 2 });
      expectObservable(forkJoin({ arch: silentArch, rim: spareRim })).toBe('--|');
      expectSubscriptions(spareRim.subscriptions).toBe('^-!');
    });
  }
}

const choice = process.argv[2] ?? 'both';
assert.ok(['A', 'B', 'both'].includes(choice));
if (choice !== 'B') { font(); console.log('A 首屏使用本地字形，下一页复用已就绪字形通过'); }
if (choice !== 'A') { puzzle(); console.log('B 拱片失败不打断轮片与边片，两处试拼分别裁决通过'); }
