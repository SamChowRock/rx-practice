import assert from 'node:assert/strict';
import {
  Subject, VirtualTimeScheduler, buffer, debounceTime, filter, map, sample,
  scan, takeUntil, timestamp,
} from 'rxjs';

// 题 A
{
  const clock = new VirtualTimeScheduler();
  const wheel = new Subject<number>(), beat = new Subject<void>();
  const clamp = (n: number) => Math.max(0, Math.min(100, n));
  const move = (level: number, delta: number) => clamp(level + delta);
  const frames: [number, number][] = [], wrong: number[] = [];
  const level = wheel.pipe(scan(move, 50));
  level.pipe(sample(beat)).subscribe(n => frames.push([clock.now(), n]));
  wheel.pipe(sample(beat), scan(move, 50)).subscribe(n => wrong.push(n));
  for (const [at, delta] of [[1, 40], [3, 30], [7, -20], [14, -90], [17, 10], [25, 0]]) {
    clock.schedule(() => wheel.next(delta), at);
  }
  for (const at of [10, 20, 30, 40]) clock.schedule(() => beat.next(), at);
  clock.schedule(() => { wheel.complete(); beat.complete(); }, 42);
  clock.flush();
  assert.deepEqual(frames, [[10, 80], [20, 10], [30, 10]]);
  assert.deepEqual(wrong, [30, 40, 40]);
  assert.equal(clamp(50 + 40 + 30 - 20), 100);
  console.log('灯光谱', frames, '先按拍取动作', wrong);
}

// 题 B
{
  type Call = { at: number; pitch: string };
  type Phrase = [number, number, number, number, '确认' | '截断'];
  function annotate(stopAt: number, submitTail: boolean) {
    const clock = new VirtualTimeScheduler();
    const heard = new Subject<string>(), stop = new Subject<void>();
    const calls = heard.pipe(timestamp(clock), map(({ value, timestamp }) =>
      ({ at: timestamp, pitch: value })));
    const silence = calls.pipe(debounceTime(5, clock));
    const phrases: Phrase[] = [];
    let closing = false;
    const confirmed = calls.pipe(buffer(silence));
    const groups = submitTail ? calls.pipe(takeUntil(stop), buffer(silence))
      : confirmed.pipe(takeUntil(stop));
    groups.pipe(filter(group => group.length > 0)).subscribe((group: Call[]) => {
      phrases.push([clock.now(), group[0].at, group[group.length - 1].at,
        group.length, closing ? '截断' : '确认']);
    });
    for (const [at, pitch] of [[2, '低'], [4, '低'], [8, '高'], [16, '高'], [23, '低']] as const) {
      clock.schedule(() => heard.next(pitch), at);
    }
    clock.schedule(() => {
      closing = true;
      stop.next();
    }, stopAt);
    clock.flush();
    return phrases;
  }
  const firstTwo: Phrase[] = [[13, 2, 8, 3, '确认'], [21, 16, 16, 1, '确认']];
  assert.deepEqual(annotate(32, false), [...firstTwo, [28, 23, 23, 1, '确认']]);
  assert.deepEqual(annotate(26, false), firstTwo);
  assert.deepEqual(annotate(26, true), [...firstTwo, [26, 23, 23, 1, '截断']]);
  console.log('鸟鸣分句', annotate(32, false));
}
