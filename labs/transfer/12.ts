import assert from 'node:assert/strict';
import { NEVER, animationFrames, distinctUntilChanged, map, switchMap, take, timer } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

const virtual = () => new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));

// 题 A
function rehearse(skipSecondFrame: boolean) {
  const onsets = [0, 3, 3, 6, 10];
  const clock = virtual();
  clock.run(({ animate, flush }) => {
    const frames = skipSecondFrame ? [2, 9, 13] : [2, 5, 9, 13];
    animate(skipSecondFrame ? '--x------x---x' : '--x--x---x---x');
    const played: number[][] = [];
    onsets.forEach((at, id) => clock.schedule(() => played.push([clock.now(), id]), at));
    const pictures: number[][] = [];
    let next = 0;
    animationFrames().pipe(take(frames.length)).subscribe(({ elapsed }) => {
      const crossed: number[] = [];
      while (next < onsets.length && onsets[next] <= elapsed) crossed.push(next++);
      pictures.push([elapsed, ...crossed]);
    });
    flush();
    assert.deepEqual(played, [[0, 0], [3, 1], [3, 2], [6, 3], [10, 4]]);
    assert.deepEqual(pictures, skipSecondFrame
      ? [[2, 0], [9, 1, 2, 3], [13, 4]]
      : [[2, 0], [5, 1, 2], [9, 3], [13, 4]]);
    assert.deepEqual(onsets.filter(t => frames.includes(t)), []);
    const soundDrivenByScreen = onsets.map(t => frames.find(frame => frame >= t));
    assert.deepEqual(soundDrivenByScreen, skipSecondFrame ? [2, 9, 9, 9, 13] : [2, 5, 5, 9, 13]);
  });
}

function score() {
  rehearse(false);
  rehearse(true);
}

// 题 B
type Side = '白' | '黑';
function hourglass(presses: number[]) {
  const clock = virtual();
  const history: [number, number, number, Side | '终局'][] = [];
  let white = 5, black = 5, last = 0;
  let active: Side = '白';
  let ended = false;
  function settle() {
    const elapsed = clock.now() - last;
    const moved = Math.min(elapsed, active === '白' ? white : black);
    white += active === '白' ? -moved : moved;
    black = 10 - white;
    last += moved;
    ended = white === 0 || black === 0;
  }
  function expire() {
    settle();
    assert.equal(clock.now(), last, '应在流空时响铃');
    history.push([last, white, black, '终局']);
  }
  clock.run(({ flush }) => {
    // 先登记按键，使相撞的按键有机会先执行，裁决仍须判超时。
    for (const time of presses) clock.schedule(() => {
      if (ended) return;
      alarm.unsubscribe();
      settle();
      if (!ended) active = active === '白' ? '黑' : '白';
      history.push([last, white, black, ended ? '终局' : active]);
      if (!ended) alarm = clock.schedule(expire, active === '白' ? white : black);
    }, time);
    let alarm = clock.schedule(expire, white);
    flush();
  });
  return history;
}

function match() {
  assert.deepEqual(hourglass([2, 6, 8]), [
    [2, 3, 7, '黑'], [6, 7, 3, '白'], [8, 5, 5, '黑'], [13, 10, 0, '终局']
  ]);
  assert.deepEqual(hourglass([5, 7]), [[5, 0, 10, '终局']]);
  assert.deepEqual(hourglass([]), [[5, 0, 10, '终局']]);
  assert.deepEqual(hourglass([2]), [[2, 3, 7, '黑'], [9, 10, 0, '终局']]);
  assert.deepEqual(hourglass([4]), [[4, 1, 9, '黑'], [13, 10, 0, '终局']]);
}

// 题 C
function dough() {
  virtual().run(({ hot, expectObservable, expectSubscriptions }) => {
    const readings = hot('-a-b-c-d-e------', { a: 25, b: 26, c: 29, d: 24, e: 27 });
    const eligible = readings.pipe(map(t => t >= 24 && t <= 27));
    const hold = (inside: boolean) => inside ? timer(5) : NEVER;
    expectObservable(eligible.pipe(distinctUntilChanged(), switchMap(hold), take(1)))
      .toBe('------------(r|)', { r: 0 });
    expectObservable(eligible.pipe(switchMap(hold), take(1)))
      .toBe('--------------(r|)', { r: 0 });
    expectSubscriptions(readings.subscriptions).toBe(['^-----------!', '^-------------!']);
  });
}

const stations: Record<string, () => void> = { A: score, B: match, C: dough };
const chosen = process.argv[2];
for (const name of chosen ? [chosen] : Object.keys(stations)) {
  assert.ok(Object.hasOwn(stations, name), '请选择 A、B 或 C');
  stations[name]();
  console.log('第 12 章工作坊', name, '通过');
}
