import test from 'node:test';
import assert from 'node:assert/strict';
import { NEVER, interval, of, startWith } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { gameStates, initial, move, step, type State } from './game.ts';

test('左右边界和落点前换轨接星', () => {
  let state = initial(2);
  for (let i = 0; i < 19; i++) state = step(move(state, -1), 2);
  assert.equal(state.lane, 0);
  const ready = { ...state, stars: [{lane: 1, row: 19}] };
  const caught = step(move(ready, 1), 2);
  assert.deepEqual([caught.score, caught.misses, caught.stars], [1, 0, []]);
  assert.equal(move({...state, lane: 4}, 1).lane, 4);
});

test('第八拍暂停，恢复第二拍生成，同一份状态供两个观察者读取', () => {
  const time = new TestScheduler((a,b) => assert.deepEqual(a,b));
  time.run(({hot, cold, expectSubscriptions, flush}) => {
    const clock = cold('-t-t-t-t-t-t-t-t-t-t-t-t');
    let draws = 0;
    const states = gameStates(4, NEVER, hot('----------------p--------p'), of(0), clock, () => { draws++; return 3; });
    const seen: [number, State][] = [];
    const one = states.subscribe(s => seen.push([time.frame, s]));
    const two = states.subscribe();
    time.schedule(() => { one.unsubscribe(); two.unsubscribe(); }, 30);
    expectSubscriptions(clock.subscriptions).toBe(['^---------------!', '-------------------------^----!']);
    flush();
    assert.equal(draws, 1);
    assert.deepEqual(seen.filter(([f]) => f >= 15).map(([f,s]) => [f,s.tick,s.paused,s.stars.length]),
      [[15,8,false,0],[16,8,true,0],[25,8,false,0],[26,9,false,0],[28,10,false,1]]);
  });
});

test('重开两次都撤掉旧时钟，第三次漏接发出结算帧并释放时钟', () => {
  const time = new TestScheduler((a,b) => assert.deepEqual(a,b));
  time.run(({hot, cold, expectSubscriptions, flush}) => {
    const clock = cold('-x', {x:0});
    const subscriptions: number[] = [];
    const ends: number[] = [];
    const states = gameStates(4, hot('---r---r'), NEVER, of(0), interval(1,time), () => 0);
    const observed: State[] = [];
    states.subscribe(s => { observed.push(s); if (!s.tick) subscriptions.push(time.frame); if(s.misses === 3) ends.push(time.frame); });
    const probe = gameStates(4, hot('---r---r'), NEVER, of(0), clock, () => 0).subscribe();
    time.schedule(() => probe.unsubscribe(), 10);
    expectSubscriptions(clock.subscriptions).toBe(['^--!', '---^---!', '-------^--!']);
    flush();
    assert.deepEqual(subscriptions,[0,3,7]);
    assert.deepEqual(ends,[57]);
    assert.equal(observed.at(-1)?.tick,50);
  });
});

test('无输入的固定路线在第 30、40、50 拍漏接，结算当拍解除时钟订阅', () => {
  const time = new TestScheduler((a,b) => assert.deepEqual(a,b));
  time.run(({cold, expectSubscriptions, flush}) => {
    const clock = cold('-' + 'x'.repeat(70));
    const route = [0,4,1,3,2];
    const misses: number[] = [];
    let previous = 0;
    gameStates(5, NEVER, NEVER, of(0), clock, i => route[i % route.length]).subscribe(s => {
      if (s.misses > previous) misses.push(s.tick);
      previous = s.misses;
    });
    expectSubscriptions(clock.subscriptions).toBe('^' + '-'.repeat(49) + '!');
    flush();
    assert.deepEqual(misses, [30,40,50]);
  });
});

test('短按在两拍之间也移动，连续换向不推进落星', () => {
  const time = new TestScheduler((a,b) => assert.deepEqual(a,b));
  time.run(({cold,hot,flush}) => {
    const keys = hot('-r-s-l-r-s', {r:1,l:-1,s:0}).pipe(startWith(0));
    const frames: number[][] = [];
    gameStates(1, NEVER, NEVER, keys, cold('------------t|'), () => 0)
      .subscribe(s => frames.push([time.frame,s.lane,s.tick]));
    flush();
    assert.deepEqual(frames, [[0,2,0],[1,3,0],[5,2,0],[7,3,0],[12,3,1]]);
  });
});

test('长按连移，松开或暂停后撤掉连移，恢复后可重新按键', () => {
  const time = new TestScheduler((a,b) => assert.deepEqual(a,b));
  time.run(({hot,flush}) => {
    const keys = hot('1ms r 195ms s 40ms l 30ms s 150ms r 10ms s', {r:1,l:-1,s:0});
    const toggles = hot('250ms p 99ms p');
    const frames: number[][] = [];
    gameStates(4, NEVER, toggles, keys.pipe(startWith(0)), NEVER, () => 0)
      .subscribe(s => frames.push([time.frame,s.lane,Number(s.paused)]));
    flush();
    assert.deepEqual(frames, [[0,2,0],[1,3,0],[161,4,0],[238,3,0],[250,3,1],[350,3,0],[420,4,0]]);
  });
});
