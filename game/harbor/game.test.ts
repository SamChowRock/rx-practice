import assert from 'node:assert/strict';
import test from 'node:test';
import { NEVER } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { advance, gates, initial, voyageStates, voyages, type State } from './game.ts';

const leave = () => advance(initial('calm'), { type: 'resume', now: 0 });
const first = gates[0] * voyages.calm.duration;

test('早到、有效灯区、标心、自动漏标的边界', () => {
  const flash = (now: number) => advance(leave(), { type: 'flash', now });
  assert.deepEqual(flash(200).marks, []);
  assert.deepEqual(flash(first - 700).marks, ['miss']);
  assert.equal(flash(first - voyages.calm.window).score, 100);
  assert.equal(flash(first + voyages.calm.perfect).score, 150);
  assert.deepEqual(flash(first + voyages.calm.window).marks, ['lit']);
  const late = flash(first + voyages.calm.window + 1);
  assert.deepEqual(late.marks, ['miss']);
  const hit = flash(first);
  assert.deepEqual(advance(hit, { type: 'flash', now: first + 50 }).marks, ['perfect']);
});

test('两种难度都可满分抵港，错过第三座时停止', () => {
  for (const mode of ['calm', 'swift'] as const) {
    let s = advance(initial(mode), { type: 'resume', now: 0 });
    for (const gate of gates) s = advance(s, { type: 'flash', now: gate * voyages[mode].duration });
    s = advance(s, { type: 'frame', now: voyages[mode].duration });
    assert.equal(s.phase, 'arrived');
    assert.equal(s.score, 750);
  }
  const loss = advance(leave(), { type: 'frame', now: voyages.calm.duration });
  assert.deepEqual(loss.marks, ['miss', 'miss', 'miss']);
  assert.equal(loss.phase, 'lost');
  assert.equal(advance(loss, { type: 'flash', now: 26000 }), loss);
});

test('暂停和切到后台冻结航程，恢复后仅累计在场时间', () => {
  const t = new TestScheduler((a,b)=>assert.deepEqual(a,b));
  t.run(({hot,cold,expectSubscriptions,flush})=>{
    const frames = cold('--x----x');
    const states: State[] = [];
    const sub = voyageStates(hot('s', {s:'calm' as const}), hot('----------p'), hot('----h-h'), hot('------f'),
      frames, ()=>t.now()).subscribe(s=>states.push(s));
    t.schedule(()=>sub.unsubscribe(),16);
    expectSubscriptions(frames.subscriptions).toBe(['^---!', '----------^-----!']);
    flush();
    assert.deepEqual(states.map(s=>[s.phase,s.elapsed]),[
      ['sailing',0],['sailing',2],['paused',4],['sailing',4],['sailing',6],
    ]);
  });
});

test('重新启航撤掉旧时钟，迷航时发出最后一帧并清理', () => {
  const t = new TestScheduler((a,b)=>assert.deepEqual(a,b));
  t.run(({hot,cold,expectSubscriptions,flush})=>{
    const frames = cold('--x-----x');
    const states: State[] = [];
    voyageStates(hot('s---s', {s:'calm' as const}), NEVER, NEVER, NEVER, frames,
      ()=>t.now()*2000).subscribe(s=>states.push(s));
    expectSubscriptions(frames.subscriptions).toBe(['^---!', '----^-------!']);
    flush();
    assert.equal(states.filter(s=>s.elapsed===0).length,2);
    assert.equal(states.at(-1)?.phase,'lost');
  });
});
