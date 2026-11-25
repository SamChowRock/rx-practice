import assert from 'node:assert/strict';
import { VirtualTimeScheduler, Subscription, from, scan } from 'rxjs';

// 题 A
// A. One tick is one quarter beat. An event at a tempo mark starts the new segment.
type KeyEvent = { tick: number; key: string; down: boolean };
type Tempo = { tick: number; beatMs: number };
const tempo = [{ tick: 0, beatMs: 400 }, { tick: 4, beatMs: 200 }];
const score: KeyEvent[] = [
  { tick: 0, key: 'C', down: true }, { tick: 2, key: 'G', down: true },
  { tick: 4, key: 'C', down: false }, { tick: 4, key: 'C', down: true },
  { tick: 6, key: 'G', down: false }, { tick: 8, key: 'C', down: false }
];
function elapsed(tick: number, marks: Tempo[]) {
  let ms = 0;
  for (let i = 0; i < marks.length && marks[i].tick < tick; i++) {
    const end = Math.min(tick, marks[i + 1]?.tick ?? tick);
    ms += (end - marks[i].tick) * marks[i].beatMs / 4;
  }
  return ms;
}
function perform(marks: Tempo[], stop?: number) {
  const clock = new VirtualTimeScheduler();
  const actions = new Subscription();
  const ringing = new Set<string>();
  const strikes: string[] = [];
  let executed = 0;
  function keyChange(key: string, down: boolean) {
    if (down) ringing.add(key);
    else ringing.delete(key);
    strikes.push([clock.now(), key, down ? 'on' : 'off'].join('/'));
  }
  for (const note of score) {
    actions.add(clock.schedule(() => {
      executed++;
      keyChange(note.key, note.down);
    }, elapsed(note.tick, marks)));
  }
  if (stop !== undefined) clock.schedule(() => {
    actions.unsubscribe();
    for (const key of ringing) keyChange(key, false);
  }, stop);
  clock.flush();
  return { strikes, executed, ringing: [...ringing] };
}
assert.deepEqual(perform(tempo), {
  strikes: ['0/C/on','200/G/on','400/C/off','400/C/on','500/G/off','600/C/off'],
  executed: 6, ringing: []
});
assert.deepEqual(perform(tempo, 450), {
  strikes: ['0/C/on','200/G/on','400/C/off','400/C/on','450/G/off','450/C/off'],
  executed: 4, ringing: []
});
assert.deepEqual(score.map(n => elapsed(n.tick, [{tick:0,beatMs:400}])), [0,200,400,400,600,800]);
console.log('14 A · 变速、同刻重奏与中止后的音门均已核对');

// 题 B
// B. Coordinates are grid intersections. Only a movement with the needle down sews an edge.
type Step = 'lower' | 'raise' | 'right' | 'up' | 'turn';
type Point = readonly [number, number];
type Head = { point: Point; down: boolean; turns: number };
type Edge = readonly [Point, Point];
const origin: Head = { point: [0,0], down: false, turns: 0 };
const pattern: Step[] = ['right','lower','up','turn','right','raise','right','lower','up','raise'];
function stitch(head: Head, step: Step): { head: Head; edge?: Edge } {
  if (step === 'lower' || step === 'raise') return { head: { ...head, down: step === 'lower' } };
  if (step === 'turn') return { head: { ...head, turns: (head.turns + 1) % 4 } };
  const direction = (head.turns + (step === 'up' ? 1 : 0)) % 4;
  const [dx, dy] = ([[1,0],[0,1],[-1,0],[0,-1]] as const)[direction];
  const point: Point = [head.point[0] + dx, head.point[1] + dy];
  return { head: { ...head, point }, edge: head.down ? [head.point, point] : undefined };
}
function runPattern(steps: Step[], initial = origin) {
  const edges: Edge[] = [];
  let last = initial;
  from(steps).pipe(scan((prior, step) => stitch(prior.head, step), { head: initial } as ReturnType<typeof stitch>))
    .subscribe(({ head, edge }) => { last = head; if (edge) edges.push(edge); });
  return { last, edges };
}
const full = runPattern(pattern);
assert.deepEqual(full.edges, [[[1,0],[1,1]], [[1,1],[1,2]], [[1,3],[0,3]]]);
assert.deepEqual(full.last, { point:[0,3], down:false, turns:1 });
const cut = runPattern(pattern.slice(0,4));
assert.deepEqual(cut.last, { point:[1,1], down:true, turns:1 });
assert.deepEqual(runPattern(pattern.slice(0,7)).last, { point:[1,3], down:false, turns:1 });
const tail = runPattern(pattern.slice(4), cut.last);
assert.deepEqual([...cut.edges, ...tail.edges], full.edges);
assert.deepEqual(tail.last, full.last);
assert.notDeepEqual(runPattern(pattern.slice(4), { ...origin, point:cut.last.point }).edges, tail.edges);
assert.deepEqual(runPattern(['lower','right','right']).edges.length, 2);
console.log('14 B · 针迹、空移、转向与从半幅续织均已核对');
