import assert from 'node:assert/strict';
import { BehaviorSubject, EMPTY, Subject, Subscription, VirtualTimeScheduler, defer, distinctUntilChanged, map, scan, share, switchMap, timer } from 'rxjs';

// 题 A
// A. Calibrate the empty case before opening. An indicator may reattach during a visit.
function cabinet(pressLate: boolean, speaker: 'sensor' | 'lamp' = 'sensor') {
  const clock = new VirtualTimeScheduler();
  const contact = new Subject<boolean>();
  const light = new BehaviorSubject(false);
  const owner = new Subscription();
  let reads = 0;
  const wire = defer(() => { reads++; return contact; }).pipe(distinctUntilChanged(), share());
  const sounds: number[] = [], lamps: string[] = [];
  owner.add(wire.subscribe(near => light.next(near)));
  if (speaker === 'sensor') owner.add(wire.subscribe(near => { if (near) sounds.push(clock.now()); }));
  const indicator = () => light.subscribe(near => {
    lamps.push(clock.now() + '/' + Number(near));
    if (speaker === 'lamp' && near) sounds.push(clock.now());
  });
  let display = indicator();
  clock.schedule(() => contact.next(true), 1);
  clock.schedule(() => display.unsubscribe(), 2);
  clock.schedule(() => contact.next(true), 3);
  clock.schedule(() => { display = indicator(); }, 4);
  if (pressLate) clock.schedule(() => { if (light.value) sounds.push(clock.now()); }, 5);
  clock.schedule(() => contact.next(false), 6);
  clock.schedule(() => contact.next(true), 8);
  clock.schedule(() => { display.unsubscribe(); owner.unsubscribe(); }, 9);
  clock.schedule(() => contact.next(false), 10);
  clock.schedule(() => contact.next(true), 11);
  clock.flush();
  assert.equal(contact.observed, false);
  return { reads, sounds, lamps };
}
assert.deepEqual(cabinet(false), { reads:1, sounds:[1,8], lamps:['0/0','1/1','4/1','6/0','8/1'] });
assert.deepEqual(cabinet(true).sounds, [1,5,8]);
assert.deepEqual(cabinet(false, 'lamp').sounds, [1,4,8]);
console.log('16 A · 灯牌重挂、接点重复和主动重听各有不同结果');

// 题 B
// B. An invalid reading breaks continuity; a new valid middle-band reading cannot restore it.
type Moisture = 'unknown' | 'dry' | 'wet';
type Sample = readonly [number, number | null];
const bed: Sample[] = [[0,55],[1,28],[2,33],[3,null],[4,34],[5,29],[6,30],[7,32],[9,28],[10,40],[11,29]];
function moisture(previous: Moisture, value: number | null): Moisture {
  if (value === null) return 'unknown';
  if (value < 30) return 'dry';
  if (value >= 40) return 'wet';
  return previous;
}
function watchBed(samples: Sample[], closeAt: number) {
  const clock = new VirtualTimeScheduler();
  const probe = new Subject<number | null>();
  const transitions: string[] = [], alarms: number[] = [];
  const monitoring = probe.pipe(
    scan(moisture, 'unknown' as Moisture), distinctUntilChanged(),
    switchMap(state => {
      transitions.push(clock.now() + '/' + state);
      return state === 'dry' ? timer(3, clock).pipe(map(() => clock.now())) : EMPTY;
    })
  ).subscribe(at => alarms.push(at));
  for (const [at, value] of samples) clock.schedule(() => probe.next(value), at);
  clock.schedule(() => monitoring.unsubscribe(), closeAt);
  clock.flush();
  return { transitions, alarms };
}
assert.deepEqual(watchBed(bed, 13), {
  transitions:['0/wet','1/dry','3/unknown','5/dry','10/wet','11/dry'], alarms:[8]
});
assert.deepEqual(watchBed(bed, 15).alarms, [8,14]);
assert.deepEqual(watchBed([[0,29],[2,40],[3,30]], 6).alarms, []);
assert.deepEqual(watchBed([[0,29],[2,39],[3,40]], 6).alarms, []);
assert.deepEqual(watchBed([[0,30],[1,29],[2,39]], 6).alarms, [4]);
assert.deepEqual(watchBed([[0,29],[2,28]], 4).alarms, [3]);
assert.deepEqual(watchBed([[0,29],[2,40]], 4).alarms, []);
console.log('16 B · 无效读数、阈值回差、同帧恢复和拆线各自有判据');
