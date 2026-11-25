import assert from 'node:assert/strict';
import { Subject, VirtualTimeScheduler, defer, from, map, mergeMap, switchMap, tap, timer, toArray } from 'rxjs';

// 题 A
// A. Rooms here are interchangeable booths. A group starts only with its whole allocation.
type Group = { name: string; booths: number; duration: number };
const groups: Group[] = [
  { name:'alto', booths:2, duration:5 }, { name:'bass', booths:2, duration:2 },
  { name:'solo', booths:1, duration:1 }, { name:'tenor', booths:2, duration:3 }
];
function rehearse(withdrawBass: boolean) {
  const clock = new VirtualTimeScheduler();
  const arrivals = new Subject<Group>();
  const waiting: Group[] = [];
  const starts: string[] = [], ends: string[] = [];
  let free = 3, peak = 0;
  function seat() {
    while (waiting.length && waiting[0].booths <= free) {
      const group = waiting.shift()!;
      free -= group.booths;
      peak = Math.max(peak, 3 - free);
      starts.push(group.name + '@' + clock.now());
      clock.schedule(() => {
        free += group.booths;
        ends.push(group.name + '@' + clock.now());
        seat();
      }, group.duration);
    }
  }
  arrivals.subscribe(group => { waiting.push(group); seat(); });
  groups.forEach((group, at) => clock.schedule(() => arrivals.next(group), at));
  if (withdrawBass) clock.schedule(() => {
    const index = waiting.findIndex(group => group.name === 'bass');
    assert.ok(index >= 0);
    waiting.splice(index, 1);
    seat();
  }, 4);
  clock.flush();
  assert.equal(free, 3);
  assert.equal(waiting.length, 0);
  return { starts, ends, peak };
}
assert.deepEqual(rehearse(false), {
  starts:['alto@0','bass@5','solo@5','tenor@7'],
  ends:['alto@5','solo@6','bass@7','tenor@10'], peak:3
});
assert.deepEqual(rehearse(true), {
  starts:['alto@0','solo@4','tenor@5'],
  ends:['alto@5','solo@5','tenor@8'], peak:3
});
console.log('15 A · 三间音舱的整组分配、等待顺序和撤队后补位均已核对');

// 反例：限制活跃组数，在男低到场的一瞬就已经用掉四间。
const wrongClock = new VirtualTimeScheduler();
const arrivals = new Subject<Group>();
let occupied = 0;
const useAtStart: number[] = [];
arrivals.pipe(mergeMap(group => {
  occupied += group.booths;
  useAtStart.push(occupied);
  return timer(group.duration, wrongClock).pipe(tap(() => { occupied -= group.booths; }));
}, 3)).subscribe();
groups.slice(0, 2).forEach((group, at) => wrongClock.schedule(() => arrivals.next(group), at));
wrongClock.flush();
assert.deepEqual(useAtStart, [2, 4]);

// 题 B
// B. A teaching font has additive advances and no kerning. Widths are in half-point units.
type Face = 'roman' | 'wide';
const widths = { roman:{ A:7, B:5 }, wide:{ A:9, B:6 } };
type Glyph = keyof typeof widths.roman;
const word: Glyph[] = ['A','B','A'];
function compose(swapFace: boolean) {
  const clock = new VirtualTimeScheduler();
  const faces = new Subject<Face>();
  const starts: string[] = [], measured: string[] = [], pages: string[] = [];
  faces.pipe(switchMap(face => from(new Set(word)).pipe(
    mergeMap(glyph => defer(() => {
      starts.push(face + glyph + '@' + clock.now());
      return timer(glyph === 'A' ? 4 : 1, clock).pipe(map(() => {
        measured.push(face + glyph + '@' + clock.now());
        return [glyph, widths[face][glyph]] as const;
      }));
    }), 2),
    toArray(),
    map(entries => {
      const table = new Map(entries);
      let advance = 0;
      const stops = word.map(glyph => Math.round((advance += table.get(glyph)!) / 2));
      return face + ':' + stops.join('/') + '@' + clock.now();
    })
  ))).subscribe(page => pages.push(page));
  clock.schedule(() => faces.next('roman'), 0);
  if (swapFace) clock.schedule(() => faces.next('wide'), 2);
  clock.schedule(() => faces.complete(), 3);
  clock.flush();
  return { starts, measured, pages };
}
assert.deepEqual(compose(false), {
  starts:['romanA@0','romanB@0'], measured:['romanB@1','romanA@4'], pages:['roman:4/6/10@4']
});
assert.deepEqual(compose(true), {
  starts:['romanA@0','romanB@0','wideA@2','wideB@2'],
  measured:['romanB@1','wideB@3','wideA@6'], pages:['wide:5/8/12@6']
});
console.log('15 B · 字形只量一次、换字样撤掉旧测量、字位按原文累计');
let roundedTotal = 0;
const roundedSeparately = word.map(glyph => roundedTotal += Math.round(widths.roman[glyph] / 2));
assert.deepEqual(roundedSeparately, [4, 7, 11]);
