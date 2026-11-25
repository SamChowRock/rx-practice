import assert from 'node:assert/strict';
import { BehaviorSubject, Subject, distinctUntilChanged, filter, map, scan, startWith, withLatestFrom } from 'rxjs';

// 题 A
type Gate = '松' | '石' | '桥';
type Punch = { type: 'punch'; bib: string; card: string; gate: Gate; at: number };
type RunnerEvent = Punch | { type: 'seal'; through: number };
const route: Gate[] = ['松', '石', '桥'];
function raceDesk() {
  const input = new Subject<RunnerEvent>();
  let through = -1;
  const punches = new Map<string, Punch>();
  const quarantine: string[] = [];
  const issued: string[] = [];
  const sub = input.subscribe(event => {
    if (event.type === 'punch') {
      if (punches.has(event.card) || quarantine.includes(event.card)) return;
      if (event.at <= through) {
        quarantine.push(event.card);
        return;
      }
      punches.set(event.card, event);
      return;
    }
    if (event.through <= through) return;
    through = event.through;
    const groups = new Map<string, Gate[]>();
    const records = [...punches.values()]
      .filter(punch => punch.at <= through)
      .sort((left, right) => left.at - right.at);
    for (const record of records) {
      const visits = groups.get(record.bib) ?? [];
      if (route[visits.length] === record.gate) visits.push(record.gate);
      groups.set(record.bib, visits);
    }
    for (const [bib, visits] of groups) {
      if (visits.length === route.length && !issued.includes(bib)) issued.push(bib);
    }
  });
  return { input, quarantine, issued, sub };
}
const punch=(card:string,gate:Gate,at:number,bib='红'):RunnerEvent=>({type:'punch',card,gate,at,bib});
const red=[punch('R1','松',10),punch('R2','石',20),punch('R3','桥',30)];
const desk=raceDesk();
for(const event of [red[2],red[0],red[0],punch('B1','松',15,'蓝')]) desk.input.next(event);
desk.input.next({type:'seal',through:19});
desk.input.next(red[1]);
assert.deepEqual([...desk.issued],[]);
desk.input.next({type:'seal',through:29});
assert.equal(desk.issued.length,0);
desk.input.next({type:'seal',through:30});
assert.deepEqual(desk.issued,['红']);
desk.input.next(red[1]);
desk.input.next(punch('BL','石',18,'蓝'));
desk.input.next(punch('BL','石',18,'蓝'));
desk.input.next(punch('B2','石',32,'蓝'));
desk.input.next(punch('B3','桥',33,'蓝'));
desk.input.next({type:'seal',through:33});
desk.input.next({type:'seal',through:31});
desk.input.next({type:'seal',through:33});
assert.deepEqual(desk.issued,['红','蓝']);
assert.deepEqual(desk.quarantine,['BL']);
desk.sub.unsubscribe();
for (const order of [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]]) {
  const permutation=raceDesk();
  for(const index of order) permutation.input.next(red[index]);
  permutation.input.next({type:'seal',through:30});
  assert.deepEqual(permutation.issued,['红']);
  permutation.sub.unsubscribe();
}
console.log('定向赛：确认线 19、29 不发章；30 只发红；33 再发蓝；破坏确认线的 BL 被隔离。');

// 题 B
type Pose = { arm: number; light: '明' | '暗' };
type Edit = { type: 'expose'; pose: Pose } | { type: 'undo' | 'redo' };
type Film = { past: Pose[]; future: Pose[] };
const initial: Film = { past: [], future: [] };
function advance(film: Film, event: Edit): Film {
  if (event.type === 'expose') {
    return { past: [...film.past, { ...event.pose }], future: [] };
  }
  if (event.type === 'undo') {
    const last = film.past.at(-1);
    if (!last) return film;
    return { past: film.past.slice(0, -1), future: [last, ...film.future] };
  }
  const next = film.future[0];
  if (!next) return film;
  return { past: [...film.past, next], future: film.future.slice(1) };
}
const rig = new BehaviorSubject<Pose>({ arm: 0, light: '明' });
const shutter = new Subject<void>();
const cuts = new Subject<Edit>();
const film = new BehaviorSubject<Film>(initial);
const exposures = shutter.pipe(
  withLatestFrom(rig),
  map(([, pose]): Edit => ({ type: 'expose', pose }))
).subscribe(event => cuts.next(event));
const editor = cuts.pipe(
  scan(advance, initial),
  startWith(initial),
  distinctUntilChanged()
).subscribe(film);
const recollect=()=>film.value.past.map(pose=>pose.arm);
const noFilm=film.value;
cuts.next({type:'undo'});
assert.equal(film.value,noFilm);
rig.next({arm:10,light:'暗'}); shutter.next(); shutter.next();
assert.deepEqual(recollect(),[10,10]);
rig.next({arm:20,light:'明'}); shutter.next();
const original=film.value;
cuts.next({type:'undo'}); cuts.next({type:'undo'}); cuts.next({type:'redo'});
assert.deepEqual(recollect(),[10,10]);
rig.next({arm:40,light:'暗'});
assert.equal(film.value.future[0].arm,20);
shutter.next(); cuts.next({type:'redo'});
assert.deepEqual(recollect(),[10,10,40]);
assert.deepEqual(original.past.map(pose=>pose.arm),[10,10,20]);
const exportsRequested=new Subject<void>(), exported:number[][]=[];
const exporter=exportsRequested.pipe(withLatestFrom(film),filter(([,state])=>state.past.length>0)).subscribe(([,state])=>exported.push(state.past.map(pose=>pose.arm)));
cuts.next({type:'undo'}); exportsRequested.next(); cuts.next({type:'redo'});
assert.deepEqual(exported,[[10,10]]);
assert.deepEqual(recollect(),[10,10,40]);
exposures.unsubscribe(); editor.unsubscribe(); exporter.unsubscribe();

const dial = new BehaviorSubject(0);
const pedal = new Subject<void>();
const reversed: number[] = [];
const reversedWire = dial.pipe(withLatestFrom(pedal)).subscribe(([angle]) => reversed.push(angle));
dial.next(10);
pedal.next();
assert.deepEqual(reversed, []);
dial.next(20);
assert.deepEqual(reversed, [20]);
reversedWire.unsubscribe();
console.log('逐格皮影：两张相同姿态仍是两帧；试摆不清重做，曝光才分叉；一次导出只锁定当时的底片。');
