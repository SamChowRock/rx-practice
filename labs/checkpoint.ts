import assert from 'node:assert/strict';
import { Observable, Subject, BehaviorSubject, of, from, fromEvent, merge, concat, map, filter, scan, debounceTime, distinctUntilChanged, switchMap, combineLatest, startWith, shareReplay, bufferTime } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

const stage = Number(process.argv[2]);
if (!Number.isInteger(stage) || stage < 1 || stage > 9) throw Error('输入第 1 至 9 章的编号；网络阶段请运行 npm start');
type Entry = { task:string; level:string; text:string };
const clock = new TestScheduler((actual,expected) => assert.deepEqual(actual,expected));
clock.run(({cold,flush}) => {
  const channel = new BehaviorSubject('A');
  const term = new Subject<string>();
  const incoming = new EventTarget();
  let starts = 0, stops = 0;
  const baseline = (name:string) => new Observable<Entry>(out => {
    starts++;
    const resource = new Subject<void>();
    const line = (text:string,level='info'): Entry => ({task:name,level,text});
    const history = stage>=2 ? from([line('历史记录')]) : of(line('首行'));
    const push = stage>=2 ? fromEvent<CustomEvent<Entry>>(incoming,'line').pipe(map(event=>event.detail),filter(item=>item.task===name)) : resource.pipe(map(()=>line('心跳')));
    const sub = concat(history,push).subscribe(out);
    return () => { stops++; sub.unsubscribe(); resource.complete(); };
  });
  let events:Observable<Entry> = stage>=6 ? channel.pipe(distinctUntilChanged(),switchMap(baseline)) : baseline('A');
  if (stage>=3) events=events.pipe(filter(entry=>entry.level!=='heartbeat'),map(entry=>({...entry,text:entry.text.trim()})));
  const snapshot = stage>=4 ? events.pipe(scan((rows:Entry[],entry)=>rows.at(-1)?.task===entry.task?[...rows,entry]:[entry],[] as Entry[])) : events.pipe(map(entry=>[entry]));
  const query = stage>=5 ? term.pipe(debounceTime(2),distinctUntilChanged(),startWith('')) : of('');
  const preview = stage>=7 ? combineLatest([snapshot,query]).pipe(map(([rows,word])=>rows.filter(row=>row.text.includes(word)))) : snapshot;
  const shared = stage>=8 ? preview.pipe(shareReplay({bufferSize:1,refCount:true})) : preview;
  const screen = stage>=9 ? shared.pipe(bufferTime(3),filter(batch=>!!batch.length),map(batch=>batch[batch.length-1])) : shared;
  const mounted = screen.subscribe(rows=>console.log('帧',clock.frame,'画面',rows.map(row=>row.task+':'+row.text).join(' / ')));
  if (stage===5 || stage===6) mounted.add(query.subscribe(word=>console.log('帧',clock.frame,'候选筛选',word)));
  if (stage>=8) mounted.add(shared.subscribe(rows=>console.log('帧',clock.frame,'可见错误',rows.filter(row=>row.level==='error').length)));
  const actions=cold(' -abcd--e-f|',{a:()=>term.next('等'),b:()=>term.next('等待'),c:()=>send('A','heartbeat','tick'),d:()=>send('A','error',' 等待重试 '),e:()=>channel.next('B'),f:()=>send('B','error','等待响应')});
  function send(task:string,level:string,text:string) { incoming.dispatchEvent(new CustomEvent('line',{detail:{task,level,text}})); }
  actions.subscribe(action=>action()); clock.schedule(()=>mounted.unsubscribe(),13); flush();
  assert.equal(starts,stage>=6?2:1); assert.equal(stops,starts);
  console.log('启动/清理',starts,stops);
});
