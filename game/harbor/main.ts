import { Subject, animationFrames, fromEvent } from 'rxjs';
import { gates, initial, offset, voyageStates, voyages, type Mode, type State } from './game.ts';
import { chart } from './chart.ts';

function el<T extends HTMLElement>(id: string) { return document.getElementById(id) as T; }
const mode = el<HTMLSelectElement>('mode'), action = el<HTMLButtonElement>('action');
const pause = el<HTMLButtonElement>('pause'), restart = el<HTMLButtonElement>('restart');
const canvas = el<HTMLCanvasElement>('chart'), draw = chart(canvas);
const start$ = new Subject<Mode>(), toggle$ = new Subject<void>(), hide$ = new Subject<void>(), flash$ = new Subject<void>();
const marks = { perfect: '正中', lit: '点亮', miss: '失误' };
const phases = { idle:'待出港', sailing:'航行中', paused:'已暂停', arrived:'已抵港', lost:'已迷航' };
const log = gates.map((_,i)=>{
  const item = document.createElement('li'), number = document.createElement('span'), result = document.createElement('span');
  number.className='num';number.textContent='0'+(i+1);item.append(number,result);el('log').append(item);return {item,result};
});
const best = { calm:0, swift:0 };
for(const key of ['calm','swift'] as const){
  try { best[key] = Math.max(0,Number(localStorage.getItem('rx-harbor-'+key))||0); } catch { /* Offline storage is optional. */ }
}
let state = initial('calm');
function write(id:string,value:string) { const node=el(id);if(node.textContent!==value)node.textContent=value; }
function render(s:State) {
  const ended = s.phase==='arrived'||s.phase==='lost', open = s.phase==='sailing'||s.phase==='paused';
  if(ended && s.score>best[s.mode]){
    best[s.mode]=s.score;try{localStorage.setItem('rx-harbor-'+s.mode,String(s.score));}catch { /* Keep this session's record. */ }
  }
  state=s;draw(s);
  write('score',String(s.score));write('misses',s.marks.filter(m=>m==='miss').length+'/3');write('phase',phases[s.phase]);
  write('record','本港最高 '+best[s.mode]+' 分');write('target',s.marks.length<5?'航标 0'+(s.marks.length+1):'东港');
  write('action',s.phase==='idle'?'启航':ended?'再航一程':s.phase==='paused'?'继续航行':'点灯');
  write('pause',s.phase==='paused'?'继续':'暂停');pause.disabled=!open;restart.disabled=!open;mode.disabled=open;
  const v=voyages[s.mode], delta=offset(s);
  el('zone').style.width=(100*v.window/1250)+'%';el('center').style.width=(100*v.perfect/1250)+'%';
  el('needle').style.left=Math.min(100,Math.max(0,50+delta/25))+'%';
  const freshFeedback=s.elapsed-s.feedbackAt<850;
  let feedback=s.message;
  if(s.phase==='sailing' && !freshFeedback){
    feedback=s.marks.length===5?'航标已通过，正在靠岸':Math.abs(delta)<=v.window?'船头进圈了，点灯！':delta>=-1250?'航标接近，准备点灯':'沿虚线驶向下一座航标';
  }
  if(s.phase==='paused')feedback='航程已暂停，继续后从这里出发。';
  if(s.phase==='lost')feedback='本次航行已结束，可以重新启航。';
  write('feedback',feedback);
  el('overlay').hidden=!ended&&s.phase!=='paused';
  if(!el('overlay').hidden){
    write('overlayTitle',s.phase==='paused'?'航行暂停':s.phase==='arrived'?'邮船抵达东港':'邮船迷航');
    write('overlayText',s.phase==='paused'?'点「继续」返回航线':s.marks.filter(m=>m!=='miss').length+' 座点亮　·　'+s.score+' 分');
  }
  log.forEach(({item,result},i)=>{
    const mark=s.marks[i], active=!ended && i===s.marks.length;
    const tag=mark??'';if(item.dataset.mark!==tag)item.dataset.mark=tag;
    if(active && !item.hasAttribute('aria-current'))item.setAttribute('aria-current','step');
    if(!active)item.removeAttribute('aria-current');
    const text=mark?marks[mark]:ended?'未到达':active&&open?'接近中':'待通过';
    if(result.textContent!==text)result.textContent=text;
  });
}
function start(){start$.next(mode.value as Mode);}
function primary(){
  if(state.phase==='sailing')flash$.next();
  else if(state.phase==='paused')toggle$.next();
  else start();
}
voyageStates(start$,toggle$,hide$,flash$,animationFrames(),()=>performance.now()).subscribe(render);
fromEvent(action,'click').subscribe(primary);
fromEvent(pause,'click').subscribe(()=>toggle$.next());
fromEvent(restart,'click').subscribe(start);
fromEvent(canvas,'click').subscribe(()=>{if(state.phase==='sailing')flash$.next();});
fromEvent(mode,'change').subscribe(()=>render(initial(mode.value as Mode)));
fromEvent<KeyboardEvent>(window,'keydown').subscribe(e=>{
  if(e.altKey||e.metaKey||e.ctrlKey||e.target===mode)return;
  if(e.code==='Space'){
    if(e.target instanceof HTMLButtonElement && e.target!==action)return;
    e.preventDefault();if(!e.repeat)primary();
  }
  if(e.code==='KeyP'&&!e.repeat&&(state.phase==='sailing'||state.phase==='paused')){e.preventDefault();toggle$.next();}
});
fromEvent(document,'visibilitychange').subscribe(()=>{if(document.hidden)hide$.next();});
fromEvent(window,'blur').subscribe(()=>hide$.next());
render(state);
