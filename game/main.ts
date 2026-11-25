import {
  distinctUntilChanged, filter, fromEvent, interval, map, merge, scan,
  shareReplay, startWith, switchMap, take, tap,
} from 'rxjs';
import { FLOOR, LANES, finished, gameStates, initial, type Stage, type State } from './game.ts';

function el<T extends HTMLElement = HTMLElement>(id: string) {
  return document.getElementById(id) as T;
}

const selection = el<HTMLSelectElement>('stage');
const requested = Number(new URL(location.href).searchParams.get('stage'));
const stage: Stage = [1,2,3,4,5].includes(requested) ? requested as Stage : 4;
selection.value = String(stage);
fromEvent(selection, 'change').subscribe(() => {
  const url = new URL(location.href);
  url.searchParams.set('stage', selection.value);
  location.assign(url.href);
});

const field = el('field');
const start = el<HTMLButtonElement>('start');
const pause = el<HTMLButtonElement>('pause');
const titles = ['移动接收器', '接住第一颗星', '接住尽可能多的星星', '接住尽可能多的星星', '固定轨道验收'];
const rules = [
  '按住方向键，让接收器沿五条轨道移动。',
  '一颗星从左起第二轨落下。接住或漏掉它，本局即结束。',
  '每 10 拍生成一颗星，接住得 1 分，漏掉 3 颗结算。',
  '接住得 1 分，漏掉 3 颗结算。按空格暂停或继续。',
  '落星轨道按 1、5、2、4、3 循环。保持居中，预测第三次漏接的节拍。',
];
el('headline').textContent = titles[stage - 1];
el('hint').textContent = rules[stage - 1];
el('instructions').textContent = '轻点 ← → 或屏幕方向按钮换一轨，按住连续移动。' + rules[stage - 1];
pause.hidden = stage < 4;

const down$ = fromEvent<KeyboardEvent>(window, 'keydown');
const up$ = fromEvent<KeyboardEvent>(window, 'keyup');
type Press = {id: string; delta: number; down: boolean};
function arrow(event: KeyboardEvent) {
  return (event.code === 'ArrowLeft' || event.code === 'ArrowRight') && event.target !== selection;
}
function keyboard(event: KeyboardEvent): Press {
  return {id: event.code, delta: event.code === 'ArrowLeft' ? -1 : 1, down: event.type === 'keydown'};
}
function pointer(button: HTMLElement, delta: number) {
  const press$ = fromEvent<PointerEvent>(button, 'pointerdown').pipe(tap(e => {
    e.preventDefault();
    button.setPointerCapture(e.pointerId);
    field.focus();
  }));
  const release$ = merge(...['pointerup','pointercancel','lostpointercapture'].map(name => fromEvent<PointerEvent>(button,name)));
  return merge(press$,release$).pipe(map((e): Press => ({
    id: 'pointer' + e.pointerId, delta, down: e.type === 'pointerdown',
  })));
}
const direction$ = merge(
  down$.pipe(filter(arrow), tap(e => e.preventDefault()), filter(e => !e.repeat), map(keyboard)),
  up$.pipe(filter(arrow), map(keyboard)),
  pointer(el('left'), -1), pointer(el('right'), 1),
  fromEvent(window, 'blur').pipe(map(() => null)),
).pipe(
  scan((held, press) => {
    const next = new Map(held);
    if (!press) next.clear();
    else if (press.down) next.set(press.id, press.delta);
    else next.delete(press.id);
    return next;
  }, new Map<string, number>()),
  map(held => [...held.values()].at(-1) ?? 0),
  startWith(0), distinctUntilChanged(),
);

const toggle$ = merge(
  fromEvent(pause,'click').pipe(tap(() => field.focus())),
  down$.pipe(filter(e => e.code === 'Space' && (e.target === field || e.target === document.body)),
    tap(e => e.preventDefault()), filter(e => !e.repeat)),
);
const start$ = fromEvent(start,'click').pipe(tap(() => field.focus()));
const route = [0,4,1,3,2];
const states$ = start$.pipe(
  take(1),
  switchMap(() => gameStates(stage, start$, toggle$, direction$, interval(140),
    index => stage === 5 ? route[index % route.length] : Math.floor(Math.random() * LANES))),
  shareReplay({bufferSize: 1, refCount: true}),
);

function paint(state: State) {
  const center = (lane: number) => ((lane + 0.5) * 100 / LANES) + '%';
  el('sled').style.transform = 'translateX(' + state.lane * 100 + '%)';
  el('stars').replaceChildren(...state.stars.map(star => {
    const node = document.createElement('span');
    node.className = 'star';
    node.textContent = '★';
    node.style.left = center(star.lane);
    node.style.top = (6 + 86 * star.row / FLOOR) + '%';
    return node;
  }));
}
paint(initial(stage));
states$.subscribe(paint);
states$.subscribe(state => {
  const over = finished(state, stage);
  const phase = over ? '已结算' : state.paused ? '已暂停' : '进行中';
  if (el('phase').textContent !== phase) el('phase').textContent = phase;
  el('score').textContent = String(state.score);
  el('misses').textContent = String(state.misses);
  el('tick').textContent = String(state.tick);
  start.textContent = over ? '再来一局' : '重新开始';
  pause.disabled = over;
  pause.textContent = state.paused ? '继续' : '暂停';
  el('cover').hidden = !(over || state.paused);
  if (over || state.paused) {
    el('headline').textContent = over ? '本局结束' : '已暂停';
    el('hint').textContent = over ? '得分 ' + state.score + ' · 漏接 ' + state.misses : '按空格或点「继续」回到这一拍。';
  }
});
