import { BehaviorSubject, EMPTY, Subscription, bufferTime, catchError, combineLatest, concatMap, debounceTime, defer, distinctUntilChanged, filter, fromEvent, map, startWith, withLatestFrom } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';
import { webSocket } from 'rxjs/webSocket';
import { consoleModel, type Row } from './model.ts';

function el<T extends HTMLElement = HTMLElement>(id: string): T {
  const node = document.getElementById(id);
  if (!node) throw Error('未找到界面元素：' + id);
  return node as T;
}
function json<T>(url: string, method = 'GET') {
  return fromFetch<T>(url, {method, selector: async response => {
    if (!response.ok) throw Error('HTTP ' + response.status);
    return await response.json() as T;
  }});
}
const life = new Subscription();
const task = new BehaviorSubject('A');
const status = (message: string) => { el('status').textContent = message; };
const rows = consoleModel(task,
  name => json<Row[]>('/history?task=' + name),
  (name, after) => webSocket<Row>({url:'ws://' + location.host + '/live?task=' + name + '&after=' + after,
    openObserver:{next:() => status('推送已连接 · ' + name)}}), status);
life.add(fromEvent(el('task'),'change').subscribe(() => {
  status('切换中'); task.next(el<HTMLSelectElement>('task').value);
}));
const inputs = el<HTMLInputElement>('query');
const query = defer(() => fromEvent(inputs,'input').pipe(map(() => inputs.value.trim()), debounceTime(180), distinctUntilChanged(), startWith(inputs.value.trim())));
const view = combineLatest([rows, query]).pipe(map(([items, term]) => items.filter(row => (row.level + row.text).includes(term))));
function panel(id: string, mount: () => Subscription, clear: () => void) {
  const toggle = el<HTMLInputElement>(id);
  let active: Subscription | undefined;
  const update = () => {
    active?.unsubscribe();
    if (toggle.checked) active = mount(); else clear();
  };
  update(); life.add(fromEvent(toggle,'change').subscribe(update)); life.add(() => active?.unsubscribe());
}
panel('panel', () => {
  const group = new Subscription();
  const snapshot = new BehaviorSubject<Row[]>([]);
  group.add(view.subscribe(items => snapshot.next(items)));
  group.add(snapshot.pipe(bufferTime(150), filter(batch => batch.length > 0), map(batch => batch[batch.length-1])).subscribe(items => {
    el('rows').textContent = items.slice(-12).map(row => '#' + row.id + '  ' + row.task + '  ' + row.level + '   ' + row.text).join('\n') || '暂无匹配记录';
  }));
  group.add(fromEvent(el('export'),'click').pipe(withLatestFrom(snapshot)).subscribe(([,items]) => {
    const objectURL = URL.createObjectURL(new Blob([JSON.stringify(items,null,2)], {type:'application/json'}));
    const anchor = document.createElement('a'); anchor.href = objectURL; anchor.download = 'logs-' + task.value + '.json'; anchor.click();
    setTimeout(() => URL.revokeObjectURL(objectURL),1000);
  }));
  el<HTMLButtonElement>('export').disabled = false;
  return group;
}, () => { el('rows').textContent = '面板已关闭'; el<HTMLButtonElement>('export').disabled = true; });
panel('counter', () => rows.subscribe(items => { el('errors').textContent = String(items.filter(row => row.level === 'error').length); }),
  () => { el('errors').textContent = '已关闭'; });
const note = el<HTMLInputElement>('note');
life.add(fromEvent(el('save'),'click').pipe(map(() => ({task:task.value,text:note.value.trim()})),filter(item => item.text !== ''),
  concatMap(item => json<{text:string;task:string}>('/notes?' + new URLSearchParams(item),'POST').pipe(
    catchError(() => { el('saved').textContent = '未确认 · 请先检查服务端'; return EMPTY; })
  ))).subscribe(item => { el('saved').textContent = item.task + ' 已保存：' + item.text; }));
life.add(fromEvent(el('drop'),'click').pipe(concatMap(() => json('/drop','POST').pipe(catchError(() => EMPTY)))).subscribe());
life.add(fromEvent(el('inspect'),'click').pipe(concatMap(() => json<{opened:number;active:number;notes:unknown[]}>('/stats').pipe(
  catchError(() => { status('服务端不可达'); return EMPTY; })
))).subscribe(data => { el('stats').textContent = '活跃连接：' + data.active + '　累计建立：' + data.opened + '　已保存备注：' + data.notes.length; }));
window.addEventListener('pagehide', () => life.unsubscribe(), {once:true});
window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
