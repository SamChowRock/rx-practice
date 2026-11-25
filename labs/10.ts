import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { EventEmitter, once } from 'node:events';
import { createRequire } from 'node:module';
import { BehaviorSubject, Subject, combineLatest, defer, exhaustMap, firstValueFrom, from, fromEvent, share, shareReplay, switchMap, takeUntil, withLatestFrom } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';
import type { Observable } from 'rxjs';
import { filter as allow, lastValueFrom } from 'rxjs';
import type { AjaxConfig, AjaxDirection, AjaxRequest } from 'rxjs/ajax';
import { webSocket, type WebSocketSubjectConfig } from 'rxjs/webSocket';

const config: AjaxConfig = { url: '/history', includeDownloadProgress: true };
const direction: AjaxDirection = 'download';
const socketConfig: WebSocketSubjectConfig<number> = { url: 'ws://localhost/log', serializer: n => JSON.stringify(n) };
// @ts-expect-error AjaxRequest describes the normalized request and requires method, headers, etc.
const incomplete: AjaxRequest = config;
// @ts-expect-error progress direction has two literal values
const invalid: AjaxDirection = 'both';
// @ts-expect-error serializer must return a wire payload
const badSocket: WebSocketSubjectConfig<number> = { ...socketConfig, serializer: n => n };

// Signals record actual server events without guessing network timing.
function signal() {
  let fire!: () => void;
  const ready = new Promise<void>(resolve => { fire = resolve; });
  return { ready, fire };
}
const slow = signal(), closed = signal(), release = signal();
const pendingPromise = signal(), releasePromise = signal();
const sent: string[] = [];
const browserMode = process.argv.includes('--browser');
const browserCode = browserMode ? (await (await import('esbuild')).build({
  stdin: {
    resolveDir: process.cwd(),
    contents: `
      import { combineLatest, exhaustMap, fromEvent, map, of, startWith, withLatestFrom } from 'rxjs';
      import { fromFetch } from 'rxjs/fetch';
      import { ajax } from 'rxjs/ajax';
      const field = document.querySelector('input');
      const output = document.querySelector('output');
      const log = line => document.querySelector('pre').append(line + '\\n');
      const term = fromEvent(field, 'input').pipe(map(() => field.value), startWith(field.value));
      const view = combineLatest([of(['info A', 'error B']), term]).pipe(
        map(([rows, term]) => rows.filter(row => row.includes(term)))
      );
      view.subscribe(rows => output.textContent = rows.join(', '));
      fromEvent(document.querySelector('#export'), 'click').pipe(
        withLatestFrom(term),
        exhaustMap(([, term]) => fromFetch('/export/' + encodeURIComponent(term), { selector: r => r.text() }))
      ).subscribe(log);
      fromEvent(document.querySelector('#refresh'), 'click').pipe(exhaustMap(() =>
        ajax({ url: '/refresh', responseType: 'text', includeDownloadProgress: true }).pipe(
          map(e => e.type + '：' + e.loaded + ' 字节')
        )
      )).subscribe(log);
    `
  }, write: false, bundle: true, platform: 'browser'
})).outputFiles[0].text : '';
const server = createServer(async (req, res) => {
  const path = req.url!;
  sent.push(path);
  if (browserMode && path === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end('<title>第 10 章网络实验</title><label>筛选 <input></label><button id="export">导出</button><button id="refresh">刷新</button><p><output></output></p><pre></pre><script src="/lab.js"></script>');
    return;
  }
  if (browserMode && path === '/lab.js') {
    res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8' });
    res.end(browserCode);
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  if (path === '/slow') {
    res.write('partial');
    res.once('close', closed.fire);
    slow.fire();
    return;
  }
  if (path === '/refresh') {
    if (browserMode) {
      res.write('first ');
      await new Promise(resolve => setTimeout(resolve, 1000));
    } else await release.ready;
  }
  if (path === '/promise') {
    pendingPromise.fire();
    await releasePromise.ready;
  }
  res.end(path);
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
try {
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const base = 'http://127.0.0.1:' + address.port;
  if (browserMode) {
    console.log('打开', base, '修改筛选词、点击导出，或连点刷新。Ctrl+C 结束。');
    await once(process, 'SIGINT');
  } else await runAssertions(base);
} finally {
  server.closeAllConnections();
  server.close();
}

async function runAssertions(base: string) {
  const read = (path: string) => fromFetch(base + path, { selector: r => r.text() });
  const eager = fetch(base + '/eager');
  const eagerStream = from(eager);
  const lazy = defer(() => from(fetch(base + '/lazy')));
  await eager;
  assert.deepEqual(sent, ['/eager']);
  assert.equal(await (await firstValueFrom(eagerStream)).text(), '/eager');
  assert.equal(await (await firstValueFrom(lazy)).text(), '/lazy');
  console.log('创建 Promise 已发出 /eager；订阅工厂后才发出 /lazy。');

  const choose = new Subject<Promise<string>>();
  const selected = firstValueFrom(choose.pipe(switchMap(p => from(p))));
  const oldBody = fetch(base + '/promise').then(r => r.text());
  choose.next(oldBody);
  await pendingPromise.ready;
  choose.next(Promise.resolve('B'));
  assert.equal(await selected, 'B');
  releasePromise.fire();
  assert.equal(await oldBody, '/promise');
  console.log('旧 Promise 已退订，视图只收到 B，旧响应仍被完整读出。');

  const task = new Subject<string>();
  const bodyStarted = signal();
  const rows = firstValueFrom(task.pipe(switchMap(path => fromFetch(base + path, {
    selector: response => {
      if (path === '/slow') bodyStarted.fire();
      return response.text();
    }
  }))));
  task.next('/slow');
  await Promise.all([slow.ready, bodyStarted.ready]);
  task.next('/new');
  assert.equal(await rows, '/new');
  await closed.ready;
  console.log('旧响应头已到，切任务只收到 /new；服务端观察到旧响应连接关闭。');

  const filter = new BehaviorSubject('all');
  const latest = new BehaviorSubject(3);
  const view: string[] = [];
  combineLatest([latest, filter]).subscribe(v => view.push(v.join('/')));
  const clicks = new EventTarget();
  const exported = firstValueFrom(fromEvent(clicks, 'export').pipe(withLatestFrom(filter), switchMap(([, f]) => read('/export/' + f))));
  filter.next('error');
  assert.deepEqual(view, ['3/all', '3/error']);
  assert.ok(!sent.some(p => p.startsWith('/export')));
  clicks.dispatchEvent(new Event('export'));
  assert.equal(await exported, '/export/error');
  assert.equal(sent.filter(p => p.startsWith('/export')).length, 1);
  const results: string[] = [];
  const done = [signal(), signal()];
  const refresh = fromEvent(clicks, 'refresh').pipe(exhaustMap(() => read('/refresh'))).subscribe(v => {
    results.push(v);
    done[results.length - 1].fire();
  });
  const click = () => clicks.dispatchEvent(new Event('refresh'));
  click(); click(); click(); release.fire();
  await done[0].ready;
  click();
  await done[1].ready;
  refresh.unsubscribe();
  assert.equal(sent.filter(p => p === '/refresh').length, 2);
  console.log('改筛选刷新视图，导出只发一次；三连点加响应后再点共两次刷新。');

  // ws provides the loopback server. Node 24 supplies the native client.
  type Peer = EventEmitter & { send(text: string): void };
  const { WebSocketServer } = createRequire(import.meta.url)('ws') as {
    WebSocketServer: new (options: { server: typeof server }) => EventEmitter & { close(): void };
  };
  const hub = new WebSocketServer({ server });
  try {
    for (const replay of [false, true]) {
      let opens = 0, closes = 0;
      const received = signal(), disconnected = signal();
      const onConnection = (peer: Peer) => {
        opens++;
        peer.once('close', () => { closes++; disconnected.fire(); });
        peer.send('1');
      };
      hub.on('connection', onConnection);
      const channel: Observable<number> = defer(() => webSocket<number>({ ...socketConfig, url: base.replace('http:', 'ws:') }));
      const stream = replay ? channel.pipe(shareReplay({ bufferSize: 1, refCount: true })) : channel.pipe(share());
      const leave = new Subject<void>();
      stream.pipe(takeUntil(leave)).subscribe(received.fire);
      await received.ready;
      const late: number[] = [];
      const b = stream.subscribe(v => late.push(v));
      leave.next();
      assert.deepEqual([opens, closes, b.closed], [1, 0, false]);
      b.unsubscribe();
      await disconnected.ready;
      assert.deepEqual([opens, closes, late], [1, 1, replay ? [1] : []]);
      hub.off('connection', onConnection);
      console.log('回环 WebSocket 连接一次、最后退出时关闭；晚到者收到', late);
    }
  } finally {
    hub.close();
  }
  await checkBodyReaders(base);
  checkExportDuringSwitch();
}

async function checkBodyReaders(base: string) {
  const raw = await fetch(base + '/raw-body');
  assert.equal(await raw.text(), '/raw-body');
  await assert.rejects(raw.text(), TypeError);
  assert.equal(sent.filter(path => path === '/raw-body').length, 1);

  const parsed = fromFetch(base + '/parsed-body', { selector: r => r.text() }).pipe(
    shareReplay({ bufferSize: 1, refCount: true })
  );
  assert.equal(await lastValueFrom(parsed), '/parsed-body');
  assert.equal(await lastValueFrom(parsed), '/parsed-body');
  assert.equal(sent.filter(path => path === '/parsed-body').length, 1);
  console.log('同一 Response 第二次读体失败；先读成文本，完成后再订阅也只请求一次。');
}

function checkExportDuringSwitch() {
  type View = { task: string; ready: boolean };
  const state = new BehaviorSubject<View>({ task: 'A', ready: true });
  const click = new Subject<void>();
  const stale: string[] = [], accepted: string[] = [];
  click.pipe(withLatestFrom(state.pipe(allow(view => view.ready))))
    .subscribe(([, view]) => stale.push(view.task));
  click.pipe(withLatestFrom(state), allow(([, view]) => view.ready))
    .subscribe(([, view]) => accepted.push(view.task));
  state.next({ task: 'B', ready: false });
  click.next();
  assert.deepEqual(stale, ['A']);
  assert.deepEqual(accepted, []);
  state.next({ task: 'B', ready: true });
  click.next();
  assert.deepEqual(accepted, ['B']);
  click.complete();
  state.complete();
  console.log('切换中先筛状态会取到旧 A；先取样再检查只放行就绪的 B。');
}
