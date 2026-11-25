import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { once } from 'node:events';
import { build } from 'esbuild';
import { WebSocketServer, WebSocket } from 'ws';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

export async function start(port = 4173, cadence = 750) {
  const bundle = await build({ entryPoints: [fileURLToPath(new URL('./main.ts', import.meta.url))], bundle: true, write: false, format: 'esm' });
  const html = await readFile(new URL('./index.html', import.meta.url));
  const history = { A: [], B: [] };
  const notes = [];
  const sockets = new WebSocketServer({noServer:true, maxPayload:1024});
  let sequence = 0;
  let opened = 0;
  const sendRow = task => {
    const id = ++sequence;
    const phase = (Math.floor((id - 1) / 2) + (task === 'A' ? 0 : 2)) % 4;
    const row = { id, task, level: phase === 3 ? 'error' : 'info', text: ['收到任务','读取分片','写入索引','等待重试'][phase] };
    history[task].push(row);
    if (history[task].length > 200) history[task].shift();
    for (const peer of sockets.clients) if (peer.task === task && peer.readyState === WebSocket.OPEN) peer.send(JSON.stringify(row));
  };
  for (let count = 0; count < 4; count++) { sendRow('A'); sendRow('B'); }
  const fallback = (response, code, value) => {
    if (response.destroyed) return;
    response.writeHead(code, { 'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store' });
    response.end(JSON.stringify(value));
  };
  const server = createServer(async (request, response) => {
    try {
      if (request.headers.host !== '127.0.0.1:' + server.address().port) return fallback(response,403,'仅接受本地地址');
      const url = new URL(request.url, 'http://127.0.0.1');
      const task = url.searchParams.get('task');
      if (url.pathname === '/' || url.pathname === '/main.js') {
        const home = url.pathname === '/';
        response.writeHead(200, { 'Content-Type':home ? 'text/html; charset=utf-8' : 'text/javascript; charset=utf-8', 'Cache-Control':'no-store' });
        return response.end(home ? html : bundle.outputFiles[0].contents);
      }
      if (url.pathname === '/stats') return fallback(response,200,{opened, active:sockets.clients.size, notes});
      if (url.pathname === '/history' && Object.hasOwn(history, task)) {
        const snapshot = history[task].slice();
        const timer = setTimeout(() => fallback(response,200,snapshot), task === 'A' ? 350 : 40);
        response.once('close', () => clearTimeout(timer));
        return;
      }
      if (request.method !== 'POST') return fallback(response,404,'没有这条路由');
      if (request.headers.origin && request.headers.origin !== 'http://' + request.headers.host) return fallback(response,403,'跨域写入不允许');
      if (url.pathname === '/drop') {
        for (const peer of sockets.clients) peer.close(1012, '演练断线');
        return fallback(response,200,true);
      }
      if (url.pathname === '/notes' && Object.hasOwn(history, task)) {
        const note = url.searchParams.get('text')?.trim().slice(0,160);
        if (!note) return fallback(response,400,'备注不能为空');
        notes.push({task,text:note});
        if (notes.length > 200) notes.shift();
        return fallback(response,201,notes.at(-1));
      }
      fallback(response,404,'没有这条路由');
    } catch { fallback(response,400,'无效请求'); }
  });
  server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url, 'http://127.0.0.1');
    const task = url.searchParams.get('task');
    if (request.headers.host !== '127.0.0.1:' + server.address().port ||
       (request.headers.origin && request.headers.origin !== 'http://' + request.headers.host) ||
       url.pathname !== '/live' || !Object.hasOwn(history, task)) return socket.destroy();
    sockets.handleUpgrade(request, socket, head, peer => {
      opened++;
      peer.task = task;
      const after = Number(url.searchParams.get('after'));
      for (const row of history[task]) if (row.id > after) peer.send(JSON.stringify(row));
    });
  });
  server.listen(port,'127.0.0.1');
  await once(server,'listening');
  const ticker = setInterval(() => { sendRow('A'); sendRow('B'); }, cadence);
  return { url:'http://127.0.0.1:' + server.address().port, close: async () => {
    clearInterval(ticker);
    for (const peer of sockets.clients) peer.terminate();
    sockets.close();
    await new Promise(done => { server.close(done); server.closeAllConnections(); });
  }};
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const demo = await start(Number(process.env.PORT ?? 4173));
  console.log('实时日志控制台：' + demo.url + '  ·  Ctrl+C 结束');
}
