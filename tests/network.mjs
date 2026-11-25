import assert from 'node:assert/strict';
import { once } from 'node:events';
import { WebSocket } from 'ws';
import { start } from '../app/server.mjs';

const app = await start(0,60);
try {
  const request = async (path, method = 'GET') => {
    const response = await fetch(app.url + path, {method});
    assert.ok(response.ok); return response.json();
  };
  const history = await request('/history?task=A');
  const socket = new WebSocket(app.url.replace('http:','ws:') + '/live?task=A&after=' + history.at(-1).id);
  try {
    const [data] = await once(socket,'message',{signal:AbortSignal.timeout(5000)});
    const row = JSON.parse(data);
    assert.equal(row.task,'A'); assert.ok(row.id > history.at(-1).id);
    assert.equal((await request('/stats')).active,1);
    const closed = once(socket,'close',{signal:AbortSignal.timeout(5000)});
    await request('/drop','POST');
    assert.equal((await closed)[0],1012);
    assert.deepEqual(await request('/notes?task=B&text=checkpoint','POST'),{task:'B',text:'checkpoint'});
    assert.equal((await request('/stats')).notes.length,1);
    const page = await fetch(app.url); assert.match(await page.text(),/日志面板/);
    assert.ok((await (await fetch(app.url+'/main.js')).text()).length > 1000);
    console.log('通过：本机 HTTP、WebSocket 补发与断线、备注、浏览器入口与打包');
  } finally { socket.terminate(); }
} finally { await app.close(); }
