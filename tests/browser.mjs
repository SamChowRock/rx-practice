import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as pause } from 'node:timers/promises';
import { start } from '../app/server.mjs';

const app = await start(0,120);
const profile = await mkdtemp(join(tmpdir(),'rxjs-course-browser-'));
const browser = spawn(process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  ['--headless','--disable-gpu','--no-first-run','--disable-background-networking','--disable-component-update',
    '--remote-debugging-pipe','--user-data-dir='+profile,'about:blank'],{stdio:['ignore','ignore','ignore','pipe','pipe']});
const stopped = once(browser,'close').catch(() => {});
const pending = new Map();
let serial = 0, buffer = '';
browser.stdio[4].setEncoding('utf8');
browser.stdio[4].on('data',chunk => {
  buffer += chunk;
  let end;
  while ((end = buffer.indexOf('\0')) >= 0) {
    const message = JSON.parse(buffer.slice(0,end)); buffer = buffer.slice(end+1);
    if (pending.has(message.id)) pending.get(message.id)(message);
  }
});
async function command(method, params = {}, sessionId) {
  const id = ++serial;
  return new Promise((resolve,reject) => {
    const timeout = setTimeout(() => { pending.delete(id); reject(Error('浏览器命令超时：'+method)); },5000);
    pending.set(id,message => { clearTimeout(timeout); pending.delete(id); if (message.error) reject(Error(message.error.message)); else resolve(message.result); });
    browser.stdio[3].write(JSON.stringify({id,method,params,sessionId})+'\0');
  });
}
try {
  const {targetId} = await command('Target.createTarget',{url:'about:blank'});
  const {sessionId} = await command('Target.attachToTarget',{targetId,flatten:true});
  await command('Page.navigate',{url:app.url},sessionId);
  const evaluate = async expression => {
    const result = await command('Runtime.evaluate',{expression,returnByValue:true},sessionId);
    if (result.exceptionDetails) throw Error(result.exceptionDetails.exception?.description);
    return result.result.value;
  };
  const stats = async () => (await fetch(app.url+'/stats')).json();
  async function until(check,label) {
    for (let count=0;count<80;count++) { if (await check()) return; await pause(60); }
    throw Error('未观察到：'+label);
  }
  await until(async () => (await stats()).active===1,'双面板共用一个连接');
  await until(async () => (await evaluate("document.querySelector('#rows')?.textContent || ''")).includes('A'),'显示 A');
  await evaluate("document.querySelector('#task').value='B'; document.querySelector('#task').dispatchEvent(new Event('change'))");
  await until(async () => (await evaluate("document.querySelector('#rows').textContent")).includes('  B  '),'切 B');
  await evaluate("document.querySelector('#panel').click()");
  assert.equal((await stats()).active,1);
  await evaluate("document.querySelector('#counter').click()");
  await until(async () => (await stats()).active===0,'最后一个消费者离开');
  await evaluate("document.querySelector('#panel').click(); document.querySelector('#note').value='现场记录'; document.querySelector('#save').click()");
  await until(async () => (await stats()).notes.at(-1)?.task==='B','备注冻结任务');
  await until(async () => (await stats()).active===1,'面板重开');
  const before = (await stats()).opened;
  await evaluate("document.querySelector('#drop').click()");
  await until(async () => (await stats()).opened>before,'断线恢复');
  console.log('通过：浏览器点击与渲染、双订阅共享、最后退出、备注、断线恢复。');
} finally {
  browser.kill(); await stopped; await app.close();
  await rm(profile,{recursive:true,force:true,maxRetries:3,retryDelay:100});
}
