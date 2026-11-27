import assert from 'node:assert/strict';
import { VirtualTimeScheduler, defer, finalize, map, of, switchMap, takeUntil, timer } from 'rxjs';
import { pollJob, type JobStatus } from './report-job.ts';

const clock = new VirtualTimeScheduler();
const replies: JobStatus[] = [
  { phase: 'writing', written: 1 },
  { phase: 'writing', written: 2 },
  { phase: 'ready', written: 2 },
];
const created: string[] = [], requests: string[] = [], screen: string[] = [];
const create = defer(() => {
  created.push('R7');
  return of('R7');
});
const view = create.pipe(switchMap(id => pollJob(() => {
  const reply = replies[requests.length];
  requests.push(id + '@' + clock.now());
  return timer(3, clock).pipe(map(() => reply));
}, 2, clock)));
assert.deepEqual(created, []);
view.subscribe({
  next: status => screen.push(clock.now() + ' ' + status.phase + '/' + status.written),
  complete: () => screen.push(clock.now() + ' 完成'),
});
clock.flush();
assert.deepEqual(created, ['R7']);
assert.deepEqual(requests, ['R7@0', 'R7@5', 'R7@10']);
assert.deepEqual(screen, ['3 writing/1', '8 writing/2', '13 ready/2', '13 完成']);
console.log('创建', created, '查询', requests, '画面', screen);

const fixed = new VirtualTimeScheduler();
const starts: number[] = [], ends: number[] = [], displayed: number[] = [];
timer(0, 2, fixed).pipe(
  switchMap(() => {
    starts.push(fixed.now());
    return timer(3, fixed).pipe(finalize(() => ends.push(fixed.now())));
  }),
  takeUntil(timer(11, fixed)),
).subscribe(() => displayed.push(fixed.now()));
fixed.flush();
assert.deepEqual(starts, [0, 2, 4, 6, 8, 10]);
assert.deepEqual(ends, [2, 4, 6, 8, 10, 11]);
assert.deepEqual(displayed, []);
console.log('固定节拍启动', starts, '退出', ends, '收到', displayed);

const closing = new VirtualTimeScheduler();
const opened: number[] = [];
const panel = pollJob(() => {
  opened.push(closing.now());
  return timer(3, closing).pipe(map(() => replies[0]));
}, 2, closing).subscribe();
closing.schedule(() => panel.unsubscribe(), 4);
closing.flush();
assert.deepEqual(opened, [0]);
console.log('等下一轮时关窗，只启动过', opened);
