import assert from 'node:assert/strict';
import {
  BehaviorSubject, EMPTY, Observable, Subject, Subscription, catchError, concatMap,
  defer, exhaustMap, map, merge, mergeMap, of, retry, scan, share,
  shareReplay, startWith, switchMap, takeUntil, tap, timeout
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

const scheduler = () => new TestScheduler((a, b) => assert.deepEqual(a, b));
scheduler().run(({ hot, cold, expectObservable }) => {
  const edits = hot('abc-----|');
  const work = (id: string) => cold(id === 'a' ? '----(v|)' : '-(v|)', { v: id });
  expectObservable(edits.pipe(switchMap(work))).toBe('---c----|');
  expectObservable(edits.pipe(concatMap(work))).toBe('----abc-|');
  expectObservable(edits.pipe(mergeMap(work, 2))).toBe('--bca---|');
  expectObservable(edits.pipe(exhaustMap(work))).toBe('----a---|');
});
console.log('a 最慢：任务只显示 c；备注提交 abc；两路详情 bca；忙时刷新只执行 a。');

const clock = scheduler();
clock.run(({ cold, expectSubscriptions, flush }) => {
  const task = new BehaviorSubject('A'), clear = new Subject<'clear'>();
  const a = cold('-e-----x'), b = cold('-e-----x');
  const wait = cold('----r|');
  let opens = 0;
  const feed = task.pipe(switchMap(id => defer(() => {
    opens++;
    return id === 'A' ? a : b;
  }).pipe(timeout({ each: 3 }), retry({ count: 1, delay: () => wait }),
    catchError(() => EMPTY), map(value => id + value))), share());
  const count = merge(feed, clear).pipe(scan((n, v) => v === 'clear' ? 0 : n + 1, 0),
    startWith(0), shareReplay({ bufferSize: 1, refCount: true }));
  // 会话持有状态；面板隐藏期间仍须计数。最终连同会话一起释放。
  const owner = new Subscription();
  owner.add(count.subscribe());
  const rows: string[] = [], resumed: number[] = [], commands: string[] = [];
  owner.add(feed.subscribe(v => rows.push(v)));
  const firstPanel = count.subscribe();
  clock.schedule(() => { task.next('B'); clear.next('clear'); firstPanel.unsubscribe(); }, 2);
  clock.schedule(() => {
    owner.add(count.subscribe(n => resumed.push(n)));
    owner.add(clear.subscribe(v => commands.push(v)));
  }, 7);
  clock.schedule(() => owner.unsubscribe(), 8);
  expectSubscriptions(a.subscriptions).toBe('^-!');
  expectSubscriptions(b.subscriptions).toBe('--^---!');
  expectSubscriptions(wait.subscriptions).toBe('------^-!');
  flush();
  assert.deepEqual([rows, resumed, commands, opens], [['Ae', 'Be'], [1], [], 2]);
});
console.log('A/B 各建一次连接，重开计数首值 1，旧清空命令 0 条；重连等待在帧 8 被释放。');

scheduler().run(({ hot, cold, expectObservable }) => {
  const tasks = hot('a------b---|');
  expectObservable(tasks.pipe(switchMap(id => (id === 'a' ? cold<string>('-#') : cold('-s|')).pipe(
    retry(1), catchError(() => of('d'))
  )))).toBe('--d-----s--|');
});
console.log('另一次演练让重试耗尽：先降级 d，之后切换仍收到 s。');

function notifications(legacy: boolean) {
  const trace: string[] = [];
  const next = (v: number) => trace.push(String(v));
  const error = () => trace.push('error'), complete = () => trace.push('done');
  for (const source of [of(0), new Observable<number>(s => s.error(Error('offline')))]) {
    if (legacy) source.subscribe(next, error, complete);
    else source.subscribe({ next, error, complete });
  }
  return trace;
}
assert.deepEqual(notifications(false), ['0', 'done', 'error']);
assert.deepEqual(notifications(true), notifications(false));
console.log('迁移前后通知轨迹一致：0、完成；另一路失败。');

const bookingClock = scheduler();
bookingClock.run(({ cold, hot, expectObservable, expectSubscriptions, flush }) => {
  const day = hot('ab-------|'), left = hot('-----x');
  const stale = cold('----x|'), fresh = cold('-y|');
  expectObservable(day.pipe(switchMap(d => d === 'a' ? stale : fresh), takeUntil(left))).toBe('--y--|');
  expectSubscriptions(stale.subscriptions).toBe('^!');
  const slot = new BehaviorSubject('B-10'), confirm = hot('--cc------|');
  const ledger = new Map<string, string>(); // 模拟服务端以意图键唯一写入
  const pending = new Map<string, string>();
  const attempts: string[] = [], receipts: string[] = [];
  let serial = 0;
  function submit(key: string, snapshot: string) {
    return defer(() => {
      attempts.push(key);
      if (!ledger.has(key)) ledger.set(key, snapshot);
      return attempts.length === 1 ? cold<string>('----#') : cold('-(v|)', { v: ledger.get(key)! });
    });
  }
  // 页面只负责提交意图，回执处理归应用会话。离开时断开页面输入。
  confirm.pipe(takeUntil(left), exhaustMap(() => {
    const snapshot = slot.value;
    const prior = [...pending].find(([, bookedSlot]) => bookedSlot === snapshot);
    const key = prior?.[0] ?? 'intent-' + ++serial;
    pending.set(key, snapshot);
    return submit(key, snapshot).pipe(retry(1), tap(v => {
      receipts.push(v); pending.delete(key);
    }), catchError(() => EMPTY));
  })).subscribe();
  bookingClock.schedule(() => slot.next('B-16'), 4);
  bookingClock.schedule(() => assert.equal(pending.size, 1), 5);
  flush();
  assert.deepEqual(attempts, ['intent-1', 'intent-1']);
  assert.deepEqual([...ledger.values()], ['B-10']);
  assert.deepEqual(receipts, ['B-10']);
  assert.equal(pending.size, 0);
});
console.log('预约：切日只展示 y；连点只产生一个意图；离页时待核对 1 笔，同键补取 B-10 回执后归零。');

{
  const pending = new Map<string,string>(), ledger = new Set<string>(), attempts:string[] = [];
  const clicks = new Subject<string>();
  let serial = 0;
  clicks.pipe(exhaustMap(slot => {
    const key = pending.get(slot) ?? 'intent-' + ++serial;
    pending.set(slot,key);
    return defer(() => {
      attempts.push(key); ledger.add(key);
      if (attempts.length < 3) throw Error('已受理，回执未送达');
      return of(key);
    }).pipe(retry(1),tap(() => pending.delete(slot)),catchError(() => EMPTY));
  })).subscribe();
  clicks.next('B-10');
  assert.equal(pending.size,1);
  clicks.next('B-10'); clicks.complete();
  assert.deepEqual(attempts,['intent-1','intent-1','intent-1']);
  assert.deepEqual([...ledger],['intent-1']); assert.equal(pending.size,0);
}

// 变式只改接纳策略。在此独立驱动规则，服务端去重与离页边界见上一组断言。
const unresolved = new Map<string, number>();
let nextIntent = 0;
function admit(slotId: string) {
  if (unresolved.has(slotId)) return '同场待核对';
  if (unresolved.size === 2) return '稍后再试';
  unresolved.set(slotId, ++nextIntent);
  return '接纳';
}
assert.deepEqual(['A', 'A', 'B', 'C'].map(admit), ['接纳', '同场待核对', '接纳', '稍后再试']);
unresolved.delete('B'); // 取得明确回执才释放该场次名额
assert.deepEqual([...unresolved], [['A', 1]]);
assert.equal(admit('C'), '接纳');
assert.deepEqual([...unresolved], [['A', 1], ['C', 3]]);
console.log('变式：A、B 可同时待核对，重复 A 被拦下；满额时 C 被拒，B 释放后须重新点击 C。');

type Form = { slot: string; access: { seats: number[] } };
function freezeAtClick(copy: boolean) {
  const timeline = scheduler();
  const form: Form = { slot: 'B-10', access: { seats: [3, 4] } };
  const sent: number[][] = [];
  timeline.run(({ cold, flush }) => {
    const selected = new BehaviorSubject(form);
    const click = new Subject<void>();
    click.pipe(
      map(() => {
        const state = selected.value;
        return copy ? { slot: state.slot, access: { seats: [...state.access.seats] } } : { ...state };
      }),
      concatMap(job => cold('---(r|)').pipe(map(() => [...job.access.seats])))
    ).subscribe(seats => sent.push(seats));
    timeline.schedule(() => click.next(), 1);
    timeline.schedule(() => { form.access.seats[1] = 8; }, 2);
    timeline.schedule(() => click.complete(), 3);
    flush();
  });
  return sent;
}
assert.deepEqual(freezeAtClick(false), [[3, 8]]);
assert.deepEqual(freezeAtClick(true), [[3, 4]]);
console.log('深水题一：相同采样时刻，浅拷贝在实际发送时读到 3、8；复制提交字段得到 3、4。');

type Report = { key: string; revision: number; state: 'confirmed' | 'revoked' };
const reports = [
  { key: 'A', revision: 3, state: 'revoked' },
  { key: 'B', revision: 1, state: 'confirmed' },
  { key: 'A', revision: 2, state: 'confirmed' },
  { key: 'A', revision: 3, state: 'revoked' }
] satisfies Report[];
function projectRevisions(input: Report[]) {
  const snapshots: Map<string,Report>[] = [];
  of(...input).pipe(scan((byKey, report) => {
    if ((byKey.get(report.key)?.revision ?? -1) >= report.revision) return byKey;
    return new Map(byKey).set(report.key,report);
  },new Map<string,Report>())).subscribe(snapshot => snapshots.push(snapshot));
  return snapshots;
}
const revisions = projectRevisions(reports);
assert.deepEqual([...revisions.at(-1)!].map(([key, value]) => [key, value.state]), [['A','revoked'],['B','confirmed']]);
assert.equal(revisions[0].size,1);
assert.equal(revisions[1],revisions[2]);
assert.equal(revisions[2],revisions[3]);
assert.equal(projectRevisions(reports.slice(0,3).reverse()).at(-1)!.get('A')?.state,'revoked');
console.log('深水题二：同笔只接受新版本，另一笔低版本不被误丢；旧快照与重发均可辨认。');
