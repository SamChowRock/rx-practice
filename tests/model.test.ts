import assert from 'node:assert/strict';
import test from 'node:test';
import { BehaviorSubject, EMPTY, Observable, Subject, of, throwError } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { consoleModel, type Row } from '../app/model.ts';

const row = (id: number, task = 'A'): Row => ({ id, task, level: 'error', text: task + id });
test('只有匹配任务的行能推进重连游标；界面的保留上限不是消费总量', () => {
  const clock = new TestScheduler((actual,expected) => assert.deepEqual(actual,expected));
  clock.run(({flush}) => {
    const cursors:number[] = [], streams:Subject<Row>[] = [];
    const recent:Row[][] = [];
    const records = Array.from({length:205},(_,index) => row(index+1));
    const sub = consoleModel(of('A'),() => of(records),(_,after) => {
      cursors.push(after);
      const stream = new Subject<Row>(); streams.push(stream); return stream;
    },() => {},2).subscribe(rows => recent.push(rows));
    const before = recent.at(-1)!;
    streams[0].next(row(800,'B'));
    streams[0].next(row(204));
    assert.equal(recent.at(-1),before);
    streams[0].error(Error('复演断线'));
    clock.schedule(() => streams[1].next(row(206)),3);
    clock.schedule(() => sub.unsubscribe(),4);
    flush();
    const final = recent.at(-1)!;
    assert.deepEqual(cursors,[205,205]);
    assert.deepEqual([before.length,before[0].id,before.at(-1)?.id],[200,6,205]);
    assert.deepEqual([final.length,final[0].id,final.at(-1)?.id],[200,7,206]);
  });
});
test('无历史行的快照与不发出快照不是同一种协议', () => {
  for (const [history, expected] of [[of([] as Row[]),1],[EMPTY,0]] as const) {
    let starts = 0;
    const run = consoleModel(of('A'),()=>history,()=>new Observable(() => { starts++; }));
    const link = run.subscribe();
    assert.equal(starts,expected); link.unsubscribe();
  }
});
test('历史与推送按游标交接；两个面板共享，最后一个退出才断开', () => {
  const task = new BehaviorSubject('A');
  const wire = new Subject<Row>();
  const cursors: number[] = [];
  let closed = 0;
  const model = consoleModel(task, name => of([row(1, name)]), (_, after) => new Observable(out => {
    cursors.push(after);
    const link = wire.subscribe(out);
    return () => { closed++; link.unsubscribe(); };
  }));
  const seen: number[][] = [];
  const first = model.subscribe(rows => seen.push(rows.map(item => item.id)));
  const late = model.subscribe();
  wire.next(row(1)); wire.next(row(2));
  assert.deepEqual(seen.at(-1), [1, 2]);
  first.unsubscribe(); assert.equal(closed, 0);
  task.next('B'); assert.equal(closed, 1);
  late.unsubscribe(); assert.equal(closed, 2);
  assert.deepEqual(cursors, [1, 1]);
});
test('推送中断后从最后确认的游标恢复，切频道时取消等待中的重试', () => {
  const clock = new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
  clock.run(({cold, flush}) => {
    const task = new BehaviorSubject('A');
    const cursors: string[] = [];
    const seen: number[][] = [];
    const model = consoleModel(task, () => of([]), (name, after) => {
      cursors.push(name + after);
      return name === 'B' ? throwError(() => Error('离线')) : cold<Row>('a-#', {a:row(after + 1)});
    }, () => {}, 5);
    const sub = model.subscribe(rows => seen.push(rows.map(item => item.id)));
    clock.schedule(() => task.next('B'), 9);
    clock.schedule(() => sub.unsubscribe(), 11);
    flush();
    assert.deepEqual(cursors, ['A0','A1','B0']);
    assert.ok(seen.some(ids => ids.join() === '1,2'));
    assert.deepEqual(seen.at(-1), []);
  });
});
