import assert from 'node:assert/strict';
import { TimeoutError, distinctUntilChanged, filter, map, timeout, timer } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { pollJob, type JobStatus } from './report-job.ts';

const clock = new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
clock.run(({ cold }) => {
  // 每次查询各自返回一个新对象，written 不变。
  const status = pollJob(() => timer(1, clock).pipe(
    map((): JobStatus => ({ phase: 'writing', written: 1 })),
  ), 1, clock);
  const raw: number[] = [], changes: number[] = [], exits: number[] = [];
  const rawPanel = status.pipe(timeout({ each: 5, scheduler: clock })).subscribe(() => raw.push(clock.now()));
  status.pipe(
    distinctUntilChanged((a, b) => a.phase === b.phase && a.written === b.written),
    timeout({ each: 5, scheduler: clock }),
  ).subscribe({
    next: () => changes.push(clock.now()),
    error: err => { assert.ok(err instanceof TimeoutError); exits.push(clock.now()); },
  });
  clock.schedule(() => rawPanel.unsubscribe(), 12);

  const states: Record<string, JobStatus> = {
    a: { phase: 'writing', written: 1 }, b: { phase: 'writing', written: 2 },
    c: { phase: 'ready', written: 2 },
  };
  const progress = cold<JobStatus>('-a---b-c|', states);
  const phases: string[] = [], countsOnly: string[] = [];
  progress.pipe(
    distinctUntilChanged((a, b) => a.phase === b.phase && a.written === b.written),
    timeout({ each: 5, scheduler: clock }),
  ).subscribe(s => phases.push(s.phase));
  progress.pipe(distinctUntilChanged((a, b) => a.written === b.written))
    .subscribe(s => countsOnly.push(s.phase));
  const deadlines: number[] = [];
  progress.pipe(filter(s => s.phase === 'ready'), timeout({ first: 6, scheduler: clock }))
    .subscribe({ error: err => { assert.ok(err instanceof TimeoutError); deadlines.push(clock.now()); } });
  clock.flush();
  assert.deepEqual(raw, [1, 3, 5, 7, 9, 11]);
  assert.deepEqual(changes, [1]);
  assert.deepEqual(exits, [6]);
  assert.deepEqual(phases, ['writing', 'writing', 'ready']);
  assert.deepEqual(countsOnly, ['writing', 'writing']);
  assert.deepEqual(deadlines, [6]);
  console.log('回信到达', raw, '内容有变', changes, '停滞超时', exits);
  console.log('比较阶段和行数', phases, '只比较行数', countsOnly, '发布总期限', deadlines);
});
