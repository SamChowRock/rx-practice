import { asyncScheduler, defer, repeat, takeWhile, timer } from 'rxjs';
import type { Observable, SchedulerLike } from 'rxjs';

export type JobStatus = { phase: 'writing' | 'ready'; written: number };

// 每次 read 只返回一份状态。ready 表示报告已经发布，可以开始下载。
export function pollJob(
  read: () => Observable<JobStatus>, pause: number, clock: SchedulerLike = asyncScheduler,
): Observable<JobStatus> {
  return defer(read).pipe(
    repeat({ delay: () => timer(pause, clock) }),
    takeWhile(status => status.phase !== 'ready', true),
  );
}
