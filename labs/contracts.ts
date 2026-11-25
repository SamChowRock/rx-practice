import assert from 'node:assert/strict';
import { BehaviorSubject, EMPTY, EmptyError, Notification, Observable, ReplaySubject, Subscription, TimeoutError, catchError, combineLatest, firstValueFrom, forkJoin, lastValueFrom, of, shareReplay, throwError, timeout } from 'rxjs';
import type { ObservedValueOf, ShareReplayConfig, TimeoutInfo } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

const owned = new Subscription();
const order: string[] = [];
const detached = () => order.push('detached');
owned.add(detached); owned.remove(detached);
owned.add(() => order.push('cleanup'));
owned.unsubscribe(); owned.unsubscribe(); owned.add(() => order.push('late'));
assert.deepEqual(order,['cleanup','late']);
assert.equal(Subscription.EMPTY.closed,true);

const stored = new BehaviorSubject<number>(0);
const readOnly: Observable<number> = stored.asObservable();
if (false) {
  // @ts-expect-error read access must not include a write method
  readOnly.next(12);
}
stored.next(1); stored.complete();
const late: number[] = [];
readOnly.subscribe(value => late.push(value));
assert.equal(late.length,0);
assert.equal(stored.getValue(),1);
const replay = new ReplaySubject<number>(1);
replay.next(1); replay.complete(); replay.subscribe(value => late.push(value));
assert.deepEqual(late,[1]);

assert.equal(await firstValueFrom(EMPTY,{defaultValue:0}),0);
await assert.rejects(lastValueFrom(EMPTY), EmptyError);
assert.equal(await firstValueFrom(of(undefined)),undefined);
async function failedFallback() { return firstValueFrom(throwError(() => Error('offline')), {defaultValue:0}); }
await assert.rejects(failedFallback,/offline/);

const tuple = combineLatest([of(1), of('A')] as const);
const typed: ObservedValueOf<typeof tuple> = [1,'A'];
assert.deepEqual(await firstValueFrom(tuple),typed);
const dictionary = forkJoin({id:of(1),task:of('A')});
assert.deepEqual(await lastValueFrom(dictionary),{id:1,task:'A'});
if (false) {
  // @ts-expect-error position zero is numeric, not a task name
  const reversed: ObservedValueOf<typeof tuple> = ['A',1];
  // @ts-expect-error object overload requires an explicit reference-count choice
  const ambiguous: ShareReplayConfig = {bufferSize:1};
  void [reversed,ambiguous];
}
const message = Notification.createNext(7);
assert.equal(message.kind,'N'); assert.equal(await firstValueFrom(message.toObservable()),7);
assert.equal(Notification.createComplete().hasValue,false);

const clock = new TestScheduler((actual,expected) => assert.deepEqual(actual,expected));
clock.run(({cold,expectObservable}) => {
  const recover = (info: TimeoutInfo<number, string>) => of(info.meta + ':' + info.seen);
  expectObservable(cold<number>('-a----', {a:9}).pipe(timeout({first:4,each:2,meta:'A',with:recover}))).toBe('-a-(b|)',{a:9,b:'A:1'});
  expectObservable(cold('----a').pipe(timeout({first:2}),shareReplay({bufferSize:1,refCount:true}),
    catchError(error => { assert.ok(error instanceof TimeoutError); return throwError(() => error.name); })
  )).toBe('--#',undefined,'TimeoutError');
});
console.log('通过：所有权、只读视图、空与失败、回放、元组与字典重载、超时配置。');

type WireRow = { id:number; task:string };
function isWireRow(input:unknown):input is WireRow {
  return typeof input==='object' && input!==null && 'id' in input && Number.isSafeInteger(input.id)
    && typeof input.id==='number' && input.id>0 && 'task' in input && typeof input.task==='string';
}
const wire:unknown[] = [null,{id:'3',task:'A'},{id:3,task:'A'},{id:1.2,task:'B'}];
assert.deepEqual(wire.filter(isWireRow),[{id:3,task:'A'}]);
let delivered = 0;
const gate = Promise.withResolvers<void>();
const finished = Promise.withResolvers<void>();
const source = new Observable<number>(subscriber => {
  subscriber.next(0); subscriber.complete();
});
const unit = source.forEach(async () => {
  await gate.promise;
  delivered++;
  finished.resolve();
});
await unit;
assert.equal(delivered,0);
gate.resolve();
await finished.promise;
assert.equal(delivered,1);
console.log('通过：边界校验只接纳正安全整数编号；forEach 完成不等待异步回调完成。');
