# RxJS 7.8.2 · 公开 API 学习清单

基于 npm 发布包的 TypeScript 声明生成。六个公开代码入口共 365 个「入口 × 导出名」条目。相同的底层声明只在附录展示一次；不同入口的同名项不一定是同一个函数。类型、别名与弃用项均保留。排除 internal 深层导入及 package.json 元数据。

- `rxjs`：228
- `rxjs/ajax`：7
- `rxjs/fetch`：1
- `rxjs/operators`：124
- `rxjs/testing`：2
- `rxjs/webSocket`：3

## 怎么打勾

函数或方法：能写出最小例子、预测输出和终止行为，并与最容易混淆的 API 区分。类型或配置：能写出合法用法，并解释一个不合法的例子。弃用项：能认出旧代码并说明迁移方式。重复导出：核对导入路径即可，不必重复上课。

例如：先从 rxjs 和 rxjs/operators 各找到 concat，比较两份声明的返回值；再找 concatWith，解释为什么有的写在 pipe 里面，有的写在外面。

## rxjs（228）

- [ ] [animationFrame](#api-1)
- [ ] [animationFrames](#api-2)
- [ ] [animationFrameScheduler](#api-3)
- [ ] [ArgumentOutOfRangeError](#api-4)
- [ ] [asap](#api-5)
- [ ] [asapScheduler](#api-6)
- [ ] [async](#api-7)
- [ ] [asyncScheduler](#api-8)
- [ ] [AsyncSubject](#api-9)
- [ ] [audit](#api-10)
- [ ] [auditTime](#api-11)
- [ ] [BasicGroupByOptions](#api-12)
- [ ] [BehaviorSubject](#api-13)
- [ ] [bindCallback](#api-14)
- [ ] [bindNodeCallback](#api-15)
- [ ] [buffer](#api-16)
- [ ] [bufferCount](#api-17)
- [ ] [bufferTime](#api-18)
- [ ] [bufferToggle](#api-19)
- [ ] [bufferWhen](#api-20)
- [ ] [catchError](#api-21)
- [ ] [combineAll](#api-22)
- [ ] [combineLatest](#api-23)
- [ ] [combineLatestAll](#api-24)
- [ ] [combineLatestWith](#api-25)
- [ ] [CompleteNotification](#api-26)
- [ ] [CompletionObserver](#api-27)
- [ ] [concat](#api-28)
- [ ] [concatAll](#api-29)
- [ ] [concatMap](#api-30)
- [ ] [concatMapTo](#api-31)
- [ ] [concatWith](#api-32)
- [ ] [config](#api-33)
- [ ] [connect](#api-34)
- [ ] [connectable](#api-35)
- [ ] [Connectable](#api-36)
- [ ] [ConnectableObservable](#api-37)
- [ ] [ConnectConfig](#api-38)
- [ ] [Cons](#api-39)
- [ ] [count](#api-40)
- [ ] [debounce](#api-41)
- [ ] [debounceTime](#api-42)
- [ ] [defaultIfEmpty](#api-43)
- [ ] [defer](#api-44)
- [ ] [delay](#api-45)
- [ ] [delayWhen](#api-46)
- [ ] [dematerialize](#api-47)
- [ ] [distinct](#api-48)
- [ ] [distinctUntilChanged](#api-49)
- [ ] [distinctUntilKeyChanged](#api-50)
- [ ] [elementAt](#api-51)
- [ ] [empty](#api-52)
- [ ] [EMPTY](#api-53)
- [ ] [EmptyError](#api-54)
- [ ] [endWith](#api-55)
- [ ] [ErrorNotification](#api-56)
- [ ] [ErrorObserver](#api-57)
- [ ] [every](#api-58)
- [ ] [exhaust](#api-59)
- [ ] [exhaustAll](#api-60)
- [ ] [exhaustMap](#api-61)
- [ ] [expand](#api-62)
- [ ] [FactoryOrValue](#api-63)
- [ ] [Falsy](#api-64)
- [ ] [filter](#api-65)
- [ ] [finalize](#api-66)
- [ ] [find](#api-67)
- [ ] [findIndex](#api-68)
- [ ] [first](#api-69)
- [ ] [firstValueFrom](#api-70)
- [ ] [flatMap](#api-71)
- [ ] [forkJoin](#api-72)
- [ ] [from](#api-73)
- [ ] [fromEvent](#api-74)
- [ ] [fromEventPattern](#api-75)
- [ ] [generate](#api-76)
- [ ] [GlobalConfig](#api-77)
- [ ] [groupBy](#api-78)
- [ ] [GroupByOptionsWithElement](#api-79)
- [ ] [GroupedObservable](#api-80)
- [ ] [Head](#api-81)
- [ ] [identity](#api-82)
- [ ] [ignoreElements](#api-83)
- [ ] [iif](#api-84)
- [ ] [InteropObservable](#api-85)
- [ ] [interval](#api-86)
- [ ] [isEmpty](#api-87)
- [ ] [isObservable](#api-88)
- [ ] [last](#api-89)
- [ ] [lastValueFrom](#api-90)
- [ ] [map](#api-91)
- [ ] [mapTo](#api-92)
- [ ] [materialize](#api-93)
- [ ] [max](#api-94)
- [ ] [merge](#api-95)
- [ ] [mergeAll](#api-96)
- [ ] [mergeMap](#api-97)
- [ ] [mergeMapTo](#api-98)
- [ ] [mergeScan](#api-99)
- [ ] [mergeWith](#api-100)
- [ ] [min](#api-101)
- [ ] [MonoTypeOperatorFunction](#api-102)
- [ ] [multicast](#api-103)
- [ ] [never](#api-104)
- [ ] [NEVER](#api-105)
- [ ] [NextNotification](#api-106)
- [ ] [NextObserver](#api-107)
- [ ] [noop](#api-108)
- [ ] [NotFoundError](#api-109)
- [ ] [Notification](#api-110)
- [ ] [NotificationKind](#api-111)
- [ ] [ObjectUnsubscribedError](#api-112)
- [ ] [observable](#api-113)
- [ ] [Observable](#api-114)
- [ ] [ObservableInput](#api-115)
- [ ] [ObservableInputTuple](#api-116)
- [ ] [ObservableLike](#api-117)
- [ ] [ObservableNotification](#api-118)
- [ ] [ObservedValueOf](#api-119)
- [ ] [ObservedValuesFromArray](#api-120)
- [ ] [ObservedValueTupleFromArray](#api-121)
- [ ] [ObservedValueUnionFromArray](#api-122)
- [ ] [observeOn](#api-123)
- [ ] [Observer](#api-124)
- [ ] [of](#api-125)
- [ ] [onErrorResumeNext](#api-126)
- [ ] [onErrorResumeNextWith](#api-127)
- [ ] [Operator](#api-128)
- [ ] [OperatorFunction](#api-129)
- [ ] [pairs](#api-130)
- [ ] [pairwise](#api-131)
- [ ] [PartialObserver](#api-132)
- [ ] [partition](#api-133)
- [ ] [pipe](#api-134)
- [ ] [pluck](#api-135)
- [ ] [publish](#api-136)
- [ ] [publishBehavior](#api-137)
- [ ] [publishLast](#api-138)
- [ ] [publishReplay](#api-139)
- [ ] [queue](#api-140)
- [ ] [queueScheduler](#api-141)
- [ ] [race](#api-142)
- [ ] [raceWith](#api-143)
- [ ] [range](#api-144)
- [ ] [ReadableStreamLike](#api-145)
- [ ] [reduce](#api-146)
- [ ] [refCount](#api-147)
- [ ] [repeat](#api-148)
- [ ] [RepeatConfig](#api-149)
- [ ] [repeatWhen](#api-150)
- [ ] [ReplaySubject](#api-151)
- [ ] [retry](#api-152)
- [ ] [RetryConfig](#api-153)
- [ ] [retryWhen](#api-154)
- [ ] [sample](#api-155)
- [ ] [sampleTime](#api-156)
- [ ] [scan](#api-157)
- [ ] [scheduled](#api-158)
- [ ] [Scheduler](#api-159)
- [ ] [SchedulerAction](#api-160)
- [ ] [SchedulerLike](#api-161)
- [ ] [sequenceEqual](#api-162)
- [ ] [SequenceError](#api-163)
- [ ] [share](#api-164)
- [ ] [ShareConfig](#api-165)
- [ ] [shareReplay](#api-166)
- [ ] [ShareReplayConfig](#api-167)
- [ ] [single](#api-168)
- [ ] [skip](#api-169)
- [ ] [skipLast](#api-170)
- [ ] [skipUntil](#api-171)
- [ ] [skipWhile](#api-172)
- [ ] [startWith](#api-173)
- [ ] [Subject](#api-174)
- [ ] [SubjectLike](#api-175)
- [ ] [Subscribable](#api-176)
- [ ] [SubscribableOrPromise](#api-177)
- [ ] [subscribeOn](#api-178)
- [ ] [Subscriber](#api-179)
- [ ] [Subscription](#api-180)
- [ ] [SubscriptionLike](#api-181)
- [ ] [switchAll](#api-182)
- [ ] [switchMap](#api-183)
- [ ] [switchMapTo](#api-184)
- [ ] [switchScan](#api-185)
- [ ] [Tail](#api-186)
- [ ] [take](#api-187)
- [ ] [takeLast](#api-188)
- [ ] [takeUntil](#api-189)
- [ ] [takeWhile](#api-190)
- [ ] [tap](#api-191)
- [ ] [TapObserver](#api-192)
- [ ] [TeardownLogic](#api-193)
- [ ] [throttle](#api-194)
- [ ] [ThrottleConfig](#api-195)
- [ ] [throttleTime](#api-196)
- [ ] [throwError](#api-197)
- [ ] [throwIfEmpty](#api-198)
- [ ] [timeInterval](#api-199)
- [ ] [TimeInterval](#api-200)
- [ ] [timeout](#api-201)
- [ ] [TimeoutConfig](#api-202)
- [ ] [TimeoutError](#api-203)
- [ ] [TimeoutInfo](#api-204)
- [ ] [timeoutWith](#api-205)
- [ ] [timer](#api-206)
- [ ] [timestamp](#api-207)
- [ ] [Timestamp](#api-208)
- [ ] [TimestampProvider](#api-209)
- [ ] [toArray](#api-210)
- [ ] [TruthyTypesOf](#api-211)
- [ ] [UnaryFunction](#api-212)
- [ ] [Unsubscribable](#api-213)
- [ ] [UnsubscriptionError](#api-214)
- [ ] [using](#api-215)
- [ ] [ValueFromArray](#api-216)
- [ ] [ValueFromNotification](#api-217)
- [ ] [VirtualAction](#api-218)
- [ ] [VirtualTimeScheduler](#api-219)
- [ ] [window](#api-220)
- [ ] [windowCount](#api-221)
- [ ] [windowTime](#api-222)
- [ ] [windowToggle](#api-223)
- [ ] [windowWhen](#api-224)
- [ ] [withLatestFrom](#api-225)
- [ ] [zip](#api-226)
- [ ] [zipAll](#api-227)
- [ ] [zipWith](#api-228)

## rxjs/ajax（7）

- [ ] [ajax](#api-229)
- [ ] [AjaxConfig](#api-230)
- [ ] [AjaxDirection](#api-231)
- [ ] [AjaxError](#api-232)
- [ ] [AjaxRequest](#api-233)
- [ ] [AjaxResponse](#api-234)
- [ ] [AjaxTimeoutError](#api-235)

## rxjs/fetch（1）

- [ ] [fromFetch](#api-236)

## rxjs/operators（124）

- [ ] [audit](#api-10)
- [ ] [auditTime](#api-11)
- [ ] [BasicGroupByOptions](#api-12)
- [ ] [buffer](#api-16)
- [ ] [bufferCount](#api-17)
- [ ] [bufferTime](#api-18)
- [ ] [bufferToggle](#api-19)
- [ ] [bufferWhen](#api-20)
- [ ] [catchError](#api-21)
- [ ] [combineAll](#api-22)
- [ ] [combineLatest](#api-237)
- [ ] [combineLatestAll](#api-24)
- [ ] [combineLatestWith](#api-25)
- [ ] [concat](#api-238)
- [ ] [concatAll](#api-29)
- [ ] [concatMap](#api-30)
- [ ] [concatMapTo](#api-31)
- [ ] [concatWith](#api-32)
- [ ] [connect](#api-34)
- [ ] [ConnectConfig](#api-38)
- [ ] [count](#api-40)
- [ ] [debounce](#api-41)
- [ ] [debounceTime](#api-42)
- [ ] [defaultIfEmpty](#api-43)
- [ ] [delay](#api-45)
- [ ] [delayWhen](#api-46)
- [ ] [dematerialize](#api-47)
- [ ] [distinct](#api-48)
- [ ] [distinctUntilChanged](#api-49)
- [ ] [distinctUntilKeyChanged](#api-50)
- [ ] [elementAt](#api-51)
- [ ] [endWith](#api-55)
- [ ] [every](#api-58)
- [ ] [exhaust](#api-59)
- [ ] [exhaustAll](#api-60)
- [ ] [exhaustMap](#api-61)
- [ ] [expand](#api-62)
- [ ] [filter](#api-65)
- [ ] [finalize](#api-66)
- [ ] [find](#api-67)
- [ ] [findIndex](#api-68)
- [ ] [first](#api-69)
- [ ] [flatMap](#api-71)
- [ ] [groupBy](#api-78)
- [ ] [GroupByOptionsWithElement](#api-79)
- [ ] [ignoreElements](#api-83)
- [ ] [isEmpty](#api-87)
- [ ] [last](#api-89)
- [ ] [map](#api-91)
- [ ] [mapTo](#api-92)
- [ ] [materialize](#api-93)
- [ ] [max](#api-94)
- [ ] [merge](#api-239)
- [ ] [mergeAll](#api-96)
- [ ] [mergeMap](#api-97)
- [ ] [mergeMapTo](#api-98)
- [ ] [mergeScan](#api-99)
- [ ] [mergeWith](#api-100)
- [ ] [min](#api-101)
- [ ] [multicast](#api-103)
- [ ] [observeOn](#api-123)
- [ ] [onErrorResumeNext](#api-240)
- [ ] [pairwise](#api-131)
- [ ] [partition](#api-241)
- [ ] [pluck](#api-135)
- [ ] [publish](#api-136)
- [ ] [publishBehavior](#api-137)
- [ ] [publishLast](#api-138)
- [ ] [publishReplay](#api-139)
- [ ] [race](#api-242)
- [ ] [raceWith](#api-143)
- [ ] [reduce](#api-146)
- [ ] [refCount](#api-147)
- [ ] [repeat](#api-148)
- [ ] [RepeatConfig](#api-149)
- [ ] [repeatWhen](#api-150)
- [ ] [retry](#api-152)
- [ ] [RetryConfig](#api-153)
- [ ] [retryWhen](#api-154)
- [ ] [sample](#api-155)
- [ ] [sampleTime](#api-156)
- [ ] [scan](#api-157)
- [ ] [sequenceEqual](#api-162)
- [ ] [share](#api-164)
- [ ] [ShareConfig](#api-165)
- [ ] [shareReplay](#api-166)
- [ ] [ShareReplayConfig](#api-167)
- [ ] [single](#api-168)
- [ ] [skip](#api-169)
- [ ] [skipLast](#api-170)
- [ ] [skipUntil](#api-171)
- [ ] [skipWhile](#api-172)
- [ ] [startWith](#api-173)
- [ ] [subscribeOn](#api-178)
- [ ] [switchAll](#api-182)
- [ ] [switchMap](#api-183)
- [ ] [switchMapTo](#api-184)
- [ ] [switchScan](#api-185)
- [ ] [take](#api-187)
- [ ] [takeLast](#api-188)
- [ ] [takeUntil](#api-189)
- [ ] [takeWhile](#api-190)
- [ ] [tap](#api-191)
- [ ] [TapObserver](#api-192)
- [ ] [throttle](#api-194)
- [ ] [ThrottleConfig](#api-195)
- [ ] [throttleTime](#api-196)
- [ ] [throwIfEmpty](#api-198)
- [ ] [timeInterval](#api-199)
- [ ] [timeout](#api-201)
- [ ] [TimeoutConfig](#api-202)
- [ ] [TimeoutInfo](#api-204)
- [ ] [timeoutWith](#api-205)
- [ ] [timestamp](#api-207)
- [ ] [toArray](#api-210)
- [ ] [window](#api-220)
- [ ] [windowCount](#api-221)
- [ ] [windowTime](#api-222)
- [ ] [windowToggle](#api-223)
- [ ] [windowWhen](#api-224)
- [ ] [withLatestFrom](#api-225)
- [ ] [zip](#api-243)
- [ ] [zipAll](#api-227)
- [ ] [zipWith](#api-228)

## rxjs/testing（2）

- [ ] [RunHelpers](#api-244)
- [ ] [TestScheduler](#api-245)

## rxjs/webSocket（3）

- [ ] [webSocket](#api-246)
- [ ] [WebSocketSubject](#api-247)
- [ ] [WebSocketSubjectConfig](#api-248)

## 声明速查

以下摘录保留重载和类或接口自身声明的公开成员，继承成员请沿 extends 查阅；变量引用的辅助类型不在这里递归展开。标记按具体声明上的 @deprecated 标注，不表示同名所有重载都已弃用。

<a id="api-1"></a>

### animationFrame

声明：`dist/types/internal/scheduler/animationFrame.d.ts`

    [弃用] animationFrame: AnimationFrameScheduler

<a id="api-2"></a>

### animationFrames

声明：`dist/types/internal/observable/dom/animationFrames.d.ts`

    export declare function animationFrames(timestampProvider?: TimestampProvider): Observable<{ timestamp: number; elapsed: number; }>;

<a id="api-3"></a>

### animationFrameScheduler

声明：`dist/types/internal/scheduler/animationFrame.d.ts`

    animationFrameScheduler: AnimationFrameScheduler

<a id="api-4"></a>

### ArgumentOutOfRangeError

声明：`dist/types/internal/util/ArgumentOutOfRangeError.d.ts`

    export interface ArgumentOutOfRangeError extends Error
    ArgumentOutOfRangeError: ArgumentOutOfRangeErrorCtor

<a id="api-5"></a>

### asap

声明：`dist/types/internal/scheduler/asap.d.ts`

    [弃用] asap: AsapScheduler

<a id="api-6"></a>

### asapScheduler

声明：`dist/types/internal/scheduler/asap.d.ts`

    asapScheduler: AsapScheduler

<a id="api-7"></a>

### async

声明：`dist/types/internal/scheduler/async.d.ts`

    [弃用] async: AsyncScheduler

<a id="api-8"></a>

### asyncScheduler

声明：`dist/types/internal/scheduler/async.d.ts`

    asyncScheduler: AsyncScheduler

<a id="api-9"></a>

### AsyncSubject

声明：`dist/types/internal/AsyncSubject.d.ts`

    export declare class AsyncSubject<T> extends Subject<T>
    next(value: T): void;
    complete(): void;

<a id="api-10"></a>

### audit

声明：`dist/types/internal/operators/audit.d.ts`

    export declare function audit<T>(durationSelector: (value: T) => ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-11"></a>

### auditTime

声明：`dist/types/internal/operators/auditTime.d.ts`

    export declare function auditTime<T>(duration: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;

<a id="api-12"></a>

### BasicGroupByOptions

声明：`dist/types/internal/operators/groupBy.d.ts`

    export interface BasicGroupByOptions<K, T>
    element?: undefined;
    duration?: (grouped: GroupedObservable<K, T>) => ObservableInput<any>;
    connector?: () => SubjectLike<T>;

<a id="api-13"></a>

### BehaviorSubject

声明：`dist/types/internal/BehaviorSubject.d.ts`

    export declare class BehaviorSubject<T> extends Subject<T>
    constructor(_value: T);
    get value(): T;
    getValue(): T;
    next(value: T): void;

<a id="api-14"></a>

### bindCallback

声明：`dist/types/internal/observable/bindCallback.d.ts`

    export declare function bindCallback(callbackFunc: (...args: any[]) => void, resultSelector: (...args: any[]) => any, scheduler?: SchedulerLike): (...args: any[]) => Observable<any>;
    export declare function bindCallback<A extends readonly unknown[], R extends readonly unknown[]>(callbackFunc: (...args: [...A, (...res: R) => void]) => void, schedulerLike?: SchedulerLike): (...arg: A) => Observable<R extends [] ? void : R extends [any] ? R[0] : R>;

<a id="api-15"></a>

### bindNodeCallback

声明：`dist/types/internal/observable/bindNodeCallback.d.ts`

    export declare function bindNodeCallback(callbackFunc: (...args: any[]) => void, resultSelector: (...args: any[]) => any, scheduler?: SchedulerLike): (...args: any[]) => Observable<any>;
    export declare function bindNodeCallback<A extends readonly unknown[], R extends readonly unknown[]>(callbackFunc: (...args: [...A, (err: any, ...res: R) => void]) => void, schedulerLike?: SchedulerLike): (...arg: A) => Observable<R extends [] ? void : R extends [any] ? R[0] : R>;

<a id="api-16"></a>

### buffer

声明：`dist/types/internal/operators/buffer.d.ts`

    export declare function buffer<T>(closingNotifier: ObservableInput<any>): OperatorFunction<T, T[]>;

<a id="api-17"></a>

### bufferCount

声明：`dist/types/internal/operators/bufferCount.d.ts`

    export declare function bufferCount<T>(bufferSize: number, startBufferEvery?: number | null): OperatorFunction<T, T[]>;

<a id="api-18"></a>

### bufferTime

声明：`dist/types/internal/operators/bufferTime.d.ts`

    export declare function bufferTime<T>(bufferTimeSpan: number, scheduler?: SchedulerLike): OperatorFunction<T, T[]>;
    export declare function bufferTime<T>(bufferTimeSpan: number, bufferCreationInterval: number | null | undefined, scheduler?: SchedulerLike): OperatorFunction<T, T[]>;
    export declare function bufferTime<T>(bufferTimeSpan: number, bufferCreationInterval: number | null | undefined, maxBufferSize: number, scheduler?: SchedulerLike): OperatorFunction<T, T[]>;

<a id="api-19"></a>

### bufferToggle

声明：`dist/types/internal/operators/bufferToggle.d.ts`

    export declare function bufferToggle<T, O>(openings: ObservableInput<O>, closingSelector: (value: O) => ObservableInput<any>): OperatorFunction<T, T[]>;

<a id="api-20"></a>

### bufferWhen

声明：`dist/types/internal/operators/bufferWhen.d.ts`

    export declare function bufferWhen<T>(closingSelector: () => ObservableInput<any>): OperatorFunction<T, T[]>;

<a id="api-21"></a>

### catchError

声明：`dist/types/internal/operators/catchError.d.ts`

    export declare function catchError<T, O extends ObservableInput<any>>(selector: (err: any, caught: Observable<T>) => O): OperatorFunction<T, T | ObservedValueOf<O>>;

<a id="api-22"></a>

### combineAll

声明：`dist/types/internal/operators/combineAll.d.ts`

    [弃用] combineAll: typeof combineLatestAll

<a id="api-23"></a>

### combineLatest

声明：`dist/types/internal/observable/combineLatest.d.ts`

    export declare function combineLatest<T extends AnyCatcher>(arg: T): Observable<unknown>;
    export declare function combineLatest(sources: []): Observable<never>;
    export declare function combineLatest<A extends readonly unknown[]>(sources: readonly [...ObservableInputTuple<A>]): Observable<A>;
    [弃用] export declare function combineLatest<A extends readonly unknown[], R>(sources: readonly [...ObservableInputTuple<A>], resultSelector: (...values: A) => R, scheduler: SchedulerLike): Observable<R>;
    export declare function combineLatest<A extends readonly unknown[], R>(sources: readonly [...ObservableInputTuple<A>], resultSelector: (...values: A) => R): Observable<R>;
    [弃用] export declare function combineLatest<A extends readonly unknown[]>(sources: readonly [...ObservableInputTuple<A>], scheduler: SchedulerLike): Observable<A>;
    [弃用] export declare function combineLatest<A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): Observable<A>;
    [弃用] export declare function combineLatest<A extends readonly unknown[], R>(...sourcesAndResultSelectorAndScheduler: [...ObservableInputTuple<A>, (...values: A) => R, SchedulerLike]): Observable<R>;
    [弃用] export declare function combineLatest<A extends readonly unknown[], R>(...sourcesAndResultSelector: [...ObservableInputTuple<A>, (...values: A) => R]): Observable<R>;
    [弃用] export declare function combineLatest<A extends readonly unknown[]>(...sourcesAndScheduler: [...ObservableInputTuple<A>, SchedulerLike]): Observable<A>;
    export declare function combineLatest(sourcesObject: { [K in any]: never; }): Observable<never>;
    export declare function combineLatest<T extends Record<string, ObservableInput<any>>>(sourcesObject: T): Observable<{ [K in keyof T]: ObservedValueOf<T[K]>; }>;

<a id="api-24"></a>

### combineLatestAll

声明：`dist/types/internal/operators/combineLatestAll.d.ts`

    export declare function combineLatestAll<T>(): OperatorFunction<ObservableInput<T>, T[]>;
    export declare function combineLatestAll<T>(): OperatorFunction<any, T[]>;
    export declare function combineLatestAll<T, R>(project: (...values: T[]) => R): OperatorFunction<ObservableInput<T>, R>;
    export declare function combineLatestAll<R>(project: (...values: Array<any>) => R): OperatorFunction<any, R>;

<a id="api-25"></a>

### combineLatestWith

声明：`dist/types/internal/operators/combineLatestWith.d.ts`

    export declare function combineLatestWith<T, A extends readonly unknown[]>(...otherSources: [...ObservableInputTuple<A>]): OperatorFunction<T, Cons<T, A>>;

<a id="api-26"></a>

### CompleteNotification

声明：`dist/types/internal/types.d.ts`

    export interface CompleteNotification
    kind: 'C';

<a id="api-27"></a>

### CompletionObserver

声明：`dist/types/internal/types.d.ts`

    export interface CompletionObserver<T>
    closed?: boolean;
    next?: (value: T) => void;
    error?: (err: any) => void;
    complete: () => void;

<a id="api-28"></a>

### concat

声明：`dist/types/internal/observable/concat.d.ts`

    export declare function concat<T extends readonly unknown[]>(...inputs: [...ObservableInputTuple<T>]): Observable<T[number]>;
    export declare function concat<T extends readonly unknown[]>(...inputsAndScheduler: [...ObservableInputTuple<T>, SchedulerLike]): Observable<T[number]>;

<a id="api-29"></a>

### concatAll

声明：`dist/types/internal/operators/concatAll.d.ts`

    export declare function concatAll<O extends ObservableInput<any>>(): OperatorFunction<O, ObservedValueOf<O>>;

<a id="api-30"></a>

### concatMap

声明：`dist/types/internal/operators/concatMap.d.ts`

    export declare function concatMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function concatMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: undefined): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function concatMap<T, R, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R): OperatorFunction<T, R>;

<a id="api-31"></a>

### concatMapTo

声明：`dist/types/internal/operators/concatMapTo.d.ts`

    [弃用] export declare function concatMapTo<O extends ObservableInput<unknown>>(observable: O): OperatorFunction<unknown, ObservedValueOf<O>>;
    [弃用] export declare function concatMapTo<O extends ObservableInput<unknown>>(observable: O, resultSelector: undefined): OperatorFunction<unknown, ObservedValueOf<O>>;
    [弃用] export declare function concatMapTo<T, R, O extends ObservableInput<unknown>>(observable: O, resultSelector: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R): OperatorFunction<T, R>;

<a id="api-32"></a>

### concatWith

声明：`dist/types/internal/operators/concatWith.d.ts`

    export declare function concatWith<T, A extends readonly unknown[]>(...otherSources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;

<a id="api-33"></a>

### config

声明：`dist/types/internal/config.d.ts`

    config: GlobalConfig

<a id="api-34"></a>

### connect

声明：`dist/types/internal/operators/connect.d.ts`

    export declare function connect<T, O extends ObservableInput<unknown>>(selector: (shared: Observable<T>) => O, config?: ConnectConfig<T>): OperatorFunction<T, ObservedValueOf<O>>;

<a id="api-35"></a>

### connectable

声明：`dist/types/internal/observable/connectable.d.ts`

    export declare function connectable<T>(source: ObservableInput<T>, config?: ConnectableConfig<T>): Connectable<T>;

<a id="api-36"></a>

### Connectable

声明：`dist/types/internal/types.d.ts`

    export interface Connectable<T> extends Observable<T>
    connect(): Subscription;

<a id="api-37"></a>

### ConnectableObservable

声明：`dist/types/internal/observable/ConnectableObservable.d.ts`

    export declare class ConnectableObservable<T> extends Observable<T>
    source: Observable<T>;
    [弃用] constructor(source: Observable<T>, subjectFactory: () => Subject<T>);
    [弃用] connect(): Subscription;
    [弃用] refCount(): Observable<T>;

<a id="api-38"></a>

### ConnectConfig

声明：`dist/types/internal/operators/connect.d.ts`

    export interface ConnectConfig<T>
    connector: () => SubjectLike<T>;

<a id="api-39"></a>

### Cons

声明：`dist/types/internal/types.d.ts`

    export declare type Cons<X, Y extends readonly any[]> = ((arg: X, ...rest: Y) => any) extends (...args: infer U) => any ? U : never;

<a id="api-40"></a>

### count

声明：`dist/types/internal/operators/count.d.ts`

    export declare function count<T>(predicate?: (value: T, index: number) => boolean): OperatorFunction<T, number>;

<a id="api-41"></a>

### debounce

声明：`dist/types/internal/operators/debounce.d.ts`

    export declare function debounce<T>(durationSelector: (value: T) => ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-42"></a>

### debounceTime

声明：`dist/types/internal/operators/debounceTime.d.ts`

    export declare function debounceTime<T>(dueTime: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;

<a id="api-43"></a>

### defaultIfEmpty

声明：`dist/types/internal/operators/defaultIfEmpty.d.ts`

    export declare function defaultIfEmpty<T, R>(defaultValue: R): OperatorFunction<T, T | R>;

<a id="api-44"></a>

### defer

声明：`dist/types/internal/observable/defer.d.ts`

    export declare function defer<R extends ObservableInput<any>>(observableFactory: () => R): Observable<ObservedValueOf<R>>;

<a id="api-45"></a>

### delay

声明：`dist/types/internal/operators/delay.d.ts`

    export declare function delay<T>(due: number | Date, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;

<a id="api-46"></a>

### delayWhen

声明：`dist/types/internal/operators/delayWhen.d.ts`

    [弃用] export declare function delayWhen<T>(delayDurationSelector: (value: T, index: number) => ObservableInput<any>, subscriptionDelay: Observable<any>): MonoTypeOperatorFunction<T>;
    export declare function delayWhen<T>(delayDurationSelector: (value: T, index: number) => ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-47"></a>

### dematerialize

声明：`dist/types/internal/operators/dematerialize.d.ts`

    export declare function dematerialize<N extends ObservableNotification<any>>(): OperatorFunction<N, ValueFromNotification<N>>;

<a id="api-48"></a>

### distinct

声明：`dist/types/internal/operators/distinct.d.ts`

    export declare function distinct<T, K>(keySelector?: (value: T) => K, flushes?: ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-49"></a>

### distinctUntilChanged

声明：`dist/types/internal/operators/distinctUntilChanged.d.ts`

    export declare function distinctUntilChanged<T>(comparator?: (previous: T, current: T) => boolean): MonoTypeOperatorFunction<T>;
    export declare function distinctUntilChanged<T, K>(comparator: (previous: K, current: K) => boolean, keySelector: (value: T) => K): MonoTypeOperatorFunction<T>;

<a id="api-50"></a>

### distinctUntilKeyChanged

声明：`dist/types/internal/operators/distinctUntilKeyChanged.d.ts`

    export declare function distinctUntilKeyChanged<T>(key: keyof T): MonoTypeOperatorFunction<T>;
    export declare function distinctUntilKeyChanged<T, K extends keyof T>(key: K, compare: (x: T[K], y: T[K]) => boolean): MonoTypeOperatorFunction<T>;

<a id="api-51"></a>

### elementAt

声明：`dist/types/internal/operators/elementAt.d.ts`

    export declare function elementAt<T, D = T>(index: number, defaultValue?: D): OperatorFunction<T, T | D>;

<a id="api-52"></a>

### empty

声明：`dist/types/internal/observable/empty.d.ts`

    [弃用] export declare function empty(scheduler?: SchedulerLike): Observable<never>;

<a id="api-53"></a>

### EMPTY

声明：`dist/types/internal/observable/empty.d.ts`

    EMPTY: Observable<never>

<a id="api-54"></a>

### EmptyError

声明：`dist/types/internal/util/EmptyError.d.ts`

    export interface EmptyError extends Error
    EmptyError: EmptyErrorCtor

<a id="api-55"></a>

### endWith

声明：`dist/types/internal/operators/endWith.d.ts`

    [弃用] export declare function endWith<T>(scheduler: SchedulerLike): MonoTypeOperatorFunction<T>;
    [弃用] export declare function endWith<T, A extends unknown[] = T[]>(...valuesAndScheduler: [...A, SchedulerLike]): OperatorFunction<T, T | ValueFromArray<A>>;
    export declare function endWith<T, A extends unknown[] = T[]>(...values: A): OperatorFunction<T, T | ValueFromArray<A>>;

<a id="api-56"></a>

### ErrorNotification

声明：`dist/types/internal/types.d.ts`

    export interface ErrorNotification
    kind: 'E';
    error: any;

<a id="api-57"></a>

### ErrorObserver

声明：`dist/types/internal/types.d.ts`

    export interface ErrorObserver<T>
    closed?: boolean;
    next?: (value: T) => void;
    error: (err: any) => void;
    complete?: () => void;

<a id="api-58"></a>

### every

声明：`dist/types/internal/operators/every.d.ts`

    export declare function every<T>(predicate: BooleanConstructor): OperatorFunction<T, Exclude<T, Falsy> extends never ? false : boolean>;
    [弃用] export declare function every<T>(predicate: BooleanConstructor, thisArg: any): OperatorFunction<T, Exclude<T, Falsy> extends never ? false : boolean>;
    [弃用] export declare function every<T, A>(predicate: (this: A, value: T, index: number, source: Observable<T>) => boolean, thisArg: A): OperatorFunction<T, boolean>;
    export declare function every<T>(predicate: (value: T, index: number, source: Observable<T>) => boolean): OperatorFunction<T, boolean>;

<a id="api-59"></a>

### exhaust

声明：`dist/types/internal/operators/exhaust.d.ts`

    [弃用] exhaust: typeof exhaustAll

<a id="api-60"></a>

### exhaustAll

声明：`dist/types/internal/operators/exhaustAll.d.ts`

    export declare function exhaustAll<O extends ObservableInput<any>>(): OperatorFunction<O, ObservedValueOf<O>>;

<a id="api-61"></a>

### exhaustMap

声明：`dist/types/internal/operators/exhaustMap.d.ts`

    export declare function exhaustMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function exhaustMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: undefined): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function exhaustMap<T, I, R>(project: (value: T, index: number) => ObservableInput<I>, resultSelector: (outerValue: T, innerValue: I, outerIndex: number, innerIndex: number) => R): OperatorFunction<T, R>;

<a id="api-62"></a>

### expand

声明：`dist/types/internal/operators/expand.d.ts`

    export declare function expand<T, O extends ObservableInput<unknown>>(project: (value: T, index: number) => O, concurrent?: number, scheduler?: SchedulerLike): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function expand<T, O extends ObservableInput<unknown>>(project: (value: T, index: number) => O, concurrent: number | undefined, scheduler: SchedulerLike): OperatorFunction<T, ObservedValueOf<O>>;

<a id="api-63"></a>

### FactoryOrValue

声明：`dist/types/internal/types.d.ts`

    export declare type FactoryOrValue<T> = T | (() => T);

<a id="api-64"></a>

### Falsy

声明：`dist/types/internal/types.d.ts`

    export declare type Falsy = null | undefined | false | 0 | -0 | 0n | '';

<a id="api-65"></a>

### filter

声明：`dist/types/internal/operators/filter.d.ts`

    [弃用] export declare function filter<T, S extends T, A>(predicate: (this: A, value: T, index: number) => value is S, thisArg: A): OperatorFunction<T, S>;
    export declare function filter<T, S extends T>(predicate: (value: T, index: number) => value is S): OperatorFunction<T, S>;
    export declare function filter<T>(predicate: BooleanConstructor): OperatorFunction<T, TruthyTypesOf<T>>;
    [弃用] export declare function filter<T, A>(predicate: (this: A, value: T, index: number) => boolean, thisArg: A): MonoTypeOperatorFunction<T>;
    export declare function filter<T>(predicate: (value: T, index: number) => boolean): MonoTypeOperatorFunction<T>;

<a id="api-66"></a>

### finalize

声明：`dist/types/internal/operators/finalize.d.ts`

    export declare function finalize<T>(callback: () => void): MonoTypeOperatorFunction<T>;

<a id="api-67"></a>

### find

声明：`dist/types/internal/operators/find.d.ts`

    export declare function find<T>(predicate: BooleanConstructor): OperatorFunction<T, TruthyTypesOf<T>>;
    [弃用] export declare function find<T, S extends T, A>(predicate: (this: A, value: T, index: number, source: Observable<T>) => value is S, thisArg: A): OperatorFunction<T, S | undefined>;
    export declare function find<T, S extends T>(predicate: (value: T, index: number, source: Observable<T>) => value is S): OperatorFunction<T, S | undefined>;
    [弃用] export declare function find<T, A>(predicate: (this: A, value: T, index: number, source: Observable<T>) => boolean, thisArg: A): OperatorFunction<T, T | undefined>;
    export declare function find<T>(predicate: (value: T, index: number, source: Observable<T>) => boolean): OperatorFunction<T, T | undefined>;

<a id="api-68"></a>

### findIndex

声明：`dist/types/internal/operators/findIndex.d.ts`

    export declare function findIndex<T>(predicate: BooleanConstructor): OperatorFunction<T, T extends Falsy ? -1 : number>;
    [弃用] export declare function findIndex<T>(predicate: BooleanConstructor, thisArg: any): OperatorFunction<T, T extends Falsy ? -1 : number>;
    [弃用] export declare function findIndex<T, A>(predicate: (this: A, value: T, index: number, source: Observable<T>) => boolean, thisArg: A): OperatorFunction<T, number>;
    export declare function findIndex<T>(predicate: (value: T, index: number, source: Observable<T>) => boolean): OperatorFunction<T, number>;

<a id="api-69"></a>

### first

声明：`dist/types/internal/operators/first.d.ts`

    export declare function first<T, D = T>(predicate?: null, defaultValue?: D): OperatorFunction<T, T | D>;
    export declare function first<T>(predicate: BooleanConstructor): OperatorFunction<T, TruthyTypesOf<T>>;
    export declare function first<T, D>(predicate: BooleanConstructor, defaultValue: D): OperatorFunction<T, TruthyTypesOf<T> | D>;
    export declare function first<T, S extends T>(predicate: (value: T, index: number, source: Observable<T>) => value is S, defaultValue?: S): OperatorFunction<T, S>;
    export declare function first<T, S extends T, D>(predicate: (value: T, index: number, source: Observable<T>) => value is S, defaultValue: D): OperatorFunction<T, S | D>;
    export declare function first<T, D = T>(predicate: (value: T, index: number, source: Observable<T>) => boolean, defaultValue?: D): OperatorFunction<T, T | D>;

<a id="api-70"></a>

### firstValueFrom

声明：`dist/types/internal/firstValueFrom.d.ts`

    export declare function firstValueFrom<T, D>(source: Observable<T>, config: FirstValueFromConfig<D>): Promise<T | D>;
    export declare function firstValueFrom<T>(source: Observable<T>): Promise<T>;

<a id="api-71"></a>

### flatMap

声明：`dist/types/internal/operators/flatMap.d.ts`

    [弃用] flatMap: typeof mergeMap

<a id="api-72"></a>

### forkJoin

声明：`dist/types/internal/observable/forkJoin.d.ts`

    export declare function forkJoin<T extends AnyCatcher>(arg: T): Observable<unknown>;
    export declare function forkJoin(scheduler: null | undefined): Observable<never>;
    export declare function forkJoin(sources: readonly []): Observable<never>;
    export declare function forkJoin<A extends readonly unknown[]>(sources: readonly [...ObservableInputTuple<A>]): Observable<A>;
    export declare function forkJoin<A extends readonly unknown[], R>(sources: readonly [...ObservableInputTuple<A>], resultSelector: (...values: A) => R): Observable<R>;
    [弃用] export declare function forkJoin<A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): Observable<A>;
    [弃用] export declare function forkJoin<A extends readonly unknown[], R>(...sourcesAndResultSelector: [...ObservableInputTuple<A>, (...values: A) => R]): Observable<R>;
    export declare function forkJoin(sourcesObject: { [K in any]: never; }): Observable<never>;
    export declare function forkJoin<T extends Record<string, ObservableInput<any>>>(sourcesObject: T): Observable<{ [K in keyof T]: ObservedValueOf<T[K]>; }>;

<a id="api-73"></a>

### from

声明：`dist/types/internal/observable/from.d.ts`

    export declare function from<O extends ObservableInput<any>>(input: O): Observable<ObservedValueOf<O>>;
    [弃用] export declare function from<O extends ObservableInput<any>>(input: O, scheduler: SchedulerLike | undefined): Observable<ObservedValueOf<O>>;

<a id="api-74"></a>

### fromEvent

声明：`dist/types/internal/observable/fromEvent.d.ts`

    export declare function fromEvent<T>(target: HasEventTargetAddRemove<T> | ArrayLike<HasEventTargetAddRemove<T>>, eventName: string): Observable<T>;
    export declare function fromEvent<T, R>(target: HasEventTargetAddRemove<T> | ArrayLike<HasEventTargetAddRemove<T>>, eventName: string, resultSelector: (event: T) => R): Observable<R>;
    export declare function fromEvent<T>(target: HasEventTargetAddRemove<T> | ArrayLike<HasEventTargetAddRemove<T>>, eventName: string, options: EventListenerOptions): Observable<T>;
    export declare function fromEvent<T, R>(target: HasEventTargetAddRemove<T> | ArrayLike<HasEventTargetAddRemove<T>>, eventName: string, options: EventListenerOptions, resultSelector: (event: T) => R): Observable<R>;
    export declare function fromEvent(target: NodeStyleEventEmitter | ArrayLike<NodeStyleEventEmitter>, eventName: string): Observable<unknown>;
    [弃用] export declare function fromEvent<T>(target: NodeStyleEventEmitter | ArrayLike<NodeStyleEventEmitter>, eventName: string): Observable<T>;
    export declare function fromEvent<R>(target: NodeStyleEventEmitter | ArrayLike<NodeStyleEventEmitter>, eventName: string, resultSelector: (...args: any[]) => R): Observable<R>;
    export declare function fromEvent(target: NodeCompatibleEventEmitter | ArrayLike<NodeCompatibleEventEmitter>, eventName: string): Observable<unknown>;
    [弃用] export declare function fromEvent<T>(target: NodeCompatibleEventEmitter | ArrayLike<NodeCompatibleEventEmitter>, eventName: string): Observable<T>;
    export declare function fromEvent<R>(target: NodeCompatibleEventEmitter | ArrayLike<NodeCompatibleEventEmitter>, eventName: string, resultSelector: (...args: any[]) => R): Observable<R>;
    export declare function fromEvent<T>(target: JQueryStyleEventEmitter<any, T> | ArrayLike<JQueryStyleEventEmitter<any, T>>, eventName: string): Observable<T>;
    export declare function fromEvent<T, R>(target: JQueryStyleEventEmitter<any, T> | ArrayLike<JQueryStyleEventEmitter<any, T>>, eventName: string, resultSelector: (value: T, ...args: any[]) => R): Observable<R>;

<a id="api-75"></a>

### fromEventPattern

声明：`dist/types/internal/observable/fromEventPattern.d.ts`

    export declare function fromEventPattern<T>(addHandler: (handler: NodeEventHandler) => any, removeHandler?: (handler: NodeEventHandler, signal?: any) => void): Observable<T>;
    export declare function fromEventPattern<T>(addHandler: (handler: NodeEventHandler) => any, removeHandler?: (handler: NodeEventHandler, signal?: any) => void, resultSelector?: (...args: any[]) => T): Observable<T>;

<a id="api-76"></a>

### generate

声明：`dist/types/internal/observable/generate.d.ts`

    [弃用] export declare function generate<T, S>(initialState: S, condition: ConditionFunc<S>, iterate: IterateFunc<S>, resultSelector: ResultFunc<S, T>, scheduler?: SchedulerLike): Observable<T>;
    [弃用] export declare function generate<S>(initialState: S, condition: ConditionFunc<S>, iterate: IterateFunc<S>, scheduler?: SchedulerLike): Observable<S>;
    export declare function generate<S>(options: GenerateBaseOptions<S>): Observable<S>;
    export declare function generate<T, S>(options: GenerateOptions<T, S>): Observable<T>;

<a id="api-77"></a>

### GlobalConfig

声明：`dist/types/internal/config.d.ts`

    export interface GlobalConfig
    onUnhandledError: ((err: any) => void) | null;
    onStoppedNotification: ((notification: ObservableNotification<any>, subscriber: Subscriber<any>) => void) | null;
    [弃用] Promise?: PromiseConstructorLike;
    [弃用] useDeprecatedSynchronousErrorHandling: boolean;
    [弃用] useDeprecatedNextContext: boolean;

<a id="api-78"></a>

### groupBy

声明：`dist/types/internal/operators/groupBy.d.ts`

    export declare function groupBy<T, K>(key: (value: T) => K, options: BasicGroupByOptions<K, T>): OperatorFunction<T, GroupedObservable<K, T>>;
    export declare function groupBy<T, K, E>(key: (value: T) => K, options: GroupByOptionsWithElement<K, E, T>): OperatorFunction<T, GroupedObservable<K, E>>;
    export declare function groupBy<T, K extends T>(key: (value: T) => value is K): OperatorFunction<T, GroupedObservable<true, K> | GroupedObservable<false, Exclude<T, K>>>;
    export declare function groupBy<T, K>(key: (value: T) => K): OperatorFunction<T, GroupedObservable<K, T>>;
    [弃用] export declare function groupBy<T, K>(key: (value: T) => K, element: void, duration: (grouped: GroupedObservable<K, T>) => Observable<any>): OperatorFunction<T, GroupedObservable<K, T>>;
    [弃用] export declare function groupBy<T, K, R>(key: (value: T) => K, element?: (value: T) => R, duration?: (grouped: GroupedObservable<K, R>) => Observable<any>): OperatorFunction<T, GroupedObservable<K, R>>;
    [弃用] export declare function groupBy<T, K, R>(key: (value: T) => K, element?: (value: T) => R, duration?: (grouped: GroupedObservable<K, R>) => Observable<any>, connector?: () => Subject<R>): OperatorFunction<T, GroupedObservable<K, R>>;

<a id="api-79"></a>

### GroupByOptionsWithElement

声明：`dist/types/internal/operators/groupBy.d.ts`

    export interface GroupByOptionsWithElement<K, E, T>
    element: (value: T) => E;
    duration?: (grouped: GroupedObservable<K, E>) => ObservableInput<any>;
    connector?: () => SubjectLike<E>;

<a id="api-80"></a>

### GroupedObservable

声明：`dist/types/internal/operators/groupBy.d.ts`

    export interface GroupedObservable<K, T> extends Observable<T>
    readonly key: K;

<a id="api-81"></a>

### Head

声明：`dist/types/internal/types.d.ts`

    export declare type Head<X extends readonly any[]> = ((...args: X) => any) extends (arg: infer U, ...rest: any[]) => any ? U : never;

<a id="api-82"></a>

### identity

声明：`dist/types/internal/util/identity.d.ts`

    export declare function identity<T>(x: T): T;

<a id="api-83"></a>

### ignoreElements

声明：`dist/types/internal/operators/ignoreElements.d.ts`

    export declare function ignoreElements(): OperatorFunction<unknown, never>;

<a id="api-84"></a>

### iif

声明：`dist/types/internal/observable/iif.d.ts`

    export declare function iif<T, F>(condition: () => boolean, trueResult: ObservableInput<T>, falseResult: ObservableInput<F>): Observable<T | F>;

<a id="api-85"></a>

### InteropObservable

声明：`dist/types/internal/types.d.ts`

    export interface InteropObservable<T>
    [Symbol.observable]: () => Subscribable<T>;

<a id="api-86"></a>

### interval

声明：`dist/types/internal/observable/interval.d.ts`

    export declare function interval(period?: number, scheduler?: SchedulerLike): Observable<number>;

<a id="api-87"></a>

### isEmpty

声明：`dist/types/internal/operators/isEmpty.d.ts`

    export declare function isEmpty<T>(): OperatorFunction<T, boolean>;

<a id="api-88"></a>

### isObservable

声明：`dist/types/internal/util/isObservable.d.ts`

    export declare function isObservable(obj: any): obj is Observable<unknown>;

<a id="api-89"></a>

### last

声明：`dist/types/internal/operators/last.d.ts`

    export declare function last<T>(predicate: BooleanConstructor): OperatorFunction<T, TruthyTypesOf<T>>;
    export declare function last<T, D>(predicate: BooleanConstructor, defaultValue: D): OperatorFunction<T, TruthyTypesOf<T> | D>;
    export declare function last<T, D = T>(predicate?: null, defaultValue?: D): OperatorFunction<T, T | D>;
    export declare function last<T, S extends T>(predicate: (value: T, index: number, source: Observable<T>) => value is S, defaultValue?: S): OperatorFunction<T, S>;
    export declare function last<T, D = T>(predicate: (value: T, index: number, source: Observable<T>) => boolean, defaultValue?: D): OperatorFunction<T, T | D>;

<a id="api-90"></a>

### lastValueFrom

声明：`dist/types/internal/lastValueFrom.d.ts`

    export declare function lastValueFrom<T, D>(source: Observable<T>, config: LastValueFromConfig<D>): Promise<T | D>;
    export declare function lastValueFrom<T>(source: Observable<T>): Promise<T>;

<a id="api-91"></a>

### map

声明：`dist/types/internal/operators/map.d.ts`

    export declare function map<T, R>(project: (value: T, index: number) => R): OperatorFunction<T, R>;
    [弃用] export declare function map<T, R, A>(project: (this: A, value: T, index: number) => R, thisArg: A): OperatorFunction<T, R>;

<a id="api-92"></a>

### mapTo

声明：`dist/types/internal/operators/mapTo.d.ts`

    [弃用] export declare function mapTo<R>(value: R): OperatorFunction<unknown, R>;
    [弃用] export declare function mapTo<T, R>(value: R): OperatorFunction<T, R>;

<a id="api-93"></a>

### materialize

声明：`dist/types/internal/operators/materialize.d.ts`

    export declare function materialize<T>(): OperatorFunction<T, Notification<T> & ObservableNotification<T>>;

<a id="api-94"></a>

### max

声明：`dist/types/internal/operators/max.d.ts`

    export declare function max<T>(comparer?: (x: T, y: T) => number): MonoTypeOperatorFunction<T>;

<a id="api-95"></a>

### merge

声明：`dist/types/internal/observable/merge.d.ts`

    export declare function merge<A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): Observable<A[number]>;
    export declare function merge<A extends readonly unknown[]>(...sourcesAndConcurrency: [...ObservableInputTuple<A>, number?]): Observable<A[number]>;
    [弃用] export declare function merge<A extends readonly unknown[]>(...sourcesAndScheduler: [...ObservableInputTuple<A>, SchedulerLike?]): Observable<A[number]>;
    [弃用] export declare function merge<A extends readonly unknown[]>(...sourcesAndConcurrencyAndScheduler: [...ObservableInputTuple<A>, number?, SchedulerLike?]): Observable<A[number]>;

<a id="api-96"></a>

### mergeAll

声明：`dist/types/internal/operators/mergeAll.d.ts`

    export declare function mergeAll<O extends ObservableInput<any>>(concurrent?: number): OperatorFunction<O, ObservedValueOf<O>>;

<a id="api-97"></a>

### mergeMap

声明：`dist/types/internal/operators/mergeMap.d.ts`

    export declare function mergeMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O, concurrent?: number): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function mergeMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: undefined, concurrent?: number): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function mergeMap<T, R, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R, concurrent?: number): OperatorFunction<T, R>;

<a id="api-98"></a>

### mergeMapTo

声明：`dist/types/internal/operators/mergeMapTo.d.ts`

    [弃用] export declare function mergeMapTo<O extends ObservableInput<unknown>>(innerObservable: O, concurrent?: number): OperatorFunction<unknown, ObservedValueOf<O>>;
    [弃用] export declare function mergeMapTo<T, R, O extends ObservableInput<unknown>>(innerObservable: O, resultSelector: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R, concurrent?: number): OperatorFunction<T, R>;

<a id="api-99"></a>

### mergeScan

声明：`dist/types/internal/operators/mergeScan.d.ts`

    export declare function mergeScan<T, R>(accumulator: (acc: R, value: T, index: number) => ObservableInput<R>, seed: R, concurrent?: number): OperatorFunction<T, R>;

<a id="api-100"></a>

### mergeWith

声明：`dist/types/internal/operators/mergeWith.d.ts`

    export declare function mergeWith<T, A extends readonly unknown[]>(...otherSources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;

<a id="api-101"></a>

### min

声明：`dist/types/internal/operators/min.d.ts`

    export declare function min<T>(comparer?: (x: T, y: T) => number): MonoTypeOperatorFunction<T>;

<a id="api-102"></a>

### MonoTypeOperatorFunction

声明：`dist/types/internal/types.d.ts`

    export interface MonoTypeOperatorFunction<T> extends OperatorFunction<T, T>

<a id="api-103"></a>

### multicast

声明：`dist/types/internal/operators/multicast.d.ts`

    [弃用] export declare function multicast<T>(subject: Subject<T>): UnaryFunction<Observable<T>, ConnectableObservable<T>>;
    [弃用] export declare function multicast<T, O extends ObservableInput<any>>(subject: Subject<T>, selector: (shared: Observable<T>) => O): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function multicast<T>(subjectFactory: () => Subject<T>): UnaryFunction<Observable<T>, ConnectableObservable<T>>;
    [弃用] export declare function multicast<T, O extends ObservableInput<any>>(subjectFactory: () => Subject<T>, selector: (shared: Observable<T>) => O): OperatorFunction<T, ObservedValueOf<O>>;

<a id="api-104"></a>

### never

声明：`dist/types/internal/observable/never.d.ts`

    [弃用] export declare function never(): Observable<never>;

<a id="api-105"></a>

### NEVER

声明：`dist/types/internal/observable/never.d.ts`

    NEVER: Observable<never>

<a id="api-106"></a>

### NextNotification

声明：`dist/types/internal/types.d.ts`

    export interface NextNotification<T>
    kind: 'N';
    value: T;

<a id="api-107"></a>

### NextObserver

声明：`dist/types/internal/types.d.ts`

    export interface NextObserver<T>
    closed?: boolean;
    next: (value: T) => void;
    error?: (err: any) => void;
    complete?: () => void;

<a id="api-108"></a>

### noop

声明：`dist/types/internal/util/noop.d.ts`

    export declare function noop(): void;

<a id="api-109"></a>

### NotFoundError

声明：`dist/types/internal/util/NotFoundError.d.ts`

    export interface NotFoundError extends Error
    NotFoundError: NotFoundErrorCtor

<a id="api-110"></a>

### Notification

声明：`dist/types/internal/Notification.d.ts`

    export declare class Notification<T>
    readonly kind: 'N' | 'E' | 'C';
    readonly value?: T | undefined;
    readonly error?: any;
    [弃用] readonly hasValue: boolean;
    [弃用] constructor(kind: 'N', value?: T);
    [弃用] constructor(kind: 'E', value: undefined, error: any);
    [弃用] constructor(kind: 'C');
    observe(observer: PartialObserver<T>): void;
    [弃用] do(next: (value: T) => void, error: (err: any) => void, complete: () => void): void;
    [弃用] do(next: (value: T) => void, error: (err: any) => void): void;
    [弃用] do(next: (value: T) => void): void;
    [弃用] accept(next: (value: T) => void, error: (err: any) => void, complete: () => void): void;
    [弃用] accept(next: (value: T) => void, error: (err: any) => void): void;
    [弃用] accept(next: (value: T) => void): void;
    [弃用] accept(observer: PartialObserver<T>): void;
    [弃用] toObservable(): Observable<T>;
    [弃用] static createNext<T>(value: T): Notification<T> & NextNotification<T>;
    [弃用] static createError(err?: any): Notification<never> & ErrorNotification;
    [弃用] static createComplete(): Notification<never> & CompleteNotification;

<a id="api-111"></a>

### NotificationKind

声明：`dist/types/internal/Notification.d.ts`

    export declare enum NotificationKind
    NEXT = "N"
    ERROR = "E"
    COMPLETE = "C"

<a id="api-112"></a>

### ObjectUnsubscribedError

声明：`dist/types/internal/util/ObjectUnsubscribedError.d.ts`

    export interface ObjectUnsubscribedError extends Error
    ObjectUnsubscribedError: ObjectUnsubscribedErrorCtor

<a id="api-113"></a>

### observable

声明：`dist/types/internal/symbol/observable.d.ts`

    [弃用] observable: string | symbol

<a id="api-114"></a>

### Observable

声明：`dist/types/internal/Observable.d.ts`

    export declare class Observable<T> implements Subscribable<T>
    [弃用] source: Observable<any> | undefined;
    [弃用] operator: Operator<any, T> | undefined;
    constructor(subscribe?: (this: Observable<T>, subscriber: Subscriber<T>) => TeardownLogic);
    [弃用] static create: (...args: any[]) => any;
    [弃用] lift<R>(operator?: Operator<T, R>): Observable<R>;
    subscribe(observerOrNext?: Partial<Observer<T>> | ((value: T) => void)): Subscription;
    [弃用] subscribe(next?: ((value: T) => void) | null, error?: ((error: any) => void) | null, complete?: (() => void) | null): Subscription;
    forEach(next: (value: T) => void): Promise<void>;
    [弃用] forEach(next: (value: T) => void, promiseCtor: PromiseConstructorLike): Promise<void>;
    pipe(): Observable<T>;
    pipe<A>(op1: OperatorFunction<T, A>): Observable<A>;
    pipe<A, B>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>): Observable<B>;
    pipe<A, B, C>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>): Observable<C>;
    pipe<A, B, C, D>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>): Observable<D>;
    pipe<A, B, C, D, E>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>): Observable<E>;
    pipe<A, B, C, D, E, F>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>): Observable<F>;
    pipe<A, B, C, D, E, F, G>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>): Observable<G>;
    pipe<A, B, C, D, E, F, G, H>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>): Observable<H>;
    pipe<A, B, C, D, E, F, G, H, I>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>, op9: OperatorFunction<H, I>): Observable<I>;
    pipe<A, B, C, D, E, F, G, H, I>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>, op9: OperatorFunction<H, I>, ...operations: OperatorFunction<any, any>[]): Observable<unknown>;
    [弃用] toPromise(): Promise<T | undefined>;
    [弃用] toPromise(PromiseCtor: typeof Promise): Promise<T | undefined>;
    [弃用] toPromise(PromiseCtor: PromiseConstructorLike): Promise<T | undefined>;

<a id="api-115"></a>

### ObservableInput

声明：`dist/types/internal/types.d.ts`

    export declare type ObservableInput<T> = Observable<T> | InteropObservable<T> | AsyncIterable<T> | PromiseLike<T> | ArrayLike<T> | Iterable<T> | ReadableStreamLike<T>;

<a id="api-116"></a>

### ObservableInputTuple

声明：`dist/types/internal/types.d.ts`

    export declare type ObservableInputTuple<T> = { [K in keyof T]: ObservableInput<T[K]>; };

<a id="api-117"></a>

### ObservableLike

声明：`dist/types/internal/types.d.ts`

    [弃用] export declare type ObservableLike<T> = InteropObservable<T>;

<a id="api-118"></a>

### ObservableNotification

声明：`dist/types/internal/types.d.ts`

    export declare type ObservableNotification<T> = NextNotification<T> | ErrorNotification | CompleteNotification;

<a id="api-119"></a>

### ObservedValueOf

声明：`dist/types/internal/types.d.ts`

    export declare type ObservedValueOf<O> = O extends ObservableInput<infer T> ? T : never;

<a id="api-120"></a>

### ObservedValuesFromArray

声明：`dist/types/internal/types.d.ts`

    [弃用] export declare type ObservedValuesFromArray<X> = ObservedValueUnionFromArray<X>;

<a id="api-121"></a>

### ObservedValueTupleFromArray

声明：`dist/types/internal/types.d.ts`

    export declare type ObservedValueTupleFromArray<X> = { [K in keyof X]: ObservedValueOf<X[K]>; };

<a id="api-122"></a>

### ObservedValueUnionFromArray

声明：`dist/types/internal/types.d.ts`

    export declare type ObservedValueUnionFromArray<X> = X extends Array<ObservableInput<infer T>> ? T : never;

<a id="api-123"></a>

### observeOn

声明：`dist/types/internal/operators/observeOn.d.ts`

    export declare function observeOn<T>(scheduler: SchedulerLike, delay?: number): MonoTypeOperatorFunction<T>;

<a id="api-124"></a>

### Observer

声明：`dist/types/internal/types.d.ts`

    export interface Observer<T>
    next: (value: T) => void;
    error: (err: any) => void;
    complete: () => void;

<a id="api-125"></a>

### of

声明：`dist/types/internal/observable/of.d.ts`

    export declare function of(value: null): Observable<null>;
    export declare function of(value: undefined): Observable<undefined>;
    [弃用] export declare function of(scheduler: SchedulerLike): Observable<never>;
    [弃用] export declare function of<A extends readonly unknown[]>(...valuesAndScheduler: [...A, SchedulerLike]): Observable<ValueFromArray<A>>;
    export declare function of(): Observable<never>;
    [弃用] export declare function of<T>(): Observable<T>;
    export declare function of<T>(value: T): Observable<T>;
    export declare function of<A extends readonly unknown[]>(...values: A): Observable<ValueFromArray<A>>;

<a id="api-126"></a>

### onErrorResumeNext

声明：`dist/types/internal/observable/onErrorResumeNext.d.ts`

    export declare function onErrorResumeNext<A extends readonly unknown[]>(sources: [...ObservableInputTuple<A>]): Observable<A[number]>;
    export declare function onErrorResumeNext<A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): Observable<A[number]>;

<a id="api-127"></a>

### onErrorResumeNextWith

声明：`dist/types/internal/operators/onErrorResumeNextWith.d.ts`

    export declare function onErrorResumeNextWith<T, A extends readonly unknown[]>(sources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;
    export declare function onErrorResumeNextWith<T, A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;

<a id="api-128"></a>

### Operator

声明：`dist/types/internal/Operator.d.ts`

    export interface Operator<T, R>
    call(subscriber: Subscriber<R>, source: any): TeardownLogic;

<a id="api-129"></a>

### OperatorFunction

声明：`dist/types/internal/types.d.ts`

    export interface OperatorFunction<T, R> extends UnaryFunction<Observable<T>, Observable<R>>

<a id="api-130"></a>

### pairs

声明：`dist/types/internal/observable/pairs.d.ts`

    [弃用] export declare function pairs<T>(arr: readonly T[], scheduler?: SchedulerLike): Observable<[string, T]>;
    [弃用] export declare function pairs<O extends Record<string, unknown>>(obj: O, scheduler?: SchedulerLike): Observable<[keyof O, O[keyof O]]>;
    [弃用] export declare function pairs<T>(iterable: Iterable<T>, scheduler?: SchedulerLike): Observable<[string, T]>;
    [弃用] export declare function pairs(n: number | bigint | boolean | ((...args: any[]) => any) | symbol, scheduler?: SchedulerLike): Observable<[never, never]>;

<a id="api-131"></a>

### pairwise

声明：`dist/types/internal/operators/pairwise.d.ts`

    export declare function pairwise<T>(): OperatorFunction<T, [T, T]>;

<a id="api-132"></a>

### PartialObserver

声明：`dist/types/internal/types.d.ts`

    export declare type PartialObserver<T> = NextObserver<T> | ErrorObserver<T> | CompletionObserver<T>;

<a id="api-133"></a>

### partition

声明：`dist/types/internal/observable/partition.d.ts`

    [弃用] export declare function partition<T, U extends T, A>(source: ObservableInput<T>, predicate: (this: A, value: T, index: number) => value is U, thisArg: A): [Observable<U>, Observable<Exclude<T, U>>];
    export declare function partition<T, U extends T>(source: ObservableInput<T>, predicate: (value: T, index: number) => value is U): [Observable<U>, Observable<Exclude<T, U>>];
    [弃用] export declare function partition<T, A>(source: ObservableInput<T>, predicate: (this: A, value: T, index: number) => boolean, thisArg: A): [Observable<T>, Observable<T>];
    export declare function partition<T>(source: ObservableInput<T>, predicate: (value: T, index: number) => boolean): [Observable<T>, Observable<T>];

<a id="api-134"></a>

### pipe

声明：`dist/types/internal/util/pipe.d.ts`

    export declare function pipe(): typeof identity;
    export declare function pipe<T, A>(fn1: UnaryFunction<T, A>): UnaryFunction<T, A>;
    export declare function pipe<T, A, B>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>): UnaryFunction<T, B>;
    export declare function pipe<T, A, B, C>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>): UnaryFunction<T, C>;
    export declare function pipe<T, A, B, C, D>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>): UnaryFunction<T, D>;
    export declare function pipe<T, A, B, C, D, E>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>, fn5: UnaryFunction<D, E>): UnaryFunction<T, E>;
    export declare function pipe<T, A, B, C, D, E, F>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>, fn5: UnaryFunction<D, E>, fn6: UnaryFunction<E, F>): UnaryFunction<T, F>;
    export declare function pipe<T, A, B, C, D, E, F, G>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>, fn5: UnaryFunction<D, E>, fn6: UnaryFunction<E, F>, fn7: UnaryFunction<F, G>): UnaryFunction<T, G>;
    export declare function pipe<T, A, B, C, D, E, F, G, H>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>, fn5: UnaryFunction<D, E>, fn6: UnaryFunction<E, F>, fn7: UnaryFunction<F, G>, fn8: UnaryFunction<G, H>): UnaryFunction<T, H>;
    export declare function pipe<T, A, B, C, D, E, F, G, H, I>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>, fn5: UnaryFunction<D, E>, fn6: UnaryFunction<E, F>, fn7: UnaryFunction<F, G>, fn8: UnaryFunction<G, H>, fn9: UnaryFunction<H, I>): UnaryFunction<T, I>;
    export declare function pipe<T, A, B, C, D, E, F, G, H, I>(fn1: UnaryFunction<T, A>, fn2: UnaryFunction<A, B>, fn3: UnaryFunction<B, C>, fn4: UnaryFunction<C, D>, fn5: UnaryFunction<D, E>, fn6: UnaryFunction<E, F>, fn7: UnaryFunction<F, G>, fn8: UnaryFunction<G, H>, fn9: UnaryFunction<H, I>, ...fns: UnaryFunction<any, any>[]): UnaryFunction<T, unknown>;

<a id="api-135"></a>

### pluck

声明：`dist/types/internal/operators/pluck.d.ts`

    [弃用] export declare function pluck<T, K1 extends keyof T>(k1: K1): OperatorFunction<T, T[K1]>;
    [弃用] export declare function pluck<T, K1 extends keyof T, K2 extends keyof T[K1]>(k1: K1, k2: K2): OperatorFunction<T, T[K1][K2]>;
    [弃用] export declare function pluck<T, K1 extends keyof T, K2 extends keyof T[K1], K3 extends keyof T[K1][K2]>(k1: K1, k2: K2, k3: K3): OperatorFunction<T, T[K1][K2][K3]>;
    [弃用] export declare function pluck<T, K1 extends keyof T, K2 extends keyof T[K1], K3 extends keyof T[K1][K2], K4 extends keyof T[K1][K2][K3]>(k1: K1, k2: K2, k3: K3, k4: K4): OperatorFunction<T, T[K1][K2][K3][K4]>;
    [弃用] export declare function pluck<T, K1 extends keyof T, K2 extends keyof T[K1], K3 extends keyof T[K1][K2], K4 extends keyof T[K1][K2][K3], K5 extends keyof T[K1][K2][K3][K4]>(k1: K1, k2: K2, k3: K3, k4: K4, k5: K5): OperatorFunction<T, T[K1][K2][K3][K4][K5]>;
    [弃用] export declare function pluck<T, K1 extends keyof T, K2 extends keyof T[K1], K3 extends keyof T[K1][K2], K4 extends keyof T[K1][K2][K3], K5 extends keyof T[K1][K2][K3][K4], K6 extends keyof T[K1][K2][K3][K4][K5]>(k1: K1, k2: K2, k3: K3, k4: K4, k5: K5, k6: K6): OperatorFunction<T, T[K1][K2][K3][K4][K5][K6]>;
    [弃用] export declare function pluck<T, K1 extends keyof T, K2 extends keyof T[K1], K3 extends keyof T[K1][K2], K4 extends keyof T[K1][K2][K3], K5 extends keyof T[K1][K2][K3][K4], K6 extends keyof T[K1][K2][K3][K4][K5]>(k1: K1, k2: K2, k3: K3, k4: K4, k5: K5, k6: K6, ...rest: string[]): OperatorFunction<T, unknown>;
    [弃用] export declare function pluck<T>(...properties: string[]): OperatorFunction<T, unknown>;

<a id="api-136"></a>

### publish

声明：`dist/types/internal/operators/publish.d.ts`

    [弃用] export declare function publish<T>(): UnaryFunction<Observable<T>, ConnectableObservable<T>>;
    [弃用] export declare function publish<T, O extends ObservableInput<any>>(selector: (shared: Observable<T>) => O): OperatorFunction<T, ObservedValueOf<O>>;

<a id="api-137"></a>

### publishBehavior

声明：`dist/types/internal/operators/publishBehavior.d.ts`

    [弃用] export declare function publishBehavior<T>(initialValue: T): UnaryFunction<Observable<T>, ConnectableObservable<T>>;

<a id="api-138"></a>

### publishLast

声明：`dist/types/internal/operators/publishLast.d.ts`

    [弃用] export declare function publishLast<T>(): UnaryFunction<Observable<T>, ConnectableObservable<T>>;

<a id="api-139"></a>

### publishReplay

声明：`dist/types/internal/operators/publishReplay.d.ts`

    [弃用] export declare function publishReplay<T>(bufferSize?: number, windowTime?: number, timestampProvider?: TimestampProvider): MonoTypeOperatorFunction<T>;
    [弃用] export declare function publishReplay<T, O extends ObservableInput<any>>(bufferSize: number | undefined, windowTime: number | undefined, selector: (shared: Observable<T>) => O, timestampProvider?: TimestampProvider): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function publishReplay<T, O extends ObservableInput<any>>(bufferSize: number | undefined, windowTime: number | undefined, selector: undefined, timestampProvider: TimestampProvider): OperatorFunction<T, ObservedValueOf<O>>;

<a id="api-140"></a>

### queue

声明：`dist/types/internal/scheduler/queue.d.ts`

    [弃用] queue: QueueScheduler

<a id="api-141"></a>

### queueScheduler

声明：`dist/types/internal/scheduler/queue.d.ts`

    queueScheduler: QueueScheduler

<a id="api-142"></a>

### race

声明：`dist/types/internal/observable/race.d.ts`

    export declare function race<T extends readonly unknown[]>(inputs: [...ObservableInputTuple<T>]): Observable<T[number]>;
    export declare function race<T extends readonly unknown[]>(...inputs: [...ObservableInputTuple<T>]): Observable<T[number]>;

<a id="api-143"></a>

### raceWith

声明：`dist/types/internal/operators/raceWith.d.ts`

    export declare function raceWith<T, A extends readonly unknown[]>(...otherSources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;

<a id="api-144"></a>

### range

声明：`dist/types/internal/observable/range.d.ts`

    export declare function range(start: number, count?: number): Observable<number>;
    [弃用] export declare function range(start: number, count: number | undefined, scheduler: SchedulerLike): Observable<number>;

<a id="api-145"></a>

### ReadableStreamLike

声明：`dist/types/internal/types.d.ts`

    export interface ReadableStreamLike<T>
    getReader(): ReadableStreamDefaultReaderLike<T>;

<a id="api-146"></a>

### reduce

声明：`dist/types/internal/operators/reduce.d.ts`

    export declare function reduce<V, A = V>(accumulator: (acc: A | V, value: V, index: number) => A): OperatorFunction<V, V | A>;
    export declare function reduce<V, A>(accumulator: (acc: A, value: V, index: number) => A, seed: A): OperatorFunction<V, A>;
    export declare function reduce<V, A, S = A>(accumulator: (acc: A | S, value: V, index: number) => A, seed: S): OperatorFunction<V, A>;

<a id="api-147"></a>

### refCount

声明：`dist/types/internal/operators/refCount.d.ts`

    [弃用] export declare function refCount<T>(): MonoTypeOperatorFunction<T>;

<a id="api-148"></a>

### repeat

声明：`dist/types/internal/operators/repeat.d.ts`

    export declare function repeat<T>(countOrConfig?: number | RepeatConfig): MonoTypeOperatorFunction<T>;

<a id="api-149"></a>

### RepeatConfig

声明：`dist/types/internal/operators/repeat.d.ts`

    export interface RepeatConfig
    count?: number;
    delay?: number | ((count: number) => ObservableInput<any>);

<a id="api-150"></a>

### repeatWhen

声明：`dist/types/internal/operators/repeatWhen.d.ts`

    [弃用] export declare function repeatWhen<T>(notifier: (notifications: Observable<void>) => ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-151"></a>

### ReplaySubject

声明：`dist/types/internal/ReplaySubject.d.ts`

    export declare class ReplaySubject<T> extends Subject<T>
    constructor(_bufferSize?: number, _windowTime?: number, _timestampProvider?: TimestampProvider);
    next(value: T): void;

<a id="api-152"></a>

### retry

声明：`dist/types/internal/operators/retry.d.ts`

    export declare function retry<T>(count?: number): MonoTypeOperatorFunction<T>;
    export declare function retry<T>(config: RetryConfig): MonoTypeOperatorFunction<T>;

<a id="api-153"></a>

### RetryConfig

声明：`dist/types/internal/operators/retry.d.ts`

    export interface RetryConfig
    count?: number;
    delay?: number | ((error: any, retryCount: number) => ObservableInput<any>);
    resetOnSuccess?: boolean;

<a id="api-154"></a>

### retryWhen

声明：`dist/types/internal/operators/retryWhen.d.ts`

    [弃用] export declare function retryWhen<T>(notifier: (errors: Observable<any>) => ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-155"></a>

### sample

声明：`dist/types/internal/operators/sample.d.ts`

    export declare function sample<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-156"></a>

### sampleTime

声明：`dist/types/internal/operators/sampleTime.d.ts`

    export declare function sampleTime<T>(period: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;

<a id="api-157"></a>

### scan

声明：`dist/types/internal/operators/scan.d.ts`

    export declare function scan<V, A = V>(accumulator: (acc: A | V, value: V, index: number) => A): OperatorFunction<V, V | A>;
    export declare function scan<V, A>(accumulator: (acc: A, value: V, index: number) => A, seed: A): OperatorFunction<V, A>;
    export declare function scan<V, A, S>(accumulator: (acc: A | S, value: V, index: number) => A, seed: S): OperatorFunction<V, A>;

<a id="api-158"></a>

### scheduled

声明：`dist/types/internal/scheduled/scheduled.d.ts`

    export declare function scheduled<T>(input: ObservableInput<T>, scheduler: SchedulerLike): Observable<T>;

<a id="api-159"></a>

### Scheduler

声明：`dist/types/internal/Scheduler.d.ts`

    export declare class Scheduler implements SchedulerLike
    static now: () => number;
    constructor(schedulerActionCtor: typeof Action, now?: () => number);
    now: () => number;
    schedule<T>(work: (this: SchedulerAction<T>, state?: T) => void, delay?: number, state?: T): Subscription;

<a id="api-160"></a>

### SchedulerAction

声明：`dist/types/internal/types.d.ts`

    export interface SchedulerAction<T> extends Subscription
    schedule(state?: T, delay?: number): Subscription;

<a id="api-161"></a>

### SchedulerLike

声明：`dist/types/internal/types.d.ts`

    export interface SchedulerLike extends TimestampProvider
    schedule<T>(work: (this: SchedulerAction<T>, state: T) => void, delay: number, state: T): Subscription;
    schedule<T>(work: (this: SchedulerAction<T>, state?: T) => void, delay: number, state?: T): Subscription;
    schedule<T>(work: (this: SchedulerAction<T>, state?: T) => void, delay?: number, state?: T): Subscription;

<a id="api-162"></a>

### sequenceEqual

声明：`dist/types/internal/operators/sequenceEqual.d.ts`

    export declare function sequenceEqual<T>(compareTo: ObservableInput<T>, comparator?: (a: T, b: T) => boolean): OperatorFunction<T, boolean>;

<a id="api-163"></a>

### SequenceError

声明：`dist/types/internal/util/SequenceError.d.ts`

    export interface SequenceError extends Error
    SequenceError: SequenceErrorCtor

<a id="api-164"></a>

### share

声明：`dist/types/internal/operators/share.d.ts`

    export declare function share<T>(): MonoTypeOperatorFunction<T>;
    export declare function share<T>(options: ShareConfig<T>): MonoTypeOperatorFunction<T>;

<a id="api-165"></a>

### ShareConfig

声明：`dist/types/internal/operators/share.d.ts`

    export interface ShareConfig<T>
    connector?: () => SubjectLike<T>;
    resetOnError?: boolean | ((error: any) => ObservableInput<any>);
    resetOnComplete?: boolean | (() => ObservableInput<any>);
    resetOnRefCountZero?: boolean | (() => ObservableInput<any>);

<a id="api-166"></a>

### shareReplay

声明：`dist/types/internal/operators/shareReplay.d.ts`

    export declare function shareReplay<T>(config: ShareReplayConfig): MonoTypeOperatorFunction<T>;
    export declare function shareReplay<T>(bufferSize?: number, windowTime?: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;

<a id="api-167"></a>

### ShareReplayConfig

声明：`dist/types/internal/operators/shareReplay.d.ts`

    export interface ShareReplayConfig
    bufferSize?: number;
    windowTime?: number;
    refCount: boolean;
    scheduler?: SchedulerLike;

<a id="api-168"></a>

### single

声明：`dist/types/internal/operators/single.d.ts`

    export declare function single<T>(predicate: BooleanConstructor): OperatorFunction<T, TruthyTypesOf<T>>;
    export declare function single<T>(predicate?: (value: T, index: number, source: Observable<T>) => boolean): MonoTypeOperatorFunction<T>;

<a id="api-169"></a>

### skip

声明：`dist/types/internal/operators/skip.d.ts`

    export declare function skip<T>(count: number): MonoTypeOperatorFunction<T>;

<a id="api-170"></a>

### skipLast

声明：`dist/types/internal/operators/skipLast.d.ts`

    export declare function skipLast<T>(skipCount: number): MonoTypeOperatorFunction<T>;

<a id="api-171"></a>

### skipUntil

声明：`dist/types/internal/operators/skipUntil.d.ts`

    export declare function skipUntil<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-172"></a>

### skipWhile

声明：`dist/types/internal/operators/skipWhile.d.ts`

    export declare function skipWhile<T>(predicate: BooleanConstructor): OperatorFunction<T, Extract<T, Falsy> extends never ? never : T>;
    export declare function skipWhile<T>(predicate: (value: T, index: number) => true): OperatorFunction<T, never>;
    export declare function skipWhile<T>(predicate: (value: T, index: number) => boolean): MonoTypeOperatorFunction<T>;

<a id="api-173"></a>

### startWith

声明：`dist/types/internal/operators/startWith.d.ts`

    export declare function startWith<T>(value: null): OperatorFunction<T, T | null>;
    export declare function startWith<T>(value: undefined): OperatorFunction<T, T | undefined>;
    [弃用] export declare function startWith<T, A extends readonly unknown[] = T[]>(...valuesAndScheduler: [...A, SchedulerLike]): OperatorFunction<T, T | ValueFromArray<A>>;
    export declare function startWith<T, A extends readonly unknown[] = T[]>(...values: A): OperatorFunction<T, T | ValueFromArray<A>>;

<a id="api-174"></a>

### Subject

声明：`dist/types/internal/Subject.d.ts`

    export declare class Subject<T> extends Observable<T> implements SubscriptionLike
    closed: boolean;
    [弃用] observers: Observer<T>[];
    [弃用] isStopped: boolean;
    [弃用] hasError: boolean;
    [弃用] thrownError: any;
    [弃用] static create: (...args: any[]) => any;
    constructor();
    [弃用] lift<R>(operator: Operator<T, R>): Observable<R>;
    next(value: T): void;
    error(err: any): void;
    complete(): void;
    unsubscribe(): void;
    get observed(): boolean;
    asObservable(): Observable<T>;

<a id="api-175"></a>

### SubjectLike

声明：`dist/types/internal/types.d.ts`

    export interface SubjectLike<T> extends Observer<T>, Subscribable<T>

<a id="api-176"></a>

### Subscribable

声明：`dist/types/internal/types.d.ts`

    export interface Subscribable<T>
    subscribe(observer: Partial<Observer<T>>): Unsubscribable;

<a id="api-177"></a>

### SubscribableOrPromise

声明：`dist/types/internal/types.d.ts`

    [弃用] export declare type SubscribableOrPromise<T> = Subscribable<T> | Subscribable<never> | PromiseLike<T> | InteropObservable<T>;

<a id="api-178"></a>

### subscribeOn

声明：`dist/types/internal/operators/subscribeOn.d.ts`

    export declare function subscribeOn<T>(scheduler: SchedulerLike, delay?: number): MonoTypeOperatorFunction<T>;

<a id="api-179"></a>

### Subscriber

声明：`dist/types/internal/Subscriber.d.ts`

    export declare class Subscriber<T> extends Subscription implements Observer<T>
    [弃用] static create<T>(next?: (x?: T) => void, error?: (e?: any) => void, complete?: () => void): Subscriber<T>;
    [弃用] constructor(destination?: Subscriber<any> | Observer<any>);
    next(value: T): void;
    error(err?: any): void;
    complete(): void;
    unsubscribe(): void;

<a id="api-180"></a>

### Subscription

声明：`dist/types/internal/Subscription.d.ts`

    export declare class Subscription implements SubscriptionLike
    static EMPTY: Subscription;
    closed: boolean;
    constructor(initialTeardown?: (() => void) | undefined);
    unsubscribe(): void;
    add(teardown: TeardownLogic): void;
    remove(teardown: Exclude<TeardownLogic, void>): void;

<a id="api-181"></a>

### SubscriptionLike

声明：`dist/types/internal/types.d.ts`

    export interface SubscriptionLike extends Unsubscribable
    unsubscribe(): void;
    readonly closed: boolean;

<a id="api-182"></a>

### switchAll

声明：`dist/types/internal/operators/switchAll.d.ts`

    export declare function switchAll<O extends ObservableInput<any>>(): OperatorFunction<O, ObservedValueOf<O>>;

<a id="api-183"></a>

### switchMap

声明：`dist/types/internal/operators/switchMap.d.ts`

    export declare function switchMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function switchMap<T, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: undefined): OperatorFunction<T, ObservedValueOf<O>>;
    [弃用] export declare function switchMap<T, R, O extends ObservableInput<any>>(project: (value: T, index: number) => O, resultSelector: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R): OperatorFunction<T, R>;

<a id="api-184"></a>

### switchMapTo

声明：`dist/types/internal/operators/switchMapTo.d.ts`

    [弃用] export declare function switchMapTo<O extends ObservableInput<unknown>>(observable: O): OperatorFunction<unknown, ObservedValueOf<O>>;
    [弃用] export declare function switchMapTo<O extends ObservableInput<unknown>>(observable: O, resultSelector: undefined): OperatorFunction<unknown, ObservedValueOf<O>>;
    [弃用] export declare function switchMapTo<T, R, O extends ObservableInput<unknown>>(observable: O, resultSelector: (outerValue: T, innerValue: ObservedValueOf<O>, outerIndex: number, innerIndex: number) => R): OperatorFunction<T, R>;

<a id="api-185"></a>

### switchScan

声明：`dist/types/internal/operators/switchScan.d.ts`

    export declare function switchScan<T, R, O extends ObservableInput<any>>(accumulator: (acc: R, value: T, index: number) => O, seed: R): OperatorFunction<T, ObservedValueOf<O>>;

<a id="api-186"></a>

### Tail

声明：`dist/types/internal/types.d.ts`

    export declare type Tail<X extends readonly any[]> = ((...args: X) => any) extends (arg: any, ...rest: infer U) => any ? U : never;

<a id="api-187"></a>

### take

声明：`dist/types/internal/operators/take.d.ts`

    export declare function take<T>(count: number): MonoTypeOperatorFunction<T>;

<a id="api-188"></a>

### takeLast

声明：`dist/types/internal/operators/takeLast.d.ts`

    export declare function takeLast<T>(count: number): MonoTypeOperatorFunction<T>;

<a id="api-189"></a>

### takeUntil

声明：`dist/types/internal/operators/takeUntil.d.ts`

    export declare function takeUntil<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>;

<a id="api-190"></a>

### takeWhile

声明：`dist/types/internal/operators/takeWhile.d.ts`

    export declare function takeWhile<T>(predicate: BooleanConstructor, inclusive: true): MonoTypeOperatorFunction<T>;
    export declare function takeWhile<T>(predicate: BooleanConstructor, inclusive: false): OperatorFunction<T, TruthyTypesOf<T>>;
    export declare function takeWhile<T>(predicate: BooleanConstructor): OperatorFunction<T, TruthyTypesOf<T>>;
    export declare function takeWhile<T, S extends T>(predicate: (value: T, index: number) => value is S): OperatorFunction<T, S>;
    export declare function takeWhile<T, S extends T>(predicate: (value: T, index: number) => value is S, inclusive: false): OperatorFunction<T, S>;
    export declare function takeWhile<T>(predicate: (value: T, index: number) => boolean, inclusive?: boolean): MonoTypeOperatorFunction<T>;

<a id="api-191"></a>

### tap

声明：`dist/types/internal/operators/tap.d.ts`

    export declare function tap<T>(observerOrNext?: Partial<TapObserver<T>> | ((value: T) => void)): MonoTypeOperatorFunction<T>;
    [弃用] export declare function tap<T>(next?: ((value: T) => void) | null, error?: ((error: any) => void) | null, complete?: (() => void) | null): MonoTypeOperatorFunction<T>;

<a id="api-192"></a>

### TapObserver

声明：`dist/types/internal/operators/tap.d.ts`

    export interface TapObserver<T> extends Observer<T>
    subscribe: () => void;
    unsubscribe: () => void;
    finalize: () => void;

<a id="api-193"></a>

### TeardownLogic

声明：`dist/types/internal/types.d.ts`

    export declare type TeardownLogic = Subscription | Unsubscribable | (() => void) | void;

<a id="api-194"></a>

### throttle

声明：`dist/types/internal/operators/throttle.d.ts`

    export declare function throttle<T>(durationSelector: (value: T) => ObservableInput<any>, config?: ThrottleConfig): MonoTypeOperatorFunction<T>;

<a id="api-195"></a>

### ThrottleConfig

声明：`dist/types/internal/operators/throttle.d.ts`

    export interface ThrottleConfig
    leading?: boolean;
    trailing?: boolean;

<a id="api-196"></a>

### throttleTime

声明：`dist/types/internal/operators/throttleTime.d.ts`

    export declare function throttleTime<T>(duration: number, scheduler?: SchedulerLike, config?: ThrottleConfig): MonoTypeOperatorFunction<T>;

<a id="api-197"></a>

### throwError

声明：`dist/types/internal/observable/throwError.d.ts`

    export declare function throwError(errorFactory: () => any): Observable<never>;
    [弃用] export declare function throwError(error: any): Observable<never>;
    [弃用] export declare function throwError(errorOrErrorFactory: any, scheduler: SchedulerLike): Observable<never>;

<a id="api-198"></a>

### throwIfEmpty

声明：`dist/types/internal/operators/throwIfEmpty.d.ts`

    export declare function throwIfEmpty<T>(errorFactory?: () => any): MonoTypeOperatorFunction<T>;

<a id="api-199"></a>

### timeInterval

声明：`dist/types/internal/operators/timeInterval.d.ts`

    export declare function timeInterval<T>(scheduler?: SchedulerLike): OperatorFunction<T, TimeInterval<T>>;

<a id="api-200"></a>

### TimeInterval

声明：`dist/types/internal/types.d.ts`

    export interface TimeInterval<T>
    value: T;
    interval: number;

<a id="api-201"></a>

### timeout

声明：`dist/types/internal/operators/timeout.d.ts`

    export declare function timeout<T, O extends ObservableInput<unknown>, M = unknown>(config: TimeoutConfig<T, O, M> & { with: (info: TimeoutInfo<T, M>) => O; }): OperatorFunction<T, T | ObservedValueOf<O>>;
    export declare function timeout<T, M = unknown>(config: Omit<TimeoutConfig<T, any, M>, 'with'>): OperatorFunction<T, T>;
    export declare function timeout<T>(first: Date, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;
    export declare function timeout<T>(each: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>;

<a id="api-202"></a>

### TimeoutConfig

声明：`dist/types/internal/operators/timeout.d.ts`

    export interface TimeoutConfig<T, O extends ObservableInput<unknown> = ObservableInput<T>, M = unknown>
    each?: number;
    first?: number | Date;
    scheduler?: SchedulerLike;
    with?: (info: TimeoutInfo<T, M>) => O;
    meta?: M;

<a id="api-203"></a>

### TimeoutError

声明：`dist/types/internal/operators/timeout.d.ts`

    export interface TimeoutError<T = unknown, M = unknown> extends Error
    info: TimeoutInfo<T, M> | null;
    TimeoutError: TimeoutErrorCtor

<a id="api-204"></a>

### TimeoutInfo

声明：`dist/types/internal/operators/timeout.d.ts`

    export interface TimeoutInfo<T, M = unknown>
    readonly meta: M;
    readonly seen: number;
    readonly lastValue: T | null;

<a id="api-205"></a>

### timeoutWith

声明：`dist/types/internal/operators/timeoutWith.d.ts`

    [弃用] export declare function timeoutWith<T, R>(dueBy: Date, switchTo: ObservableInput<R>, scheduler?: SchedulerLike): OperatorFunction<T, T | R>;
    [弃用] export declare function timeoutWith<T, R>(waitFor: number, switchTo: ObservableInput<R>, scheduler?: SchedulerLike): OperatorFunction<T, T | R>;

<a id="api-206"></a>

### timer

声明：`dist/types/internal/observable/timer.d.ts`

    export declare function timer(due: number | Date, scheduler?: SchedulerLike): Observable<0>;
    export declare function timer(startDue: number | Date, intervalDuration: number, scheduler?: SchedulerLike): Observable<number>;
    [弃用] export declare function timer(dueTime: number | Date, unused: undefined, scheduler?: SchedulerLike): Observable<0>;

<a id="api-207"></a>

### timestamp

声明：`dist/types/internal/operators/timestamp.d.ts`

    export declare function timestamp<T>(timestampProvider?: TimestampProvider): OperatorFunction<T, Timestamp<T>>;

<a id="api-208"></a>

### Timestamp

声明：`dist/types/internal/types.d.ts`

    export interface Timestamp<T>
    value: T;
    timestamp: number;

<a id="api-209"></a>

### TimestampProvider

声明：`dist/types/internal/types.d.ts`

    export interface TimestampProvider
    now(): number;

<a id="api-210"></a>

### toArray

声明：`dist/types/internal/operators/toArray.d.ts`

    export declare function toArray<T>(): OperatorFunction<T, T[]>;

<a id="api-211"></a>

### TruthyTypesOf

声明：`dist/types/internal/types.d.ts`

    export declare type TruthyTypesOf<T> = T extends Falsy ? never : T;

<a id="api-212"></a>

### UnaryFunction

声明：`dist/types/internal/types.d.ts`

    export interface UnaryFunction<T, R>
    (source: T): R;

<a id="api-213"></a>

### Unsubscribable

声明：`dist/types/internal/types.d.ts`

    export interface Unsubscribable
    unsubscribe(): void;

<a id="api-214"></a>

### UnsubscriptionError

声明：`dist/types/internal/util/UnsubscriptionError.d.ts`

    export interface UnsubscriptionError extends Error
    readonly errors: any[];
    UnsubscriptionError: UnsubscriptionErrorCtor

<a id="api-215"></a>

### using

声明：`dist/types/internal/observable/using.d.ts`

    export declare function using<T extends ObservableInput<any>>(resourceFactory: () => Unsubscribable | void, observableFactory: (resource: Unsubscribable | void) => T | void): Observable<ObservedValueOf<T>>;

<a id="api-216"></a>

### ValueFromArray

声明：`dist/types/internal/types.d.ts`

    export declare type ValueFromArray<A extends readonly unknown[]> = A extends Array<infer T> ? T : never;

<a id="api-217"></a>

### ValueFromNotification

声明：`dist/types/internal/types.d.ts`

    export declare type ValueFromNotification<T> = T extends { kind: 'N' | 'E' | 'C'; } ? T extends NextNotification<any> ? T extends { value: infer V; } ? V : undefined : never : never;

<a id="api-218"></a>

### VirtualAction

声明：`dist/types/internal/scheduler/VirtualTimeScheduler.d.ts`

    export declare class VirtualAction<T> extends AsyncAction<T>
    constructor(scheduler: VirtualTimeScheduler, work: (this: SchedulerAction<T>, state?: T) => void, index?: number);
    schedule(state?: T, delay?: number): Subscription;

<a id="api-219"></a>

### VirtualTimeScheduler

声明：`dist/types/internal/scheduler/VirtualTimeScheduler.d.ts`

    export declare class VirtualTimeScheduler extends AsyncScheduler
    maxFrames: number;
    [弃用] static frameTimeFactor: number;
    frame: number;
    [弃用] index: number;
    constructor(schedulerActionCtor?: typeof AsyncAction, maxFrames?: number);
    flush(): void;

<a id="api-220"></a>

### window

声明：`dist/types/internal/operators/window.d.ts`

    export declare function window<T>(windowBoundaries: ObservableInput<any>): OperatorFunction<T, Observable<T>>;

<a id="api-221"></a>

### windowCount

声明：`dist/types/internal/operators/windowCount.d.ts`

    export declare function windowCount<T>(windowSize: number, startWindowEvery?: number): OperatorFunction<T, Observable<T>>;

<a id="api-222"></a>

### windowTime

声明：`dist/types/internal/operators/windowTime.d.ts`

    export declare function windowTime<T>(windowTimeSpan: number, scheduler?: SchedulerLike): OperatorFunction<T, Observable<T>>;
    export declare function windowTime<T>(windowTimeSpan: number, windowCreationInterval: number, scheduler?: SchedulerLike): OperatorFunction<T, Observable<T>>;
    export declare function windowTime<T>(windowTimeSpan: number, windowCreationInterval: number | null | void, maxWindowSize: number, scheduler?: SchedulerLike): OperatorFunction<T, Observable<T>>;

<a id="api-223"></a>

### windowToggle

声明：`dist/types/internal/operators/windowToggle.d.ts`

    export declare function windowToggle<T, O>(openings: ObservableInput<O>, closingSelector: (openValue: O) => ObservableInput<any>): OperatorFunction<T, Observable<T>>;

<a id="api-224"></a>

### windowWhen

声明：`dist/types/internal/operators/windowWhen.d.ts`

    export declare function windowWhen<T>(closingSelector: () => ObservableInput<any>): OperatorFunction<T, Observable<T>>;

<a id="api-225"></a>

### withLatestFrom

声明：`dist/types/internal/operators/withLatestFrom.d.ts`

    export declare function withLatestFrom<T, O extends unknown[]>(...inputs: [...ObservableInputTuple<O>]): OperatorFunction<T, [T, ...O]>;
    export declare function withLatestFrom<T, O extends unknown[], R>(...inputs: [...ObservableInputTuple<O>, (...value: [T, ...O]) => R]): OperatorFunction<T, R>;

<a id="api-226"></a>

### zip

声明：`dist/types/internal/observable/zip.d.ts`

    export declare function zip<A extends readonly unknown[]>(sources: [...ObservableInputTuple<A>]): Observable<A>;
    export declare function zip<A extends readonly unknown[], R>(sources: [...ObservableInputTuple<A>], resultSelector: (...values: A) => R): Observable<R>;
    export declare function zip<A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): Observable<A>;
    export declare function zip<A extends readonly unknown[], R>(...sourcesAndResultSelector: [...ObservableInputTuple<A>, (...values: A) => R]): Observable<R>;

<a id="api-227"></a>

### zipAll

声明：`dist/types/internal/operators/zipAll.d.ts`

    export declare function zipAll<T>(): OperatorFunction<ObservableInput<T>, T[]>;
    export declare function zipAll<T>(): OperatorFunction<any, T[]>;
    export declare function zipAll<T, R>(project: (...values: T[]) => R): OperatorFunction<ObservableInput<T>, R>;
    export declare function zipAll<R>(project: (...values: Array<any>) => R): OperatorFunction<any, R>;

<a id="api-228"></a>

### zipWith

声明：`dist/types/internal/operators/zipWith.d.ts`

    export declare function zipWith<T, A extends readonly unknown[]>(...otherInputs: [...ObservableInputTuple<A>]): OperatorFunction<T, Cons<T, A>>;

<a id="api-229"></a>

### ajax

声明：`dist/types/internal/ajax/ajax.d.ts`

    ajax: AjaxCreationMethod

<a id="api-230"></a>

### AjaxConfig

声明：`dist/types/internal/ajax/types.d.ts`

    export interface AjaxConfig
    url: string;
    body?: any;
    async?: boolean;
    method?: string;
    headers?: Readonly<Record<string, any>>;
    timeout?: number;
    user?: string;
    password?: string;
    [弃用] crossDomain?: boolean;
    withCredentials?: boolean;
    xsrfCookieName?: string;
    xsrfHeaderName?: string;
    responseType?: XMLHttpRequestResponseType;
    createXHR?: () => XMLHttpRequest;
    [弃用] progressSubscriber?: PartialObserver<ProgressEvent>;
    includeDownloadProgress?: boolean;
    includeUploadProgress?: boolean;
    queryParams?: string | URLSearchParams | Record<string, string | number | boolean | string[] | number[] | boolean[]> | [string, string | number | boolean | string[] | number[] | boolean[]][];

<a id="api-231"></a>

### AjaxDirection

声明：`dist/types/internal/ajax/types.d.ts`

    export declare type AjaxDirection = 'upload' | 'download';

<a id="api-232"></a>

### AjaxError

声明：`dist/types/internal/ajax/errors.d.ts`

    export interface AjaxError extends Error
    xhr: XMLHttpRequest;
    request: AjaxRequest;
    status: number;
    responseType: XMLHttpRequestResponseType;
    response: any;
    AjaxError: AjaxErrorCtor

<a id="api-233"></a>

### AjaxRequest

声明：`dist/types/internal/ajax/types.d.ts`

    export interface AjaxRequest
    url: string;
    body?: any;
    method: string;
    async: boolean;
    headers: Readonly<Record<string, any>>;
    timeout: number;
    user?: string;
    password?: string;
    crossDomain: boolean;
    withCredentials: boolean;
    responseType: XMLHttpRequestResponseType;

<a id="api-234"></a>

### AjaxResponse

声明：`dist/types/internal/ajax/AjaxResponse.d.ts`

    export declare class AjaxResponse<T>
    readonly originalEvent: ProgressEvent;
    readonly xhr: XMLHttpRequest;
    readonly request: AjaxRequest;
    readonly type: AjaxResponseType;
    readonly status: number;
    readonly response: T;
    [弃用] readonly responseType: XMLHttpRequestResponseType;
    readonly loaded: number;
    readonly total: number;
    readonly responseHeaders: Record<string, string>;
    constructor( originalEvent: ProgressEvent, xhr: XMLHttpRequest, request: AjaxRequest, type?: AjaxResponseType);

<a id="api-235"></a>

### AjaxTimeoutError

声明：`dist/types/internal/ajax/errors.d.ts`

    export interface AjaxTimeoutError extends AjaxError
    AjaxTimeoutError: AjaxTimeoutErrorCtor

<a id="api-236"></a>

### fromFetch

声明：`dist/types/internal/observable/dom/fetch.d.ts`

    export declare function fromFetch<T>(input: string | Request, init: RequestInit & { selector: (response: Response) => ObservableInput<T>; }): Observable<T>;
    export declare function fromFetch(input: string | Request, init?: RequestInit): Observable<Response>;

<a id="api-237"></a>

### combineLatest

声明：`dist/types/internal/operators/combineLatest.d.ts`

    [弃用] export declare function combineLatest<T, A extends readonly unknown[], R>(sources: [...ObservableInputTuple<A>], project: (...values: [T, ...A]) => R): OperatorFunction<T, R>;
    [弃用] export declare function combineLatest<T, A extends readonly unknown[], R>(sources: [...ObservableInputTuple<A>]): OperatorFunction<T, [T, ...A]>;
    [弃用] export declare function combineLatest<T, A extends readonly unknown[], R>(...sourcesAndProject: [...ObservableInputTuple<A>, (...values: [T, ...A]) => R]): OperatorFunction<T, R>;
    [弃用] export declare function combineLatest<T, A extends readonly unknown[], R>(...sources: [...ObservableInputTuple<A>]): OperatorFunction<T, [T, ...A]>;

<a id="api-238"></a>

### concat

声明：`dist/types/internal/operators/concat.d.ts`

    [弃用] export declare function concat<T, A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;
    [弃用] export declare function concat<T, A extends readonly unknown[]>(...sourcesAndScheduler: [...ObservableInputTuple<A>, SchedulerLike]): OperatorFunction<T, T | A[number]>;

<a id="api-239"></a>

### merge

声明：`dist/types/internal/operators/merge.d.ts`

    [弃用] export declare function merge<T, A extends readonly unknown[]>(...sources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;
    [弃用] export declare function merge<T, A extends readonly unknown[]>(...sourcesAndConcurrency: [...ObservableInputTuple<A>, number]): OperatorFunction<T, T | A[number]>;
    [弃用] export declare function merge<T, A extends readonly unknown[]>(...sourcesAndScheduler: [...ObservableInputTuple<A>, SchedulerLike]): OperatorFunction<T, T | A[number]>;
    [弃用] export declare function merge<T, A extends readonly unknown[]>(...sourcesAndConcurrencyAndScheduler: [...ObservableInputTuple<A>, number, SchedulerLike]): OperatorFunction<T, T | A[number]>;

<a id="api-240"></a>

### onErrorResumeNext

声明：`dist/types/internal/operators/onErrorResumeNextWith.d.ts`

    [弃用] onErrorResumeNext: typeof onErrorResumeNextWith

<a id="api-241"></a>

### partition

声明：`dist/types/internal/operators/partition.d.ts`

    [弃用] export declare function partition<T>(predicate: (value: T, index: number) => boolean, thisArg?: any): UnaryFunction<Observable<T>, [Observable<T>, Observable<T>]>;

<a id="api-242"></a>

### race

声明：`dist/types/internal/operators/race.d.ts`

    [弃用] export declare function race<T, A extends readonly unknown[]>(otherSources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;
    [弃用] export declare function race<T, A extends readonly unknown[]>(...otherSources: [...ObservableInputTuple<A>]): OperatorFunction<T, T | A[number]>;

<a id="api-243"></a>

### zip

声明：`dist/types/internal/operators/zip.d.ts`

    [弃用] export declare function zip<T, A extends readonly unknown[]>(otherInputs: [...ObservableInputTuple<A>]): OperatorFunction<T, Cons<T, A>>;
    [弃用] export declare function zip<T, A extends readonly unknown[], R>(otherInputsAndProject: [...ObservableInputTuple<A>], project: (...values: Cons<T, A>) => R): OperatorFunction<T, R>;
    [弃用] export declare function zip<T, A extends readonly unknown[]>(...otherInputs: [...ObservableInputTuple<A>]): OperatorFunction<T, Cons<T, A>>;
    [弃用] export declare function zip<T, A extends readonly unknown[], R>(...otherInputsAndProject: [...ObservableInputTuple<A>, (...values: Cons<T, A>) => R]): OperatorFunction<T, R>;

<a id="api-244"></a>

### RunHelpers

声明：`dist/types/internal/testing/TestScheduler.d.ts`

    export interface RunHelpers
    cold: typeof TestScheduler.prototype.createColdObservable;
    hot: typeof TestScheduler.prototype.createHotObservable;
    flush: typeof TestScheduler.prototype.flush;
    time: typeof TestScheduler.prototype.createTime;
    expectObservable: typeof TestScheduler.prototype.expectObservable;
    expectSubscriptions: typeof TestScheduler.prototype.expectSubscriptions;
    animate: (marbles: string) => void;

<a id="api-245"></a>

### TestScheduler

声明：`dist/types/internal/testing/TestScheduler.d.ts`

    export declare class TestScheduler extends VirtualTimeScheduler
    assertDeepEqual: (actual: any, expected: any) => boolean | void;
    static frameTimeFactor: number;
    [弃用] readonly hotObservables: HotObservable<any>[];
    [弃用] readonly coldObservables: ColdObservable<any>[];
    constructor(assertDeepEqual: (actual: any, expected: any) => boolean | void);
    createTime(marbles: string): number;
    createColdObservable<T = string>(marbles: string, values?: { [marble: string]: T; }, error?: any): ColdObservable<T>;
    createHotObservable<T = string>(marbles: string, values?: { [marble: string]: T; }, error?: any): HotObservable<T>;
    expectObservable<T>(observable: Observable<T>, subscriptionMarbles?: string | null): { toBe(marbles: string, values?: any, errorValue?: any): void; toEqual: (other: Observable<T>) => void; };
    expectSubscriptions(actualSubscriptionLogs: SubscriptionLog[]): { toBe: subscriptionLogsToBeFn; };
    flush(): void;
    static parseMarblesAsSubscriptions(marbles: string | null, runMode?: boolean): SubscriptionLog;
    static parseMarbles(marbles: string, values?: any, errorValue?: any, materializeInnerObservables?: boolean, runMode?: boolean): TestMessage[];
    run<T>(callback: (helpers: RunHelpers) => T): T;

<a id="api-246"></a>

### webSocket

声明：`dist/types/internal/observable/dom/webSocket.d.ts`

    export declare function webSocket<T>(urlConfigOrSource: string | WebSocketSubjectConfig<T>): WebSocketSubject<T>;

<a id="api-247"></a>

### WebSocketSubject

声明：`dist/types/internal/observable/dom/WebSocketSubject.d.ts`

    export declare class WebSocketSubject<T> extends AnonymousSubject<T>
    constructor(urlConfigOrSource: string | WebSocketSubjectConfig<T> | Observable<T>, destination?: Observer<T>);
    [弃用] lift<R>(operator: Operator<T, R>): WebSocketSubject<R>;
    multiplex(subMsg: () => any, unsubMsg: () => any, messageFilter: (value: T) => boolean): Observable<T>;
    unsubscribe(): void;

<a id="api-248"></a>

### WebSocketSubjectConfig

声明：`dist/types/internal/observable/dom/WebSocketSubject.d.ts`

    export interface WebSocketSubjectConfig<T>
    url: string;
    protocol?: string | Array<string>;
    [弃用] resultSelector?: (e: MessageEvent) => T;
    serializer?: (value: T) => WebSocketMessage;
    deserializer?: (e: MessageEvent) => T;
    openObserver?: NextObserver<Event>;
    closeObserver?: NextObserver<CloseEvent>;
    closingObserver?: NextObserver<void>;
    WebSocketCtor?: { new (url: string, protocols?: string | string[]): WebSocket; };
    binaryType?: 'blob' | 'arraybuffer';
