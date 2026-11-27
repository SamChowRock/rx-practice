# 从一行日志到实时控制台：RxJS 7.8.2 教程大纲

正文从[日志面板](book/00.md)讲起，假设读者会写 JavaScript 回调、读懂 Promise 和基本的 TypeScript 类型。前九章在内存里逐步拼出控制台；第 10 章开始接本机的 HTTP 服务和实时连接。[运行方法和阅读顺序](README.md)

这份大纲保留了各章的闭卷题。想先试自己的做法，可以只读题面，到同号实验核对结果。第 14–16 章分别练事故回放、导出和独立排错，第 17 章做控制台验收与预约系统；另有[第 18 章十道独立题](book/18.md)和[接星星游戏](game/README.md)。另一个场景里的 39 道配套题收在[练习册](发散练习导航.md)，可按章节或题目选读。

强化篇从一份已结束的构建归档讲起。[第 19 章](book/19.md)按服务器返回的游标翻页，区分空页和末页；[第 20 章](book/20.md)让列表与导出共用同一次读取；[第 21 章](book/21.md)比较两份归档，分别呈现零行与读取失败。三章都有同号实验，例如 `npm run lab -- 20`。

报告导出后续沿用同一份对照结果。[第 22 章](book/22.md)把创建任务和轮询进度分开；[第 23 章](book/23.md)分别测量回信间隔、进度停滞与等待发布的时间；[第 24 章](book/24.md)从两个下载点取第一份可用文件。三个同号实验均可独立运行。

下表和各章的名称清单供查漏。六个公开入口共 365 个「入口 × 导出名」，同一声明出现在两个入口会计两项。方法、重载和配置字段随对应条目讲。另有 26 个重点 API，各在主讲之后至少五个不同章节里再次练习。

## 00｜等下一行日志的时候

先用数组和事件监听分别找第一条错误，再用 Observable 把它们接给同一个观察者。看筛选、取一条和退订怎样配合，理解 RxJS 为什么把操作符与数据来源分开。[阅读第 0 章](book/00.md)

## 01｜关掉面板，为什么日志还在打印？

**闭卷建模**：面板打开，收到两行日志，然后用户关掉面板，服务端又发来一行。分别写下屏幕应显示什么、后台还允许做什么。仅凭屏幕不再更新，能证明后台停了吗？

从一个定时发日志的源开始，分别让它完成、报错、被退订。用启动和清理次数证明面板没有留下后台工作。

**本章主讲（按导入入口列全）**

- `rxjs`：`CompletionObserver`、`ErrorObserver`、`finalize`、`NextObserver`、`Observable`、`Observer`、`PartialObserver`、`Subscriber`、`Subscription`、`SubscriptionLike`、`TeardownLogic`、`Unsubscribable`、`UnsubscriptionError`、`using`
- `rxjs/operators`：`finalize`

**本章回访（不重复计数）**

起点章，无前置 API。先独立画出完成、出错、主动退订的三条路径，作为第 2 章更换数据源后的对照。

## 02｜让同一个面板读懂不同来源

**闭卷建模**：一个按钮在面板打开前被点过一次；另一份历史记录在打开前就已存在。两个面板先后打开，它们各自该收到什么？指出哪条规则必须先和产品确认。

把历史数组、Promise、按钮事件和回调式日志 SDK 接进同一个入口；比较订阅之前和之后才创建请求的差异。用空源、永不结束的源和失败源做后续实验的夹具。

**本章主讲（按导入入口列全）**

- `rxjs`：`bindCallback`、`bindNodeCallback`、`defer`、`EMPTY`、`FactoryOrValue`、`from`、`fromEvent`、`fromEventPattern`、`generate`、`iif`、`InteropObservable`、`isObservable`、`NEVER`、`observable`、`ObservableInput`、`ObservableLike`、`of`、`range`、`ReadableStreamLike`、`Subscribable`、`SubscribableOrPromise`、`throwError`

**本章回访（不重复计数）**

- `Observable`、`Subscription`、`finalize`：把第 1 章的定时源换成事件监听器；挂载、卸载两次，记录启动次数、清理次数，证明卸载后不再收消息。

## 03｜把原始消息变成可读的日志

**闭卷建模**：同一个任务依次报告 A、A、B、A。产品说『别重复显示』。给出两种合理的输出，用一个问题问清产品到底要哪种。

解析消息、筛掉心跳、保留诊断输出，再把重复逻辑提成带类型的操作符。用 A→B→A 的序列区分全局去重和相邻去重。

**本章主讲（按导入入口列全）**

- `rxjs`：`distinct`、`distinctUntilChanged`、`distinctUntilKeyChanged`、`Falsy`、`filter`、`identity`、`map`、`MonoTypeOperatorFunction`、`noop`、`OperatorFunction`、`pairwise`、`partition`、`pipe`、`tap`、`TapObserver`、`TruthyTypesOf`、`UnaryFunction`
- `rxjs/operators`：`distinct`、`distinctUntilChanged`、`distinctUntilKeyChanged`、`filter`、`map`、`pairwise`、`tap`、`TapObserver`

**本章回访（不重复计数）**

- `from`、`fromEvent`：把历史数组和按钮事件接到同一个解析函数；先预测订阅两次各自会看到什么，再改变订阅时机。

## 04｜怎么从一段日志得出结论？

**闭卷建模**：日志一直不结束。用户要看『目前为止错误数』，也要知道『本次运行的最后一条错误』。哪些答案现在就能给？如果到结束也没有错误，界面应该怎样？

构造正常、空、重复和不结束的记录；区分取第一条与要求恰有一条，区分持续更新的计数与结束后才出具的报告。

**本章主讲（按导入入口列全）**

- `rxjs`：`ArgumentOutOfRangeError`、`count`、`defaultIfEmpty`、`elementAt`、`EmptyError`、`endWith`、`every`、`find`、`findIndex`、`first`、`firstValueFrom`、`isEmpty`、`last`、`lastValueFrom`、`max`、`min`、`NotFoundError`、`reduce`、`scan`、`SequenceError`、`single`、`skip`、`skipLast`、`skipUntil`、`skipWhile`、`startWith`、`take`、`takeLast`、`takeUntil`、`takeWhile`、`throwIfEmpty`、`toArray`
- `rxjs/operators`：`count`、`defaultIfEmpty`、`elementAt`、`endWith`、`every`、`find`、`findIndex`、`first`、`isEmpty`、`last`、`max`、`min`、`reduce`、`scan`、`single`、`skip`、`skipLast`、`skipUntil`、`skipWhile`、`startWith`、`take`、`takeLast`、`takeUntil`、`takeWhile`、`throwIfEmpty`、`toArray`

**本章回访（不重复计数）**

- `map`、`filter`：先筛错误再取第一条，与先取第一条再筛错误对照；构造使两个版本结果不同的最小输入。
- `Observable`、`Subscription`：把同一组日志分别作为有限源和持续源发送；判断报告是否产出、什么时候必须主动退订。

- `from`：分别回放一个空数组和含一条记录的数组，写出何时结束；与不结束的日志源作对照。

## 05｜搜索框、刷新按钮和心跳为什么不能共用一种限流？

**闭卷建模**：输入发生在 0、80、190 毫秒，之后停手。需求一是停手满 150 毫秒再查，需求二是第一次立即查、此后 150 毫秒内忽略输入。分别写出查询时刻，不写代码。

在同一张输入时间线上画出应该保留的值；把停顿后搜索、立即响应首次操作、定期刷新分开。记录事件时间与相邻事件间隔。

**本章主讲（按导入入口列全）**

- `rxjs`：`audit`、`auditTime`、`debounce`、`debounceTime`、`delay`、`delayWhen`、`interval`、`sample`、`sampleTime`、`throttle`、`ThrottleConfig`、`throttleTime`、`timeInterval`、`TimeInterval`、`timer`、`timestamp`、`Timestamp`、`TimestampProvider`
- `rxjs/operators`：`audit`、`auditTime`、`debounce`、`debounceTime`、`delay`、`delayWhen`、`sample`、`sampleTime`、`throttle`、`ThrottleConfig`、`throttleTime`、`timeInterval`、`timestamp`

**本章回访（不重复计数）**

- `fromEvent`、`map`、`distinctUntilChanged`：输入『A、A、B、A』并插入停顿，预测请求候选次数；交换去重与防抖的位置后重画时间线。
- `takeUntil`、`finalize`：用关闭按钮终止心跳，检查面板销毁时是否仍有计时器。

## 06｜切任务、存备注、并行补详情：新任务来了怎么办？

**闭卷建模**：用户先选 A 再选 B，但 A 的结果最后返回；同样的顺序也可能发生在两次备注保存中。分别画出允许的结果，并解释为什么两个功能不能共享同一条并发规则。

给每个请求安排可控的完成顺序。先产生一条条内部流再展开，比较替换、排队、限并发和忽略；用慢响应与失败请求检查结果，而不是只看成功路径。

**本章主讲（按导入入口列全）**

- `rxjs`：`concatAll`、`concatMap`、`exhaustAll`、`exhaustMap`、`expand`、`mergeAll`、`mergeMap`、`mergeScan`、`switchAll`、`switchMap`、`switchScan`
- `rxjs/operators`：`concatAll`、`concatMap`、`exhaustAll`、`exhaustMap`、`expand`、`mergeAll`、`mergeMap`、`mergeScan`、`switchAll`、`switchMap`、`switchScan`

**本章回访（不重复计数）**

- `debounceTime`、`distinctUntilChanged`：修复一次搜索回归：防抖并没有解决旧结果覆盖。用两次相距足够远、返回顺序相反的输入复现，再决定取消旧查询的位置。
- `defer`、`finalize`：给每次模拟请求分别记录创建、订阅与清理，以证据区分没有启动、被取消、正常完成。

- `fromEvent`：把刷新按钮监听误放到每次选中任务的回调里；切三次再点一次，定位为什么发起了多次请求。

## 07｜历史记录、实时日志和工具栏，谁驱动谁？

**闭卷建模**：先把筛选条件改成 A，点击导出，再改成 B。分别列出预览更新和导出任务；对每个输出，圈出真正触发它的输入，而不是它读取的所有输入。

用加载历史、追加实时消息和按当前筛选条件导出三种需求，区分等待完成、等齐首值、按位置配对和仅由点击触发。检验数据源迟到、为空和永不完成。

**本章主讲（按导入入口列全）**

- `rxjs`：`combineLatest`、`combineLatestAll`、`combineLatestWith`、`concat`、`concatWith`、`Cons`、`forkJoin`、`Head`、`merge`、`mergeWith`、`ObservableInputTuple`、`ObservedValueOf`、`ObservedValuesFromArray`、`ObservedValueTupleFromArray`、`ObservedValueUnionFromArray`、`race`、`raceWith`、`sequenceEqual`、`Tail`、`ValueFromArray`、`withLatestFrom`、`zip`、`zipAll`、`zipWith`
- `rxjs/operators`：`combineLatestAll`、`combineLatestWith`、`concatWith`、`mergeWith`、`raceWith`、`sequenceEqual`、`withLatestFrom`、`zipAll`、`zipWith`

**本章回访（不重复计数）**

- `filter`、`map`、`scan`：从实时日志累积当前错误数，再接入筛选条件；解释为什么只变筛选条件也可能需要重算视图。
- `concatMap`、`exhaustMap`：连续点击两次导出：按『每次都要导出』和『忙时忽略』各实现一次，核对启动次数与读取筛选条件的时机。

- `switchMap`、`debounceTime`：预览筛选结果时快速切换条件；在慢请求返回前改变条件，标出防抖等待和旧查询退订的两个不同时刻。

## 08｜错误计数器上线，为什么多了一条连接？

**闭卷建模**：两个面板先后查看同一频道，第一个关闭后第二个还在看。画两行时间线：面板各自的监听，以及底层连接。再决定晚打开的面板是否应该补看上一条消息。

给面板和计数器接入同一源，记录连接与断开次数；让第二个面板晚到，再让最后一个面板离开。由可观察的行为决定是否共享、回放或重置。

**本章主讲（按导入入口列全）**

- `rxjs`：`AsyncSubject`、`BehaviorSubject`、`connect`、`connectable`、`Connectable`、`ConnectConfig`、`ObjectUnsubscribedError`、`ReplaySubject`、`share`、`ShareConfig`、`shareReplay`、`ShareReplayConfig`、`Subject`、`SubjectLike`
- `rxjs/operators`：`connect`、`ConnectConfig`、`share`、`ShareConfig`、`shareReplay`、`ShareReplayConfig`

**本章回访（不重复计数）**

- `Observable`、`Subscription`、`finalize`：分别记录两个面板各自的退订和底层连接的清理；解释关闭一个面板时哪一个计数应增加。
- `combineLatest`、`withLatestFrom`：让筛选栏晚于日志面板挂载；用相同输入对照首次渲染与主动导出分别由谁触发。

## 09｜每秒涌入上千行，怎么保留明细又不刷爆界面？

**闭卷建模**：正常时按秒统计，告警时要立刻看到窗口内每一条新日志。你准备什么时候交出一整批数据，什么时候必须在批次结束前就交给下游？画出两者最早可见的时刻。

用相同输入做批量落盘、逐窗口统计和按任务分组；比较数组批次与仍能订阅的窗口。给长寿命分组设置关闭条件，并检查重开后的行为。

**本章主讲（按导入入口列全）**

- `rxjs`：`BasicGroupByOptions`、`buffer`、`bufferCount`、`bufferTime`、`bufferToggle`、`bufferWhen`、`groupBy`、`GroupByOptionsWithElement`、`GroupedObservable`、`window`、`windowCount`、`windowTime`、`windowToggle`、`windowWhen`
- `rxjs/operators`：`BasicGroupByOptions`、`buffer`、`bufferCount`、`bufferTime`、`bufferToggle`、`bufferWhen`、`groupBy`、`GroupByOptionsWithElement`、`window`、`windowCount`、`windowTime`、`windowToggle`、`windowWhen`

**本章回访（不重复计数）**

- `scan`、`mergeMap`、`concatMap`：并行算各任务的窗口统计，按序落盘批次；故意延迟首批写入，用持久化顺序证明选择正确。
- `Subject`、`BehaviorSubject`：分别用事件总线发布『清空』和用状态源保存『当前任务』；验证晚挂载的面板不应重放清空指令，却应能读到当前任务。

- `combineLatest`、`withLatestFrom`：对带时间戳的分组计数与批次写入信号分别建模；换一组到达顺序，预测哪个动作会多触发一次。

## 10｜把模拟源换成真实网络

**闭卷建模**：切走任务后，旧结果没出现在屏幕上。列出还不能据此断定的两件事，并分别设计一个能观察浏览器和服务端工作的证据。

HTTP 拉取历史记录，WebSocket 订阅实时日志；观察请求取消、响应体读取、进度事件、切换频道和关闭连接。明确不把前端退订当成服务端事务回滚。

**本章主讲（按导入入口列全）**

- `rxjs/ajax`：`ajax`、`AjaxConfig`、`AjaxDirection`、`AjaxError`、`AjaxRequest`、`AjaxResponse`、`AjaxTimeoutError`
- `rxjs/fetch`：`fromFetch`
- `rxjs/webSocket`：`webSocket`、`WebSocketSubject`、`WebSocketSubjectConfig`

**本章回访（不重复计数）**

- `defer`、`from`、`switchMap`：对照已经创建的 fetch Promise 与订阅时才创建的请求；快速切任务，分别检查旧结果是否到达、网络请求是否真的中止。
- `share`、`shareReplay`、`takeUntil`：为同一实时频道开两个消费者：检查是否只连一次、晚来的面板是否重放、最后一个离开后是否断连；显式选择回放与引用计数配置。
- `combineLatest`、`withLatestFrom`：连接真实筛选栏；更改筛选条件只刷新视图，点击导出才发请求，以网络请求数验收。

- `fromEvent`、`exhaustMap`：用真实刷新按钮连续点击三次；第一次响应前应只有一个请求，响应后再点应能启动新请求。

## 11｜断线以后，只恢复该恢复的部分

**闭卷建模**：网络先断后恢复，用户在断线期间又改了一次搜索词。画出哪些操作应该失败、哪些监听应该活着；如果失败的是写入而不是查询，重试前还缺什么信息？

比较单次请求失败与整个搜索框失效；给读取加超时、有限重试和恢复策略。把错误和完成变成可保存的通知，再回放故障经过。

**本章主讲（按导入入口列全）**

- `rxjs`：`catchError`、`CompleteNotification`、`config`、`dematerialize`、`ErrorNotification`、`GlobalConfig`、`ignoreElements`、`materialize`、`NextNotification`、`Notification`、`NotificationKind`、`ObservableNotification`、`onErrorResumeNextWith`、`repeat`、`RepeatConfig`、`retry`、`RetryConfig`、`timeout`、`TimeoutConfig`、`TimeoutError`、`TimeoutInfo`、`ValueFromNotification`
- `rxjs/operators`：`catchError`、`dematerialize`、`ignoreElements`、`materialize`、`repeat`、`RepeatConfig`、`retry`、`RetryConfig`、`timeout`、`TimeoutConfig`、`TimeoutInfo`

**本章回访（不重复计数）**

- `switchMap`、`debounceTime`：分别让错误处理位于单次查询内部和整个输入流外部；第一次搜索失败后，再输入一次，判断搜索框是否还活着。
- `concatMap`、`mergeMap`、`exhaustMap`：给保存、补详情、刷新各注入一次失败，记录失败后哪些待处理操作被保留；不要盲目重试写入。
- `Subject`、`BehaviorSubject`：区分一次性的断线事件与持续的连接状态；让状态栏在重连中途挂载，预测它看到的第一条消息。

- `defer`、`finalize`：让读取连续失败两次；把请求工厂和清理分别放在重试内外，记录每次尝试与整轮恢复各执行了几次。

## 12｜把偶发的时序错误变成每次都能复现的测试

**闭卷建模**：旧任务的结果没有出现。设计两种不同实现，让它们屏幕输出一样，却只有一种及时停止旧任务。除了值，还需要记录什么才能区分？

先记录同步、异步与逐帧更新的执行顺序；再用虚拟时间替代等待。对切换任务既断言结果，也断言旧源的订阅区间。区分虚拟调度与真实网络、原生 Promise 的测试边界。

**本章主讲（按导入入口列全）**

- `rxjs`：`animationFrames`、`animationFrameScheduler`、`asapScheduler`、`asyncScheduler`、`observeOn`、`queueScheduler`、`scheduled`、`Scheduler`、`SchedulerAction`、`SchedulerLike`、`subscribeOn`、`VirtualAction`、`VirtualTimeScheduler`
- `rxjs/operators`：`observeOn`、`subscribeOn`
- `rxjs/testing`：`RunHelpers`、`TestScheduler`

**本章回访（不重复计数）**

- `switchMap`、`takeUntil`：用虚拟时间断言旧任务退订的精确帧；把关闭信号移到内部源还在运行的时刻，确认没有残留订阅。
- `debounceTime`、`distinctUntilChanged`：用相同输入测试『重复输入会不会推迟发出时间』，既断言输出值，也断言输出帧。
- `share`、`shareReplay`：把两个消费者的订阅错开，断言底层连接区间；再让两者之间出现无人订阅的空档，核对重连与缓存。
- `catchError`、`retry`、`timeout`：模拟超时后重试一次仍失败，断言订阅次数、失败帧，以及降级结果是否让外层流继续工作。

## 13｜接手旧版控制台，在行为不变的前提下迁移

**闭卷建模**：旧版和新版的首屏一样。设计一组退出、重进、失败、再进入的动作，能揭露缓存和连接清理行为的差异。先写验收结果，再读两版代码。

用前面写的测试保护旧实现。比较旧多播写法、同名的创建函数与管道操作符、旧别名和弃用重载；逐处说明语义是否等价。此章不是所有弃用项的唯一归属，其他章也就地标记。

**本章主讲（按导入入口列全）**

- `rxjs`：`animationFrame`、`asap`、`async`、`combineAll`、`concatMapTo`、`ConnectableObservable`、`empty`、`exhaust`、`flatMap`、`mapTo`、`mergeMapTo`、`multicast`、`never`、`onErrorResumeNext`、`Operator`、`pairs`、`pluck`、`publish`、`publishBehavior`、`publishLast`、`publishReplay`、`queue`、`refCount`、`repeatWhen`、`retryWhen`、`switchMapTo`、`timeoutWith`
- `rxjs/operators`：`combineAll`、`combineLatest`、`concat`、`concatMapTo`、`exhaust`、`flatMap`、`mapTo`、`merge`、`mergeMapTo`、`multicast`、`onErrorResumeNext`、`partition`、`pluck`、`publish`、`publishBehavior`、`publishLast`、`publishReplay`、`race`、`refCount`、`repeatWhen`、`retryWhen`、`switchMapTo`、`timeoutWith`、`zip`

**本章回访（不重复计数）**

- `map`、`filter`：迁移旧投影代码；输入缺失字段与合法假值，证明新筛选不会把 0 当作缺失。
- `share`、`shareReplay`：替换旧多播写法；对完成、报错、订阅数归零三种情况分别比较重置与回放行为，不以『首屏一样』算迁移成功。
- `catchError`、`retry`、`timeout`、`TestScheduler`：把旧超时和重试逻辑换成配置式调用；固定虚拟时间，验证重试延迟、次数和最终错误不变。

## 14｜给断线的现场做离线回放

**闭卷建模**：你拿到一份事故录像，只记录了消息值，没有记录到达时间、断开或取消。写出两条都符合录像却要求不同修复的历史，并列出还需要补录什么。

把一次线上会话录成带时间信息的本地事件，离线重现。交付物是可重复运行的故障用例，不接网络。

**本章回访（不重复计数）**

- `Observable`、`Subscription`、`from`：把数组回放与自定义可取消回放源接入同一播放器；中途停止，证明确实终止后续投递，而不只是隐藏输出。
- `map`、`filter`、`scan`：从录像重建错误计数；用一条合法的零值与一条非法记录检验投影、筛选与累积各自的结果。
- `distinctUntilChanged`、`Subject`、`BehaviorSubject`：回放 A→A→B 的频道选择与一次清空命令；让面板中途加入，分别断言当前频道、重复选择和历史命令的可见性。
- `TestScheduler`、`catchError`、`retry`、`timeout`：把先超时、重试成功的故障录成虚拟冷源；断言恢复结果与两段订阅区间，区别于第 12 章的最终失败。
- `combineLatest`、`withLatestFrom`：用回放的点击和网络状态重建发送记录；故意交换首个状态与首个点击的顺序，解释哪些点击没有生成请求、哪些状态更新不应重复发送。

## 15｜导出中心：慢磁盘、慢网络与重复点击

**闭卷建模**：一次导出开始后，用户换了格式，又点了一次导出。慢磁盘还没写完第一份。为文件参数、写入顺序和预览各定一条规则，再说明哪条是你作的假设。

给控制台加一个导出中心。格式和选区变化时，预览可以重算；用户已经点过的导出要保留当时的内容。两次点击离得很近时，测试分别记录参数和落盘顺序。

**本章回访（不重复计数）**

- `fromEvent`、`from`、`defer`：用点击创建导出任务，把选中记录数组展开；延后启动文件生成，证明取消排队任务不会提前生成文件。
- `concatMap`、`mergeMap`：分别对文件写入与附件读取限流；让首份附件最慢，测量峰值并发数和落盘顺序。
- `combineLatest`、`withLatestFrom`：让预计大小随筛选和格式变化；点击时冻结导出参数。导出途中再改格式，断言预览变化但当前文件不变。
- `TestScheduler`、`takeUntil`：模拟用户在同一帧取消导出和准备下一批数据；给出订阅顺序，再把取消信号提前一帧，用两条区间断言解释差异。

## 16｜接手值班：查出后台悄悄增加的工作

**闭卷建模**：某次发布后网络请求翻倍，页面却只显示一份数据。提出两个不同解释，选一个最便宜的观测点来区分它们。先写什么观测结果会推翻你的首选解释，再动手。

只给症状和故障数据，先独立定位，再对照教师索引。交付物是一个修复前失败、修复后通过的最小回归测试，以及你排除的错误解释。

**本章回访（不重复计数）**

- `Observable`、`Subscription`、`share`、`shareReplay`：从两次上游启动与一次旧状态闪现两种症状入手；分别测量底层连接数和新订阅者首值，区分重复订阅与错误回放。
- `defer`、`catchError`、`retry`、`timeout`、`TestScheduler`：复现重连复用了同一个失败 Promise 的故障；虚拟冷源检验超时与重试窗口，另用真实 Promise 的集成测试检验每次尝试是否重新创建请求、最后是否降级。
- `filter`、`distinctUntilChanged`、`debounceTime`：相同错误码在恢复后重新出现却没有告警；构造含恢复信号的最短时间线，检查过滤、去重、防抖顺序，给出能通过的输出断言。
- `scan`、`Subject`、`BehaviorSubject`：跨会话错误计数不归零；分别追踪清空命令、当前状态和累加器生命周期，用晚加入的订阅者验证修复。
- `takeUntil`、`exhaustMap`、`mergeMap`：连续刷新再关闭面板：让重复刷新被忽略，但已启动的一组详情请求必须全部停止；记录各内部源的清理时刻。

## 结业｜用一次故障演练贯穿全书

**陌生需求迁移（与控制台演练分开验收）**：给展览馆做预约页面。选日期时刷新余票，确认时锁定当时的场次；连点不能重复预约，切走页面后，已确认的预约不能被误认为撤销。服务端可能已受理但回复丢失。不给 API 名与候选方案。先提交事件时间线和必要假设，再提交实现或伪代码。最后由出题者临时改变一条业务规则，要求调整模型而不是重背答案。

**独立通过条件**：① 时间线标明触发者、读取的状态和预期输出；② 交代在途工作、错误、取消和服务端副作用的边界；③ 用一个反例排除看似可行的替代方案，并给出能区分两者的断言；④ 面对变式，指出哪些规则和测试要改、哪些不变。四项均能独立解释才通过。没通过就按缺项回到对应章做一道新变式；答出 API 名或代码跑通不代替验收。允许不用 RxJS，但须证明实现满足相同的行为约束。

同时打开日志与计数面板，快速由任务 A 切到 B，再关闭一个面板并模拟断网。检查是否串台、是否重复连接、是否误停另一个面板；最后关闭所有面板，断言没有残留订阅或重试定时器。随后给出一段带弃用重载的旧实现，在保留预期行为的条件下替换。用双向索引核对还不能解释或验证的条目，而不是仅以项目能跑作为毕业标准。

**本章回访（不重复计数）**

- `switchMap`、`concatMap`、`mergeMap`、`exhaustMap`：不提供候选操作符，为切任务、逐笔存备注、限并发补详情和防连点刷新各选策略；用乱序返回验证每个决定。
- `catchError`、`retry`、`timeout`、`TestScheduler`：模拟断网、重连等待中关闭所有面板；证明既没有错误冒泡杀死无关面板，也没有后台重试。
- `scan`、`Subject`、`BehaviorSubject`、`share`、`shareReplay`：让错误计数面板关闭后重新打开；先写下它该拿到的状态和不该重放的事件，再用连接次数和首条消息验收。

## 重点 API 回访索引

| API | 主讲章 | 后续主动练习（任务见对应章） |
|---|---:|---|
| `Observable` | 01 | 02、04、08、14、16 |
| `Subscription` | 01 | 02、04、08、14、16 |
| `finalize` | 01 | 02、05、06、08、11 |
| `from` | 02 | 03、04、10、14、15 |
| `fromEvent` | 02 | 03、05、06、10、15 |
| `defer` | 02 | 06、10、11、15、16 |
| `map` | 03 | 04、05、07、13、14 |
| `filter` | 03 | 04、07、13、14、16 |
| `distinctUntilChanged` | 03 | 05、06、12、14、16 |
| `scan` | 04 | 07、09、14、16、结业 |
| `takeUntil` | 04 | 05、10、12、15、16 |
| `debounceTime` | 05 | 06、07、11、12、16 |
| `switchMap` | 06 | 07、10、11、12、结业 |
| `concatMap` | 06 | 07、09、11、15、结业 |
| `mergeMap` | 06 | 09、11、15、16、结业 |
| `exhaustMap` | 06 | 07、10、11、16、结业 |
| `combineLatest` | 07 | 08、09、10、14、15 |
| `withLatestFrom` | 07 | 08、09、10、14、15 |
| `Subject` | 08 | 09、11、14、16、结业 |
| `BehaviorSubject` | 08 | 09、11、14、16、结业 |
| `share` | 08 | 10、12、13、16、结业 |
| `shareReplay` | 08 | 10、12、13、16、结业 |
| `catchError` | 11 | 12、13、14、16、结业 |
| `retry` | 11 | 12、13、14、16、结业 |
| `timeout` | 11 | 12、13、14、16、结业 |
| `TestScheduler` | 12 | 13、14、15、16、结业 |
