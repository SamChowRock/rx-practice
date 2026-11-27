# RxJS 教程 · 从一行日志到实时控制台

RxJS（Reactive Extensions for JavaScript）让你能对**陆续到来的值**做筛选、变换和组合，也能在用完时一起收掉相关的监听和计时器。只取一次构建结果，用 `await` 就能写清。做实时日志面板时，用户会切换任务，旧请求可能还没返回，新的日志又到了。代码还得表达哪次读取仍然有效，以及谁负责停掉旧读取。这是本教程用 RxJS 解决的问题。

全书使用 RxJS 7.8.2，假设你熟悉 JavaScript 的事件回调、数组和 Promise。[第 0 章](book/00.md)从你会写的 `find` 和事件监听讲起，给历史数组和实时事件用上同一条规则：找到第一条错误就停。

## 第一次打开

在项目根目录使用 Node.js 24 或更高版本，执行 `npm ci` 安装锁文件中的依赖。然后运行 `node`，跟着第 0 章在提示符里送出一条普通日志和一条报错，看监听器的数量什么时候从 1 变回 0。输入 `.exit` 退出 Node。

想先看看成品，执行 `npm start`，打开终端打印的[本机地址](http://127.0.0.1:4173)。等日志开始滚动，关掉列表，保留错误计数，然后点「检查服务端」。活跃连接仍是 1。再关计数器，稍后重查，活跃连接变成 0。按 Ctrl+C 停服务。若默认端口被占，在 macOS 或 Linux 上可用 `PORT=4180 npm start`。

## 按顺序读

| 你要实现的行为 | 对应的 RxJS 用法 |
|---|---|
| 打开面板，读历史，接新行，关掉时清理 | [00 Observable](book/00.md) · [01 subscribe 与清理](book/01.md) · [02 from 与 defer](book/02.md) · [03 pipe 与操作符](book/03.md) |
| 边收边统计，等输入停顿，处理迟到的请求 | [04 scan 与 reduce](book/04.md) · [05 debounceTime 等时间操作符](book/05.md) · [06 switchMap 等高阶映射](book/06.md) |
| 筛选随输入更新，面板共用连接，列表分批绘制 | [07 combineLatest 与 withLatestFrom](book/07.md) · [08 Subject 与 share](book/08.md) · [09 buffer 与 window](book/09.md) |
| 续接断开的日志，用测试核对行为，替换旧实现 | [10 HTTP 与 WebSocket](book/10.md) · [11 catchError 与 retry](book/11.md) · [12 TestScheduler](book/12.md) · [13 迁移](book/13.md) |
| 从事故记录还原故障，给导出和保存安排顺序 | [14 事故回放](book/14.md) · [15 导出中心](book/15.md) · [16 独立排错](book/16.md) · [17 控制台验收与展览预约](book/17.md) |
| 换一个场景自己动手 | [18 十道独立练习](book/18.md) · [实战篇：接星星游戏](game/README.md) |

每章的实验可以单跑，例如 `npm run lab -- 06`。前九章还有一个逐步装配的控制台。先跑 `npm run stage -- 5`，再跑 `npm run stage -- 6`，两次都在第 7 帧切到 B。看看哪一次真的开始读取 B 的历史。[控制台装配指南](book/控制台装配.md)

章内时序图从左往右读。同一条竖线上的事件发生在同一刻，横条的两端表示工作从何时开始、到何时结束。章末的 API 表可以按需查阅。

## 遇到具体问题时

| 现象 | 可以直接读的例子 |
|---|---|
| 只显示了第一行，后台却解码了整份历史 | [第 1 章的同步生产循环](book/01.md#首行预览为什么读完了整份历史) |
| 搜索框已经改成 B，却在停手的几毫秒里画出了 A | [第 6 章的防抖与请求切换](book/06.md#输入框已经是-b为什么还能显示-a) |
| 一个统计面板显示出从未同时成立的两个数字 | [第 7 章的两路统计](book/07.md#两个字段来自同一次修改中间能出现半张新表吗) |
| 屏幕刷新已经变少，待写入的行却越来越多 | [第 9 章的批量写入](book/09.md#画面不再卡顿积压去了哪里) |
| 备注保存超时，再点一次有可能多出一条 | [第 11 章丢失的回执](book/11.md#能重新订阅是否就能把业务操作再做一次) |
| 已经点了取消，旧请求还占着连接 | [第 12 章的订阅区间断言](book/12.md#测哪一刻才有机会推翻自己的实现) |
| 列表显示的内容和下载文件对不上 | [装配指南里的搜索框与导出按钮](book/控制台装配.md#搜索框和导出按钮读的是哪一刻) |
| 会读函数签名，仍拿不准空结果该怎么处理 | [声明、重载与行为实验](book/声明也要会用.md) |

[配套练习册](发散练习导航.md)收有 39 道跨场景题，按对应章节编排。比如 `npm run transfer -- 07` 会运行第 7 章的练习。查函数或旧重载可用 [API 正文索引](API-正文索引.md)和[公开声明清单](RxJS-7.8.2-API清单.md)；各章的闭卷题列在[课程大纲](RxJS-教程大纲.md)。

## 运行整套检查

```sh
npm run check
npm test
node tests/network.mjs
node labs/contracts.ts
node labs/18.ts all reference
node --test game/game.test.ts
```

网络实验会在本机随机端口启动服务。声明与实验均以锁文件里的 RxJS 7.8.2 为准。[Observable 官方指南](https://rxjs.dev/guide/observable) · [虚拟时间测试](https://rxjs.dev/guide/testing/marble-testing) · [多播迁移说明](https://rxjs.dev/deprecations/multicasting)
