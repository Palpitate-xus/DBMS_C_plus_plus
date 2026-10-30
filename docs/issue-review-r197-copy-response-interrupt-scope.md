# R197：COPY 响应可见前必须建立中断作用域

状态：按用户最新“skip it”跳过；候选源码、夹具与下列已有证据保留在工作区，未提交、未验收。
不再继续本项测试或实现，也不将候选计入已接受修复。以下记录描述跳过前的候选及观察，
不是全部 COPY 或 PG 取消语义验收。

## 原失败与可以证明的范围

冻结全量的 COPY 夹具在等待自身有效 CancelRequest 的 57014 时超时，位置是原第 243 行、
明确的 15 秒 socket 界限，不是后续 256×16KB 导入的位置。
未改生产代码的两套当前服务器复跑完整原夹具均通过；最新 R196 服务器在修复前也均通过。
这些通过不能解释或改写原失败，也不能据此宣布问题已消失。

代码中另有可独立复现的窗口：CopyInResponse G 已发送，`receiveCopyIn` 才加载 schema、
构造 CopyInterruptGuard，清除取消标志并把 queryActive 设为 true。
客户端看见 G 后发出的有效取消可能在这个窗口被当成非活动查询忽略，或被后建作用域清除。

本机外部测试在 `sendCopyInResponse` 成功写出 G 后暂停 500ms，模拟合法后台调度抢占。
调度点、前端帧、有效自身 PID/secret、15 秒界限在对照中完全相同：

| 实际程序 | Simple | Extended |
| --- | --- | --- |
| 未插入调度点的旧代码 | 通过 | 通过 |
| 同一外部调度点，旧作用域 | 等待 ErrorResponse 超时，退出 1 | 同样超时，退出 1 |
| 同一外部调度点，修复作用域 | 57014、I、空表、后续 SELECT42 | 同样通过 |

六个实际进程包含两次预期旧红，不是六次程序退出 0。
这证明一处丢失取消的窗口，不证明原冻结运行唯一由该窗口导致。
外部调度代码只存在于临时 PostgresProtocol 对象，未安装、未提交到生产源码。

## 修改方法

在 COPY 内部事务边界建立后、发送 G/H 前读取真实 schema。
为响应及数据流建立单一 CopyInterruptGuard，让中断状态先于可见响应激活；
输入助手接收该 guard 引用，保留失败时 suspendForCleanup 的原锁释放行为。
两个流助手复用同一 const schema 引用，不再在响应后重建并清空中断状态。

作用域仍在 finishCopyBoundary 之前结束，与原数据流结束后的定时器／清理所有权一致。
响应发送失败的事务收尾仍在 guard 外。没有生产 sleep、调大超时、改持久化设置、
改数据格式、删原 SQL 或减少断言；所有 schema、整数输入和错误码检查保留。

## PostgreSQL 差异必须单独记录

新建实际 PostgreSQL 18.6 实例，按同一“看见 G、有效自身取消、不再发送数据”探针运行时，
首个 Simple 场景也在 15 秒等待 ErrorResponse 处超时；该次 Extended 场景未执行。
实例版本检查为 180006，结束时自有 PG 正常停止 0。
因此它不是 PostgreSQL 会立即中断空闲 COPY 的参考证明，也不能把这一有限观察推广到所有取消流程。

本项目现有夹具明确要求等待下一条输入时也主动响应取消，旧程序正常运行时已经实现该行为；
本项只使这个既有约定在响应窗口中保持一致，不宣称因此取得完整 PG 取消兼容性。
两个已有 COPY 整数输入、错误后锁释放夹具则另在私有 PG18.6 完整通过，证据范围不同。
所有请求仅使用本次自建服务和自身连接的有效标识，不涉及认证、伪造标识或外部服务审计。

## 永久测试、构建和验证

原完整 COPY 夹具所有 SQL、断言、15 秒界限、256 行大数据和慢读端 statement_timeout 检查保留。
新增 Extended P/B/E 的主动取消，要求 57014、Sync 后 I，以及原全部 id 导出字节保持不变。
原 Simple 取消和新增 Extended 取消都完成后输出两模式计数标记，最终仍必须到达原完整 footer。

两套最新 R196 基础上的候选各实际完成 11 个完整入口与两个未插桩取消探针，共 26 个进程退出 0：
原 COPY、扩展 COPY、copy_integer_input、copy_error_lock_release、statement_timeout（含部分 COPY 帧）、
transaction_begin_grammar、set_transaction_modes、subtransaction_error_lock_release、pg_class_query、
prepared_global_aggregate、computed_query_host_metadata，及 Simple/Extended 自身取消探针。
R196 的派生 COUNT 元数据和 331 项描述检查均保留通过。
永久夹具安装后，两套程序另完整运行增强 COPY，各退出 0。

两侧各 fresh 编译 1 个 NetworkServer 并链接，分别核对其余 57 个生产对象、127 个公共头、
编译器、参数和 stub。工作区实际执行正常构建，重复零编译／零链接；
安装永久 Python 夹具后再次正常构建也零编译／零链接。默认 dbms_main 实际参与完整验证。
没有重新编译 Main、全部生产代码或原生测试的声明。正常工作区含原有未接受改动，
通过不表示接受它们；本项精确组合在最新 R196 源码上，没有移植旧探针整文件覆盖元数据修复。

## 本机证据

- 未改原完整夹具复跑：`/tmp/dbms-copy-full-current-observation.310kdube/completed-observation-receipt.json`，SHA256 `fbf716bee699ac5ff44b58c0285df764e39d5f7fb4dd502b527320a78222056e`。
- 六进程调度对照：`/tmp/dbms-copy-response-window-2onrxikq/completed-controlled-observation.json`，SHA256 `9cc64b9de23acb7ea2792a9afa71f61a54da04d8c76430b0e913817567b72721`。
- PG 空闲等待对照（实际退出 1）：`/tmp/dbms-private-pg18-full-fixture.2beqdvlj/completed-reference-receipt.json`，SHA256 `075528ad431a78fc268e22fcdaf42186a708c693158b6f848536eda156a006d8`。
- PG 整数输入／错误锁释放完整参考：`/tmp/dbms-private-pg18-full-fixture.a1th1iut/completed-reference-receipt.json`，SHA256 `c93d88b5aae2f3d56fe2098d517f231972bebc550e74f549e91333df1d164771`。
- 当前源码两套 26 进程与正常构建：`/tmp/dbms-r197-copy-scope.uenbfeze/completed-candidate-receipt.json`，SHA256 `3e47c56d67dc77d75131da47e063100f21f3b29d68fd05c1391f3c96320ef79f`。
- 永久夹具两次完整执行：`/tmp/dbms-r197-permanent-fixture.fn_9i2qe/completed-permanent-receipt.json`，SHA256 `f1ed3d70a5d0ee061cae07965fc87cd2e7761beacde0cf00417269302f6d209b`。

这些 `/tmp` 是本机临时证据，不是可移植发布包。旧对象／默认服务器在缓存更新前已复制保留。
未证明所有并发输入、取消后已排队帧、COPY WAL/崩溃、TLS 或 sanitizer 场景；相关静态猜测不算已定位漏洞。
原冻结全量仍保留 1481 PASS/11 FAIL，未运行修复后全 1492，原 273 项未全部闭合。
用户跳过的默认协议 search_path、R185 及其他暂缓模块保持原状。不 push，Actions 不启用。
