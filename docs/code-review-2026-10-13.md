# 2026-10-13 全仓库代码审查

## 输入与结论范围

审查基准为 `a502ffa30df379c88f176c377cdd0f1dae4f3cd4`，产品源码末次提交 `2dc9554e`。
主工作区开始时干净；独立 EXPLAIN 工作区的未提交修改没有纳入本报告的产品结论。
本轮工作是检测、记录和文档更新，产品修复方案需另行实现及独立验证。

基准包含 `src/` 下 205 个文件、201263 行（含注释及模块 README），412 份已跟踪 Markdown。
静态检查覆盖文件清单和模块接口；深入审查按下面的模块表记录，不能推导为每行都已证明正确。

本轮已复现查询结果错误。标准回归正在运行，最终计数将在终态后填写；目前不能宣称全套通过。
原总账 273 项仍为 22 complete、166 partial、70 unverified、15 deferred_by_user。

## 检测计划与覆盖

1. 固定提交、生产二进制和源码摘要；盘点测试、模块、文档和独立工作区。
2. 并行检查查询执行、存储与目录、网络与复制、构建与测试入口；对具体疑点追溯调用链。
3. 使用独立测试目录运行标准注册回归，并以 PostgreSQL 18.6 对照新发现的 SQL 边界。
4. 汇总严重度、触发条件、实际/预期、代码位置、修复方案和验收测试。
5. 修订使用和状态文档，检查所有文档的本地文件链接，保留历史失败和尚未验证的边界。

| 模块 | 文件数 | 检查重点 |
| --- | ---: | --- |
| parser / expression / executor / main.cpp | 35 | 绑定、窗口语义、数值运算、关联子查询、EXPLAIN |
| storage / access / catalog / commands / transaction | 70 | 持久化、索引读取错误、TEMP 启动恢复、目录原子性 |
| network / replication | 16 | 二进制参数、连接池生命周期、复制状态 |
| common / types / process / utils / interfaces | 84 | 类型及错误边界、后台任务、运行状态、调用契约 |
| scripts / cmake / tests / 部署配置 | 单独盘点 | 源码清单、测试注册、语法、构建与文档一致性 |

用户此前暂缓的 15 项安全/TDE 专项，以及 CTAS／后台 checkpoint 专项诊断继续暂缓。
现有标准回归中的相关结果只如实登记，不扩展为该专项已完成。

## 证据登记

生产输入 SHA-256：`280da1dffcf542dbe9708fe9cbdea995609fc3363db8e01464f2198e6f969e72`。
生产二进制 SHA-256：`0a24168fe54d61b1382448b4df58a983a978ad1c12733c37c959930ef1b5c80a`。
标准回归日志：`/tmp/dbms-review-20261013.f9Ywep98/full-regression.log`。
原始运行日志为本机辅助证据，关键复现和结论会保存在本仓库中，避免只依赖临时文件。

已完成的机械检查：465 个 Python 文件 AST 解析、13 个 shell 文件 `bash -n`，均通过。
412 份 Markdown 的本地文件链接检查发现两处失效链接，均在历史 `all-gaps-todo.md` 中指向已移动的档案。
`check_gap_progress.py` 通过一致性检查；`--require-complete` 按预期失败，因为清单尚未完成。

本轮新增报告后共417份Markdown，本地文件链接复查通过；465 Python / 13 shell 语法检查不代表运行时通过。
现有文档状态、版本、兼容性契约检查和6个总账单元测试通过。
注册集合机械计数为785个普通native、3个真实Main frontend、444个Python driver，共1232个driver；这不是当前已通过数量。
五个未注册驱动另行运行，均实际exit 0，摘要在 `/tmp/dbms-review-20261013.f9Ywep98/unregistered-summary.log`：复制槽持久化、pg_settings查询、bound DML children、named ON CONFLICT、离线checksum验证。这些通过不消除注册缺口。

本机缺少 `cmake`，配置尝试exit127，未运行CMake构建；未安装额外依赖。标准脚本使用TLS stub/plain TCP，不能当作真实OpenSSL/TLS验收。
本轮未运行全库ASan/UBSan/TSan、fuzz、长期soak或实际多节点复制。新SQL对照共80条语句；47条观测不同，其中包含错误写入造成的后续状态差异，不能当作47个独立bug。正常binary float 6项中4项失败。

## 问题与修复方法

P1 表示可能导致错误结果、崩溃或服务不可用；P2 表示受限输入下的行为错误或验证缺口。
以下均未在本轮修改产品实现。完整 SQL、输出行和 OID 已保存到[原始观测](review-evidence-2026-10-13.md)。

| ID | 等级 | 问题 | 证据状态 | 建议修复顺序 |
| --- | --- | --- | --- | ---: |
| R01 | P1 | 大 NUMERIC 的 SQRT 返回负数 | PG18.6 对照复现 | 3 |
| R02 | P1 | 窗口排序、peer、MIN/MAX 猜测类型并丢失整数精度 | PG18.6 对照复现 | 4 |
| R03 | P2 | NUMERIC 等值但不同 scale 被拆成多个窗口分区 | PG18.6 对照复现 | 4 |
| R04 | P1 | 窗口 frame 丢失边界方向，非法组合仍成功 | PG18.6 对照复现 | 5 |
| R05 | P2 | NTILE 桶大小分配不正确，桶号跳号 | PG18.6 对照复现 | 6 |
| R06 | P2 | NTILE 非正桶数被静默改为 1 | PG18.6 对照复现 | 6 |
| R07 | P2 | LAG/LEAD 零偏移及负偏移错误 | PG18.6 对照复现 | 6 |
| R08 | P1 | binary float4/float8 参数截断到六位小数 | 正常协议精确回传复现 | 2 |
| R09 | P1 | 连接池等待期间保存了可被删除的容器引用 | 源码生命周期确认，未跑并发/ASan | 1 |
| R10 | P1 | 同用户跨数据库连接池可能永久等待 | 源码条件链确认，未跑阻塞探针 | 1 |
| R11 | P1 | 窗口精确 SUM/AVG 仍执行可能溢出的 int64 中间累加 | 静态 UB 风险；O2 结果未失败，未跑 UBSan | 4 |
| R12 | P2 | 独立测试未进入完整注册回归 | 文件与调度列表核对 | 7 |
| R13 | P1 | 逻辑变更行像不能区分 NULL、空串和字段分隔符 | 源码编码链确认，未做运行时流对照 | 3 |
| R14 | P1 | publication 读取失败被当作未发布，可能漏采变更 | 静态错误路径确认，未做故障注入 | 3 |
| R15 | P1 | 服务停机先 join 再唤醒池等待者，可能无法退出 | 源码等待链确认，未跑停机探针 | 1 |
| R16 | P1 | 损坏 catalog 行被跳过，后续可能将缺行状态写回 | 静态路径确认，未做故障注入 | 2 |
| R17 | P1 | DDL 回滚失败仍删除恢复备份 | 静态错误路径确认，未做故障注入 | 2 |
| R18 | P2 | catalog getter 返回的内部行指针可能失效 | API 生命周期风险；SQL 并发可达性待验证 | 2 |
| R19 | P1 | 失效 session-temp 元数据阻断普通启动恢复 | 历史全套失败及本轮源码追踪；当前回归待终态 | 2 |
| R20 | P1 | INSERT/UPDATE RETURNING 子查询被当成字符串，错误写入提交 | PG18.6 对照及后续表内容复现 | 1 |
| R21 | P1 | ON CONFLICT WHERE 缺少 BOOLEAN 绑定，`'f'` 也更新 | PG18.6 对照及后续表内容复现 | 1 |
| R22 | P2 | 严格模式接受缺少 FROM/INTO 的修改语句 | 显式 postgresql18 模式对照复现 | 3 |
| R23 | P1 | 后台维护状态读取/持久化失败仍执行任务 | 静态控制流确认，未运行故障探针 | 2 |
| R24 | P1 | SQL EXECUTE 参数按引用次数/行数求值，未引用参数不求值 | PG18.6 对照及 sequence 状态复现 | 1 |
| R25 | P2 | Numeric::withPrecision 丢失整数数量级 | 静态公共 API 问题；没有生产调用者 | 7 |

### R01：大 NUMERIC 平方根

位置：`src/expression/ExprEvaluator.cpp:8809`、`:8817`。

```sql
SELECT sqrt(10000000000000000000000000000000000000000::numeric);
```

预期 `100000000000000000000`，实际 `-9223372036854775808`，双方结果 OID 均为 1700。
输入经 `asDouble()` 丢失精度，整数形状的结果又直接转为 `long long`；超出 int64 的浮点转整数是未定义行为。

修复：按已绑定的 numeric/float8 重载分别计算。NUMERIC 使用十进制算法并保留结果 scale，禁止经 double/int64 中转；float8 使用对应浮点处理。仅检查 int64 上限只能阻止非法转换，不能恢复 NUMERIC 精度。
验收：大数及相邻值、边界、小数、NULL/NaN/Infinity、负数错误、参数和表列路径。

### R02、R03：窗口比较和分区等价关系

位置：`src/executor/ExecutionPlan.cpp:3784` 的 `compareNonNullAggregateValue`、`:3807` 的窗口比较和 `:3817` 的 `sameWindowPartition`。

实证：TEXT `('2','10','1','01')` 被猜成数值比较，`'01'` 与 `'1'` 变成 peer，MIN 返回 `'1'`；BIGINT `9007199254740993` 与 `9007199254740992` 因 double 舍入被当成相同值，RANK 和 MIN 错误；NUMERIC `1.0,1.00,1,2` 的分区 COUNT 实际为 `1,1,1,1`，预期 `3,3,3,1`。

修复：将声明类型和 collation 传入统一类型化比较器。整数保持精确、NUMERIC 按十进制、文本按文本规则比较；排序、peer 和分区使用一致的 SQL 等价关系，NULL 依照分组语义处理。不能对所有字符串尝试 `strtod`，也不能以原始字符串差异决定 NUMERIC 分区。
验收：数字状 TEXT、空字符串/NULL、2^53 附近 BIGINT、高精度 NUMERIC、不同 scale、复合分区、ASC/DESC、RANK/DENSE_RANK/MIN/MAX 和 peer frame。

### R04：窗口 frame 边界

位置：`src/main.cpp:6022` 的窗口解析；`src/executor/ExecutionPlan.cpp:4085` 的 frame 计算。

共同输入 `w(id,v) = (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)`：

| SUM(v) frame | PostgreSQL 18.6 | 当前程序 |
| --- | --- | --- |
| `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING` | NULL,10,20,30,40,50 | 10,30,50,70,90,110 |
| `ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING` | 50,70,90,110,60,NULL | 30,60,100,150,210,210 |
| `ROWS BETWEEN CURRENT ROW AND 1 PRECEDING` | SQLSTATE 42P20 | 成功返回当前行的值 |

修复：从 typed WindowDef AST 保存两端各自的 boundary kind 和表达式，分别表示 unbounded/current/offset preceding/offset following；绑定阶段验证非法组合，执行时计算、裁剪并保留空 frame。现有两个整数偏移无法完整表达方向。
验收：ROWS/RANGE/GROUPS、全部合法方向组合、简写、非法组合、NULL/负数 offset、分区边缘、EXCLUDE、空输入与 LIMIT 0 的验证阶段。

### R05、R06：NTILE

位置：`src/executor/ExecutionPlan.cpp:4164`、`:4166`；legacy 分支 `src/main.cpp:35825`。
六行输入下 `ntile(4)` 实际为 `1,1,2,3,3,4`，预期 `1,1,2,2,3,4`；`ntile(8)` 实际为 `1,2,3,5,6,7`，预期 `1,2,3,4,5,6`。
`ntile(0)` 和 `ntile(-1)` 应返回 SQLSTATE `22014`，实际均成功返回桶号 1。

修复：用商 `q=rows/buckets` 和余数 `r=rows%buckets` 分配，前 r 桶各 q+1 行，其余各 q 行；桶数超过行数时连续编号。参数非 NULL 且非正时抛结构化 `22014`，保留 NULL 语义。统一两个执行路径，避免乘法溢出和默认值掩盖错误。
验收：6/4、6/8、7/3、整除、单行、多分区、NULL、非正桶数、参数及列表达式。

### R07：LAG/LEAD 偏移

位置：`src/main.cpp:6285`、`:35798`；`src/executor/ExecutionPlan.cpp:4142`。
在上述六行输入中，`lag(v,0)`、`lead(v,0)` 均返回全 NULL，预期返回当前行 v；`lag(v,-1)` 也错误返回全 NULL。
偏移经 `stoull` 解析，零导致 legacy 回退，负数可变成巨大无符号值；执行器还把 offset 强制为至少 1。

修复：按 SQL INTEGER 保存有符号偏移，零取当前行，负数反转方向；使用足够宽的有符号计算后再检查分区边界。保留 NULL offset 和 typed default，避免回退到把 `v,0` 当列名的字符串分支。
验收：0、±1、INT_MIN/MAX、多分区、NULL offset、默认空文本/NULL/字面量 `NULL`。

### R08：二进制浮点参数

位置：`src/network/NetworkServer.cpp:753`、`:764`，Bind 消费位置 `:5992`。
使用正常 Parse/Bind/Describe/Execute/Sync，`SELECT $1` 要求 binary 输入原样作为同 OID 的 binary 输出：

| 输入类型和值 | 当前输出 | 结果 |
| --- | --- | --- |
| float8 0.1234567890123456 | 0.123457 | 丢精度 |
| float8 1e-12 | 0 | 丢值 |
| float4 1.2345678806304932 | 1.2345679998397827 | 位模式 3f9e0652 变为 3f9e0653 |
| float4 约 1e-12 | 0 | 丢值 |
| float4/float8 1.25 | 1.25 | 两个阳性控制通过 |

原因是解码后调用 `std::to_string`，默认只有六位小数，之后 CAST 无法恢复输入。
修复：使用保真的 typed datum；若现有接口必须返回文本，则以 `to_chars` general/max_digits10 或已验证的往返格式化函数输出。保留 NaN/Infinity、负零和次正规值语义。
验收：输入/输出逐字节比较、OID/format code、正常事务状态、float4/8 极值、小值、负零、非有限值和 INSERT/UPDATE 参数。

### R09、R10：连接池生命周期和等待条件

位置：`src/network/ConnectionPool.cpp:58`、`:83`、`:129`；真实消费见 `src/network/NetworkServer.cpp:4953`、`:4967`。

R09：acquire 保存 `pools_[user]` 的 vector 引用后 `cv.wait` 解锁；另一线程 discard 最后一个 context 时删除整个 map 节点，等待谓词和后续遍历继续使用该引用。正常连接关闭与另一同用户连接等待即可形成这条生命周期路径。本轮未运行并发探针或 ASan，不声称已观测到崩溃。

R10：容量按 user 计算，复用却要求 database 一致。pool_size=1 时，db1 的 idle context 占满容量，随后同 user 请求 db2 会等待一个永远不会匹配的 context；等待条件也没有“容量已释放，可以创建”的分支。本轮未执行阻塞探针。

修复：把 acquire 改为持锁重查 key 的循环，任何唤醒后重新寻找容器；以 `(user,database)` 明确池的身份和容量规则，或提供安全的闲置 context 重建策略。等待条件应包含 shutdown、可复用 context、可创建容量；不要跨 wait 保存可删除元素的引用。
验收：pool_size=1、同用户跨库、release/discard、等待期间关闭、并发租还、取消/超时及 ASan/TSan。每个连接的 session 状态重置须单独验证。

### R11：窗口 SUM/AVG 中间有符号溢出

位置：`src/executor/ExecutionPlan.cpp:4267` 的 `sum += number`。
即使最终使用 `Numeric exactSum`，两个 MAX_BIGINT 输入仍先执行 int64 加法。当前 O2 对照结果正确，因此本项是静态 UB 风险，不能登记为已复现错值或 UBSan 已通过。
修复：删除无用的 int64 后备累加，或使用有检查且符合结果类型的累加器；精确累加失败应传播错误，禁止回退到已经溢出的中间值。
验收：实际 WindowOp 的 UBSan 测试，包含 MAX+MAX、MIN+MIN、AVG 和累计 frame。

### R12：测试注册遗漏

`scripts/build_tests.sh` 只调度 `scripts/build_common.sh` 的 `DBMS_E2E_TESTS`，不会自动扫描所有 Python 文件。
以下独立 E2E 未在该列表中：`replication_slot_persistence_e2e_test.py`、`pg_settings_query_protocol_e2e_test.py`、`bound_dml_query_children_protocol_e2e_test.py`、`on_conflict_constraint_protocol_e2e_test.py` 和 `checksum_verification_e2e_test.py`。
源码清单则完整，TLS 的两个实现按配置二选一加入，没有生产 TU 遗漏。

修复：核对每个测试的依赖和预期运行环境，接入规范入口；对需专门环境的驱动明确单独入口和理由。新增测试清单一致性检查，使文件新增却未注册在检查中可见。CMake check、ci 和安静入口均应继续共用该清单。
验收：每个新增注册驱动实际运行，并验证清单检查能捕获人为遗漏；不能只改统计数字。

### R13、R14：逻辑变更预览的值与连续性

位置：`src/commands/TableManage.cpp:26282` 的 INSERT 行像、`:31177` 的 UPDATE 行像；
`src/replication/LogicalDecoder.h` 的 LogicalChange 和 `LogicalDecoder.cpp:24`、`:54`、`:604`。

R13：行像按 `|` 拼接各列，只追加非 NULL 的文本值，因此两列 `(NULL,'x')` 和 `('','x')` 都变成 `|x`；`('a|b','c')` 与 `('a','b|c')` 都变成 `a|b|c`。输出插件只为整行添加长度，不能恢复已丢失的字段边界和 NULL 标志。
修复：以 nullable typed cells 保存各列，逐列编码长度/NULL/类型信息，对持久化流格式明确版本。测试 INSERT/UPDATE/DELETE 的 NULL、空串、分隔符、多字节和回滚。

R14：`PublicationCatalog::publishes` 调用严格 list 失败后返回 false，与合法“不属于任何 publication”相同；写路径据此跳过逻辑变更缓冲。若已发布对象的元数据发生读取错误，DML 可能成功而变更流没有对应记录。本轮没有实施该故障注入。
修复：返回可区分成员关系与读取失败的结果，向事务传播错误，或明确使受影响槽失效并要求重新同步；不能静默宣称流连续。测试读失败、非法元数据、正常无 publication 和已有槽的连续性。

这些问题属于本项目的 `dbms_pgoutput_preview` / `dbms_test_decoding` 预览功能，不表示真实 PostgreSQL 流复制已经实现。

### R15：停机顺序

位置：`src/network/NetworkServer.cpp:6675` 至 `:6677`。
当前顺序是关闭客户端 socket、join 所有 worker、最后 `ConnectionPool::shutdown()`。
阻塞于 acquire 条件变量的 worker 并不在等 socket，因此关闭 socket 不能唤醒它；join 又阻止后面的池 shutdown 执行。
修复：先设置池 shutdown 并通知等待者，再等待 worker 退出；同时核对其他关停和重启入口的一致性。
验收：满池或跨库等待期间正常请求停机，有限时退出且无遗留 worker，保留已接收事务的正确清理行为。本轮未执行 hang 探针。

### R16：损坏 catalog 行不能静默消失

位置：`src/catalog/catalog.cpp:2215`、`:2276`。
pg_class / pg_attribute 可选尾段解析错误执行 continue；loadAllLocked 随后接受缺行模型为 baseline，后续 persistAll 可能将该模型写回。基础前缀的流状态检查同样不完整。
修复：完整校验可接受格式的所有字段、布尔值和尾部；坏行抛结构化损坏错误，I/O 失败返回读取错误。通过现有 replacement-model 异常恢复保留原缓存和磁盘，禁止跳过坏行后发布。
验收：两种表的截断/坏字段/尾随数据/读取失败，验证未改变已有文件和已加载模型。现有 pg_type 坏行测试不覆盖这两个解析器。

### R17：失败回滚的恢复备份

位置：`src/commands/DdlTransaction.cpp:266`、`:276`；`src/commands/TableManage.cpp:50141`。
rollbackTransaction 返回非 OK 后仅输出 warning，随后仍调用 discardTransactionBackup；事务 ID 已清零时也允许删除已拥有的备份。
修复：只有完整持久回滚确认成功后才清理恢复图像。失败时保留备份及状态，向调用者报告 incomplete rollback；明确下次启动的恢复步骤。
验收：引擎回滚失败、DDL snapshot 恢复失败及随后重启。正常回滚通过不能证明本错误分支安全。

### R18：catalog 行指针的寿命

位置：`src/catalog/catalog.cpp:646`、`:656` 返回 vector 内部地址；`:485` 的 createClass 可能重分配，peer reload 也替换模型。`src/commands/TableManage.cpp:19625` 在 getter 锁释放后复制返回值。
本轮确认 API 层面的悬空风险，但尚未证明普通 SQL 的外层数据库锁允许具体交错。
修复：返回 `optional<Row>` 值快照，或提供不可变 snapshot/显式生命周期守卫；仅给 getter 加 mutex 不能保护返回后的指针。
验收：查询、创建、刷新和 evict 的并发 API 测试，再核对真实 SQL 的锁层级和吞吐影响。

### R19：失效 TEMP 条目阻断启动

原完整日志 `root-full786-443.log` 的 `stale_temp_startup_recovery_test` 实际异常退出。
`src/commands/TableManage.cpp:43821` 枚举全部关系建立 physicalRelationId 映射，`:43823` 在临时条目缺少 schema 时直接失败；普通启动构造器据此中止。已有较晚的 specialized-index 临时名过滤无法覆盖该入口。
修复：恢复的持久关系枚举统一排除真正的 reserved session-temp 名称，保留普通持久表缺 schema 的 fail-closed 行为；一并检查同文件 `42835`、`42918`、`43667` 相邻枚举。
验收：原测试不缩减，并覆盖有 lifecycle WAL、残留 TEMP、健康持久表和持久 schema 真缺失的不同情况。

### R20：RETURNING 子查询没有执行

位置：`src/commands/DmlExecutor.cpp:1367`、`:2437`、`:2993`、`:4045`；子查询占位的来源为 `src/parser/parser.cpp:2765`。

```sql
CREATE TABLE e(id INTEGER PRIMARY KEY,v INTEGER);
INSERT INTO e VALUES (1,10),(2,20),(3,30);
INSERT INTO e VALUES (4,40) RETURNING id,(SELECT 1/0);
UPDATE e SET v=99 WHERE id=1 RETURNING id,(SELECT 1/0);
INSERT INTO e VALUES (5,50) RETURNING (SELECT v FROM e);
```

PostgreSQL 18.6 分别报 `22012`、`22012`、`21000`，数据不变。
当前程序把子查询源文本以 VARCHAR/OID1043 返回，随后 SELECT 证明 id4/id5 已插入、id1 已改成99。
正常 COUNT 子查询也返回源文本；DELETE 的对应控制则正常执行，缺陷不能归结为所有 RETURNING 均不支持。

原因：子查询最初以 LiteralExpr 保存并由 binder 赋予 preparedSubquery；INSERT/UPDATE 虽创建绑定计划，RETURNING 仍消费原 `stmt.returning`，普通 evaluator 将未绑定占位当成字符串。错误没有发生，语句正常提交。
修复：使用完整绑定 AST 的 RETURNING 表达式和静态描述符，通过 PreparedQueryExecution 的子查询执行器传入真实 OLD/NEW 行上下文；分析与执行错误保留 SQLSTATE，同一语句边界内清除部分结果并回滚。不能只按字符串外观拒绝子查询。
验收：三种 DML 的正常计数、空子查询、21000/22012、关联子查询、多行、OID/name、PREPARE/EXECUTE，失败后检查表内容和事务状态。

### R21：ON CONFLICT 条件没有 BOOLEAN 上下文

位置：`src/expression/query_binding.cpp:1516`；`src/commands/DmlExecutor.cpp:3376`。

```sql
INSERT INTO e VALUES (2,200)
ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 'f' RETURNING id,v;
```

预期 `INSERT 0 0`、原 id2 保持20；当前返回 `(2,200)` / `INSERT 0 1`，实际表已修改。
`WHERE 1` 应报42804，`WHERE 'notboolean'` 应报22P02，当前都被当真；没有冲突时非法条件也没有在分析期拒绝，PREPARE 同样错误成功。

原因：binder 只解析条件，没有 BOOLEAN 验证及 UNKNOWN 转换；执行时又丢弃类型，只把 NULL、空串、`0`、`false` 当假。
修复：分析阶段复用普通 WHERE 的 BOOLEAN 验证/UNKNOWN 转换，执行实际消费已绑定的 typed ExprValue，仅非 NULL 且 asBool 为真才更新。确保无冲突、空源和 PREPARE 也按正确阶段验证。
验收：true/false/NULL、`'t'/'f'/'off'/'notboolean'`、1、NULL::integer，冲突/不冲突、VALUES/SELECT 和预备语句；检查 SQLSTATE、tag 及表内容。

### R22：严格模式中的缺失关键字

位置：`src/parser/parser.cpp:4375`（DELETE）、`:4013`（INSERT）。
显式 `postgresql18` 模式下，`DELETE e WHERE id=3 RETURNING id` 仍实际删行，`INSERT e VALUES(7,700) RETURNING id` 仍实际插行；PostgreSQL 两者均报42601。
修复：通用严格 parser 必须要求 DELETE FROM 和 INSERT INTO，缺失时在名称解析、表达式执行和修改前报42601；如确有项目扩展需求，应由显式 extended 模式单独路由。
验收：parser/native、Simple Query 和 PREPARE；正确/缺失关键字、注释、大小写、ONLY/别名/RETURNING，并确认拒绝后数据和 sequence 不变。

### R23：后台维护任务在持久化不健康时继续执行

位置：`src/process/MaintenanceJobManager.cpp:126`、`:142`、`:264`、`:277`、`:300`、`:329`、`:362`。
loadLocked 先清空 jobs 再逐行插入，后续坏行导致失败却留下已解析的 Queued 前缀；构造器仍启动 worker。worker 不检查 persistenceHealthy，且写入 Running 状态失败后也继续调用 runner。结束状态持久化失败后，剩余队列仍可能执行。
修复：完整解析到临时模型，全部成功后交换；以 persistenceHealthy 控制 dispatch，只有 Running 状态持久成功才调用 runner。失败时保留可恢复状态并向操作方明确报告，不能执行被拒绝状态文件中的部分任务。
验收：有效前缀加坏尾部、Running 写入失败、终态写入失败、正常重启恢复/取消/限速。此项未做动态故障验证。

### R24：SQL EXECUTE 参数求值次数

位置：`src/main.cpp:5131`、`:5146`、`:5153`、`:5186`；`src/utils/prepared_stmts.cpp:275`。

```sql
CREATE SEQUENCE s;
PREPARE p(bigint) AS SELECT $1,$1;
EXECUTE p(nextval('s'));
SELECT currval('s');
```

预期返回 `(1,1)` 且 currval=1；当前返回 `(1,2)` 且 currval=2。
声明了参数但模板只有 `SELECT 7` 时，PG18 仍求值实参一次，当前完全不求值；两行查询的同一 `$1` 则在当前程序返回1和2，PG18两行均为1。
Root 输出列名也受到替换表达式污染。14条语句对照和 sequence 状态已保存到[原始观测](review-evidence-2026-10-13.md)。

原因：EXECUTE 保存原参数表达式文本并附 CAST，在每个 `$n` 位置重复拼入，再重新执行 SQL。求值次数由模板引用和行数决定，而非 EXECUTE 的参数边界。
修复：EXECUTE 入口绑定并按声明类型对每个实参求值一次，保存 typed/nullable cells；使用保存的 prepared AST/plan 和同一参数 slot 执行，保留原输出描述符。未被模板使用的声明参数也按参考行为求值。
验收：重复引用、未引用、多行、NULL、错误、多个参数的求值顺序、类型转换失败以及 DML 消费者。该问题与 wire Bind 已有参数修复不同。

### R25：Numeric 公共 API 的有效数字舍入

位置：`src/types/numeric.cpp:618` 至 `:621`。
withPrecision 删除整数末位后将负 scale 钳为0，却没有补回数量级；静态推导 `Numeric("12345").withPrecision(3)` 为123，预期12300；999取两位会成为100，预期1000。
本轮没有动态验证，源码搜索没有发现生产调用者，不能由此宣称 SQL NUMERIC typmod 当前错误。
修复：校验 precision，复用保留负 scale 数量级的 withScale 逻辑；验证整数、负数、进位和非法精度，以及现有小数控制。

## 历史失败与待判定项目

| 项目 | 已有证据及正确解释 | 下一步 |
| --- | --- | --- |
| table_owner_atomicity | 原全套在第105行健康重试返回非 OK；实际 status 未记录，不能据此认定数据回滚失败 | 记录 status、schema/catalog 发布阶段和 peer generation；确认原因后修复并保留重试断言 |
| owner 测试覆盖说明 | 第82行称 class 先发布、attribute 后失败，但当前 persistAll 在所有写入前检查目录，且 map 顺序为 attribute 在前 | 保留原控制，另加真实分阶段发布失败控制，不能用现有绿色断言证明部分发布恢复 |
| index_corruption / EXISTS | 原 fixture 期望 XX001，实际 t；EXISTS 固定读取健康 heap，没有证明它访问了被破坏的 B-tree | 先核对所要求的访问计划；若接入公共 planner，再验证错误传播，不能无条件检查未访问索引 |
| ALTER ARRAY | 前次同源码专项在建立两个索引后执行 `ALTER TABLE array_values ALTER COLUMN i TYPE BIGINT[]` 超时 | 保留失败和原10秒期限，定位等待/错误边界，不将超时直接归因于环境 |
| EXPLAIN / JOIN | 原 OFFSET 1 负向 fixture 已过时：查询实际可正确返回4行，但该路线的 EXPLAIN 又缺少 JOIN/scan 子图 | 分别修正合法行为的断言和真实执行图；私有补丁还存在关联重绑计数遗漏，尚未合入 |
| CTAS / background checkpoint | 原正式回归存在58030失败，后续重复通过不抹掉失败 | 按用户要求暂缓专项调查，保持未修复 |
| HAVING 非 BOOLEAN | 新差分 `HAVING 1` / `HAVING 'notboolean'` 实际成功空结果，PG18分别42804/22P02 | 追踪实际 aggregate/HAVING 消费路径，共用 BOOLEAN 绑定；保留空输入和准备阶段控制 |
| ORDER BY 重复别名 | 新差分 `SELECT id AS x,v AS x FROM d ORDER BY x` 与无FROM双 x 均未返回预期42702 | 在 ORDER BY 名称解析阶段检查多重匹配，区分合法同一表达式引用与歧义；核对标签及行序 |

## 实施顺序

优先修连接池生命周期/等待/关停、回滚备份/catalog读取、参数精度和已确认的数据修改错误，再处理数值与窗口比较、frame、NTILE/LAG及逻辑变更预览；每个问题独立提交并加入能先失败后通过的回归。
修复后先跑匹配的专项及相邻测试，再跑注册回归；性能、并发和 sanitizer 结果分别登记。
本轮的文档提交不改变上述问题的未修复状态，也不改变原 273 项完成统计。
