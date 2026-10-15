# 冻结27bac6ee的完整注册回归终态

未过滤的 `bash scripts/build_tests.sh` 已完整结束，2026-10-15 07:52 UTC
确认实际退出1。1368个预期入口各出现一次，无missing、extra或duplicate。
本轮不是全量通过，也没有因首次失败而提前停止。

| 输入/产物 | SHA256或固定值 |
| --- | --- |
| HEAD | 27bac6ee2e3197ef11ecba4637479c2032e53712 |
| 全部tracked输入 | d9e1a41cb0a2d8e1c2da9c5079cd0873f786ce644d8e854f22337e4cc735ba1f |
| 生产binary | 00c89d047b6e3cf91af3c7fe2d1be90f11092edbda97b0f7dd848ec0d645e984 |
| 生产cache输入 | 7f3f76d44e09520b3283d4bef3b01f6666db510df4da63293292d2698d057622 |
| 完整日志 | 863ae11897902ca7cc5f5b500c79b05e33d967270ead53bbf6073a864aa5180e |

| 类别 | 全部入口 | PASSED退出 | FAILED |
| --- | ---: | ---: | ---: |
| native | 864 | 863 | 1 |
| 真实Main frontend | 3 | 3 | 0 |
| Python注册入口 | 501 | 496 | 5 |
| 合计 | 1368 | 1362 | 6 |

1362个PASSED退出含1个明确的TLS环境skip：实际通过1361、环境跳过1、失败6。
TLS驱动报告binary没有OpenSSL支持；不将其记为真实TLS运行通过。
没有COMPILE FAILED或LINK FAILED，不据此宣称全库sanitizer、CMake或完整PG兼容。

## 冻结与完整性证据

原始目录 `/tmp/dbms-review-full-20261015-r85.6HbDjOD6/` 包含
run-terminal.log、full-regression.log、expected-drivers.txt、driver-summary.log。
包装器末尾检查HEAD、全部tracked输入、生产binary/cache及全部58份receipt一致。
07:52 UTC另做独立只读复核：清洁HEAD、同一输入/binary/cache/build stamp、
全部58份生产receipt及57份test库对象与当前Root对象逐字节相同，退出0；日志为
`/tmp/dbms-review-r46.ifu6SWLp/r85-full-terminal-fence.log`。

开跑前逐字节复制相同Root的57份当前O2库对象，fresh编译当前test stubs，
见 `r85-full-cache-preparation.log`。全部native仍逐文件实际编译、链接、
隔离执行；三个真实Main前端及501个Python入口均按原注册集合执行。
运行期间没有改tracked输入、HEAD、SQL、断言、socket期限或注册集合。
缓存复用不等于重新编译全部58个生产单元；临时路径也不是可移植发行产物。

## 六项失败及修改/定位方法

| 入口 | 原始观测 | 后续方法与边界 |
| --- | --- | --- |
| index_scan_corruption_error_test | 前置损坏检测、健康图像恢复、显式rollback重试、REINDEX及写入断言执行后，line181末尾DROP DATABASE断言exit134；日志未打印DROP实际返回状态 | 保留原失败及完整断言，不猜测为状态25或R65同根因。R51 DROP/cache专项仍按用户要求暂缓；本次无过滤运行不意味着重开该专项 |
| postgres_protocol_test.py | line2971发送 `CREATE TABLE dec_t (id INT PRIMARY KEY, v NUMERIC, name TEXT)` 后socket timeout | 原完整驱动、原期限复现，记录确切SQL/响应耗时和失败线程状态，区分锁、同步I/O及执行路径后按确证根因独立修复，不提高期限或归因于ROUND |
| materialized_lateral_composition_protocol_e2e_test.py | 前面的LATERAL组合断言执行后，line68两语句循环等待超时：`CREATE TEMP TABLE __cte_0(keep TEXT)` 或随后INSERT；日志未打印触发的是哪一个 | 完整驱动原样观测，补发送语句与owned-server失败栈，保留真实用户TEMP关系不被内部物化器覆盖/删除的阳性断言，不把该超时称为已证实LATERAL错值 |
| copy_protocol_e2e_test.py | line262：`COPY copy_wire (id, payload) FROM STDIN` 的256条大记录已发送CopyData/CopyDone，等待finish_simple_copy超时；每条payload为16384字节，原socket期限15秒 | 在原完整驱动直接read_until_ready的真实TimeoutError内部采集owned-server状态，记录帧长度/发送返回与完成等待，区分批量写入、undo/snapshot、I/O和协议阻塞。不打印大payload、不重试；后面的slow-reader COPY TO尚未执行，不能归因于该分支 |
| materialized_view_dml_target_protocol_e2e_test.py | line140 `SAVEPOINT mv_case;` 等待响应超时，上一条sequence sentinel已返回69 | 保留完整目标类型/副作用/保存点矩阵；记录SAVEPOINT建立恢复图像、锁及同步调用链，确证后独立修复。不把先前正确的MV拒绝和sentinel当成全71项通过 |
| check_add_validation_protocol_e2e_test.py | line82多动作ALTER等待超时，原期望23514；trace进入line36的extended读取分支 | 由该分支及固定循环可恢复SQL为 `ALTER TABLE check_add_ext ADD COLUMN unpublished INT, ADD CONSTRAINT check_add_ext_upper CHECK(value < 0)`；这是源码/trace推导，不是原日志直接打印的SQL。观测原完整extended路径，保留23514、未发布列42703、行/NULL/事务断言，不放宽为任意错误 |

五个协议超时尚未修复，也未证明共享根因或由R83/R84/R85引入。
不以其他入口通过、原样复验通过或有插桩诊断通过抹去本轮失败。
若定位进入[用户暂缓范围](deferred-scope.md)，保留证据及未完成状态，不自行重开。

## 修复项与历史失败的本轮结果

[R83](issue-review-r83-float8-scalar-signatures.md)原生906/237及新951协议、
[R84](issue-review-r84-integral-rounding-result-types.md)原生310及新266协议、
[R85](issue-review-r85-numeric-sign-null.md)原生25均实际通过。
这六个新增入口相对于[上一轮1362组合](issue-full-registered-d974c80d.md)
全部保留；没有删除旧入口。

上一轮失败的prepared_dml_plan_test、JOIN完整协议、bound DML完整37项矩阵
本轮均通过，R84定向批次失败的stored_function_order_metadata_test及COPY
error/lock-release也通过。它们不是新的产品修复证明，原失败继续保留。

## 后续执行顺序

1. 先保存本轮全量结果、独立校验和逐项失败方法，单独本地文档commit。
2. 原完整COPY用外部观察器单次诊断，保持原SQL/帧/断言/期限；插桩不算验收。
   若未再现超时，只记录观测，不把R82关闭。
3. 对普通、有限的新问题先运行冻结旧实现基线，再逐根因修复/回归/commit：
   ARRAY_POSITION空数组与NULL起点；MOD整数宽度/numeric NULL结果域；
   ROUND单参数结果声明；并行布尔聚合表达式及错误传播。
   这些后续候选目前只有源码审查/PG参考及未执行草案，不是Root验收。
4. ARRAY_POSITION数值语义相等、非空NULL-needle快捷路径和UNKNOWN输入声明，
   以及嵌套函数metadata重复推断的复杂度，分别保留后续验证，不冒称已修复。
   后者尚无Root性能测量，不能把源码递归分析当成实测耗时。
5. 产品变更后重新匹配构建、定向门禁及新的完整注册组合；不修改本页冻结结果。

原273项仍为22 complete、166 partial、70 unverified、15 deferred_by_user，
R62广义函数/UDF范围仍partial。总目标没有全部完成。修复逐项保留本地commit，
不push；GitHub Actions工作流继续禁用，README保留长期项目入口。

## 终态后COPY原期限的单次诊断：捕获到失败现场

在修改产品之前，同一生产binary下单次运行原完整COPY，外部观察器复用了
只记录慢fsync的preload，并在唯一client的直接read_until_ready/read_message
真实TimeoutError内部取栈。原SQL、帧字节、断言、15秒socket期限及清理不变，
没有重试；无其他Root构建/测试并行。诊断会扰动时序，不计为无插桩验收。

证据仍在 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r82-copy-stream-frozen27-1-client.log`、同名前缀server.log，以及
`r82-copy-stream-frozen27-1-symbolized-156e45e9.txt`。driver实际exit1，
仍在原line262超时；fixture子进程随后正常清理exit0不等于COPY成功。
实际CopyData body为4195840字节、wire frame为4195845字节，CopyDone为5字节；
原sendall均返回后才进入等待，首个响应字节实际等待15.014426076秒后超时。
这不证明服务器已经消费完CopyData或执行到CopyDone提交。

仅对该次Popen自有临时服务器，在核对PID/start ticks/exe/cwd/两个preload
及READY标记后发送取栈信号；四个线程均收到信号。先取wchan，再顺序取栈。
COPY线程的实际符号链为 `receiveCopyIn insertRecord -> insertRow ->
insertInternal -> maybeAutoAnalyze -> analyzeTable -> sort -> compareValues ->
文本collation -> strcoll`；该线程预采wchan为0。binary无精确debug行号，
报告只给真实函数符号；源码行位置来自另行源码读取，不伪造PC到行的精确映射。

源码确认普通插入在修改计数达到默认50时，同步调用全表ANALYZE；统计排序
逐对含两次self-compare及一次小于比较，长TEXT会实际detoast并进入locale比较。
这是具体的同步维护/CPU扫描放大候选，不证明一次strcoll持续15秒、比较死锁，
也不证明该链解释其他四处协议超时或本轮原始无插桩运行的全部耗时。

在首个真实等待窗口内，COPY线程有18个至少50ms的成功fsync：9个toastmeta
临时文件合计约2.867秒，9个info目录合计约2.748秒，合计约5.615秒。
另一线程的14个heap/WAL成功同步与其有重叠，不能跨线程相加当墙钟耗时。
所有32个调用均返回0；残留errno不是I/O失败。更快的同步没有被记录，
这些调用的合计也不是完整查询剖析。

后续分别验证自动统计的重复比较/全表物化开销与长值元数据发布成本，保持
类型、NULL、locale排序和恢复/持久化语义；不能删fsync、减少样本或提高期限
来制造通过。观察到已有后台线程不意味着重开用户暂缓的后台checkpoint专项。
本次只定位，未合入产品修复；R82及原六项FAILED保持未解决。
