# R88：ANALYZE 排序重复验证及相同长文本的比较开销

状态：重复比较的本范围已修复、定向验收通过；COPY 超时仍未解决。

## 问题与修改方法

[原完整 COPY 的失败现场](issue-full-registered-27bac6ee.md)捕获到了同步
`maybeAutoAnalyze -> analyzeTable -> sort -> compareValues -> strcoll`。
旧排序每次比较两个值，都先分别比较它们自身以判断是否可排序，再比较大小。
相同长文本也走这三次比较。该调用链是实测现场，但不能解释全部超时耗时。

修复只改变单列、单次 ANALYZE 内的排序准备与执行：每个输入先验证一次
可比较性，相邻字节完全相同的值复用前一个验证结果；排序序号，不移动验证
对象的身份；保留“可比较值在前、不可比较值按字节排在后”的分区规则。
两个已经验证的值若字节相同直接判定不小于，否则仍用原类型/locale 比较。
验证在字节相等快捷路径之前，不以相同文本绕过 locale 或类型检查。

最终序号置换按环原位应用，只额外保存 N 个 size_t、N 个可比较性位及一个
临时 string。长文本不复制，也不增加 N 个 string 对象。首版保存 string
对象的中间实现未提交。零值/单值不新增比较，保持旧实现行为。

没有改变自动统计默认开关/阈值、全量扫描与 NULL 提取、histogram/MCV 格式、
类型比较、TOAST 分配或任何持久化同步屏障；没有减少样本或提高协议期限。
仍有 O(N) 辅助元数据与全表物化成本，不宣称统计算法整体已经最优。

## 旧基线与回归方法

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 运行 | 实际结果 |
| --- | --- |
| 匹配 7f9b70ed 的旧实现，r88-old-cost.log | reference=1、analyzed=243、available=1、semantics=1，实际退出 1 |
| 首版对象排序，r88-first-cost.log | reference=1、analyzed=1、available=1、semantics=1，退出 0；不是最终实现验收 |
| 最终序号排序，r88-final-native.log | 新比较次数测试 reference=1、analyzed=1、available=1、semantics=1；置换测试 96 个列/运行控制通过；十四个完整 native 全部退出 0 |

永久 `analyze_duplicate_text_cost_test.cpp` 使用真实引擎插入 32 个各 16KiB 的
相同 TEXT 和 4 个真正 NULL，断言行数、NULL 数、min/max、cardinality、全部
MCV 与十个 histogram 桶。fixture 仅在自身进程暂时关闭自动统计以隔离手工
ANALYZE，RAII 恢复配置；不是产品配置调整。

glibc 下测试转发真实 `__strcoll_l` 并仅在线程局部计数测量区间内的调用，
不替换比较结果。一个直接 locale 比较作阳性计数校验。当前环境实际可用；
其他 libc/locale 不可用时明确输出 COUNTER_UNAVAILABLE，仍检查所有统计值，
但不能据此声称操作次数门禁实际运行。该测试不使用墙钟耗时阈值。

永久 `analyze_sort_permutation_test.cpp` 覆盖 0/1/2/3/17/20/33/65 行、六列、
每表两次 ANALYZE：超出 double 精度的 bigint、精确 NUMERIC、NaN/Infinity/
小数、普通与 nocase 文本、空串、字面 NULL、真正 NULL、全 NULL 列。
从实际存储及 NULL 位读取输入，以旧比较器和原 histogram/MCV 规则计算独立
期望值，逐项比对全部统计字段，并确认存储行没有改变。96 是列/运行控制数，
不是 SQL 查询数。旧语义 oracle 没有复用新索引置换算法。

## 最终构建与完整邻居

`r88-final-build.log` 标准构建实际重编 TableManage.cpp 并链接成功，重复构建
无需重编。公共头未改，其余 57 份生产对象保留匹配，不冒称全 58 份重新编译。
最终生产 binary SHA256：
`26588797c4f249354d75bcd99d53ef9f17a259aa9000900b946eeac04908ee1b`。

`r88-final-native.log` 十四个完整 native 全部通过，包括两个新增测试、原类型/
NULL/MCV/histogram、统计对象、统计 planner/runtime、pg_stats、sql_stats、
ALTER STATISTICS、collation 和 TOAST；fresh stubs/driver 链接当前 57 份生产
库，隔离执行，全部 58 份 receipt 与输入签名一致。

`r88-copy-unmodified/copy_protocol_e2e_test.log`：原完整 COPY 单次无插桩验收
实际退出 1，仍在 line262 等待 COPY 256 完成时超时；后面的慢读 COPY TO 未
到达。无其他 Root 编译/测试并行，未使用 preload/信号取栈，未修改原 SQL、
断言、15 秒期限或后续慢读检查。此结果确认比较次数修复不足以关闭 R82；
不重试到绿，不将这次超时称为全统计测试失败或已证明的 TOAST 单一根因。

`r88-final-protocols.log` 六个完整协议全部通过：typed statistics、NULL
statistics、typed MCV、pg_stats histogram extent、native ANALYZE relation
list 和 legacy forced ANALYZE；生产输入/binary/全部 58 receipt 首尾一致。
COPY error/lock-release 完整邻居也实际退出 0，独立运行另记在
r88-copy-error-neighbor.log，首尾输入/binary/全部 58 receipt 一致。
独立终态 r88-terminal-fence.log 再核对全部 58 receipt、cache/build stamp、
上述 binary 哈希及重复构建，实际退出 0。

提交前全 511 份 Markdown/877 个本地内联目标、523 Python AST、13 shell
语法、503 unique 注册/485 standalone E2E、文档状态/兼容契约/版本及六个
总账单测均通过。`r88-require-complete-actual-exit.log` 实际退出 1，总账仍未
完成。文档检查不证明全部历史技术结论在当前版本已经验收。

## 尚未完成

本项只修复已测量的重复比较；TOAST 元数据同步、全表 detoast/物化、其他
协议超时仍需分别定位。旧六项完整失败不由比较次数门禁改写为通过。
OPT-04/OPT-05 与原 273 总账状态保持不变，用户暂缓专项不重开。
新注册组合为 869 native +3 真实 Main frontend +503 Python =1375 入口，
尚未全量运行，不称总目标完成；逐项本地提交，不 push，
GitHub Actions 继续禁用。
