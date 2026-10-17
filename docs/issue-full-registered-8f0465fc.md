# 干净提交8f0465fc的完整注册回归

状态：完整运行已结束，实际退出1；不是全绿，也不计原273总账全部完成。
1431个不同入口按预期顺序各执行一次：1427实际通过、3失败、1个TLS环境skip。
全部冻结输入、程序、双缓存、58 receipt及57份测试库字节的独立终态核对通过。

## 冻结范围

目标提交`8f0465fc144d961020315bea346cc6fcad61dc56`，包含独立提交的
[R126](issue-review-r126-time-precision-grammar.md)、
[R127](issue-review-r127-time-excess-precision.md)及
[R128](issue-review-r128-time-binary-parameter.md)。各项有限验收及提交前后检查
仍只证明其对应范围，不用工作区组合替代本轮干净提交证明。

临时干净工作树：`/tmp/dbms-clean-r128.WKnzNc54`；证据目录：
`/tmp/dbms-full-r128.OOGk9Ifa`。所有已跟踪输入初始封印为
`0f2989ea3d0faa8eccd013b860d06c4efdeaac4284bd79e192ec894bff44311d`。
运行时间为2026-10-17 06:02:01至08:49:29 UTC；实际终态见`driver.log`。

主工作区原有未提交候选不复制进该工作树，也不覆盖或删除；其三个原生、
三个注册Python入口及另一个未注册draft不属于本轮1431。用户已要求跳过的
专项不新增实现或复审，Actions保持禁用，不push。

## 执行计划

1. 在干净提交上从零正常O2编译全部58个生产TU，再做重复构建；不复用主工作区
   含待确认头变更的对象或R124的真实Main测试对象。
2. 核对全部58 receipt，将57份匹配生产对象用于测试缓存，并按当前干净头fresh
   编译stubs；双缓存/来源/对象字节分别验证。
3. 标准`scripts/build_tests.sh`串行运行898原生、3真实Main、530注册Python，
   共1431个不同完整入口。链接引擎的原生程序经初始化前隔离器运行。
   真实Main中explain/session测试TU为O0、window及生产代码为O2，不链接stubs。
4. 运行期间冻结该干净工作树全部已跟踪输入、产物及缓存；保留每个失败，
   不把未执行、编译失败、缺失入口或环境skip算作实际通过。
5. 终态独立核对1431个ID的完整性、唯一性、顺序及实际状态，重新核对输入、
   二进制、双缓存、58 receipt和57份测试库字节。TLS若环境skip须单列，
   不冒真实TLS验证；没有整库sanitizer证明。
6. 将确认的新问题登记为期望/实际/影响/方法/永久回归/commit，再逐项修复和
   独立本地提交。普通未完问题不改记为用户暂缓。

## 实际终态

正常构建实际重编全部58个生产TU、实际0；重复构建0个CPP、实际0。
57份生产对象逐字节复制进测试缓存、当前干净头下fresh stubs，以及全部58
receipt/双缓存/57字节的运行前复核均实际0。标准1431入口已全部结束，runner
实际退出1；不能把构建通过、1428个PASSED退出或运行完整等同全部入口通过。

本轮干净版本程序SHA256
`1e305110a1e6b9e3bd26ad8e93ca06b037a077c03265721682d681515976298a`，
缓存`7431fd52eab34118977817aad554510009ba071e555368a525c3dea8e9bcbf2c`，
干净头/生产flags签名
`0b2ff0368600ebc25996cd26a28ebf1309a5087ac3ed0cb1947e070c5b901dfb`。
它不同于R128有限验收的工作区组合产物/739签名，正因不包含原待确认头/源码，
不能复用主工作区库或旧真实Main TU对象冒充本轮干净重建。

| 阶段 | 不同完整入口 | 实际通过 | 环境skip | 失败 |
| --- | ---: | ---: | ---: | ---: |
| 原生 | 898 | 895 | 0 | 3 |
| 真实Main前端 | 3 | 3 | 0 | 0 |
| 注册Python | 530 | 529 | 1 | 0 |
| 合计 | 1431 | 1427 | 1 | 3 |

三个真实Main在干净头/当前57库下分别fresh编译、新链接并实际隔离执行通过：
explain/session测试TU O0、window与生产TU O2，不链接stubs。这是本轮实际
新编译，不复用R124旧头测试对象。R125–R128各自永久协议入口亦实际通过。

Python阶段已实际观察到`statement_timeout_tls_protocol_e2e_test.py`打印
`skipped (binary has no OpenSSL support)`，其脚本退出0但属于环境skip。
终态已从1428个pass-exit计数中扣除，不能据此声称真实TLS已验证。

独立检查确认1431个ID完整、唯一且顺序一致，没有compile/link失败或缺失入口。
正常58个TU均真正重编，repeat为0个；运行前后所有tracked输入、HEAD/status、
binary、双缓存、58个source/header/flags receipt和预先记录的57份库对象SHA256
均一致。独立核对输出的输入/产物一致性标记实际0，但检查器最终实际1，因为
三项真实失败必须保留。标准runner、汇总器及外层driver也都实际1。

完整日志SHA256为
`fcb2f423c8c2c18542bbdbbf7b1da134b206326019e3f15507e9719c6339e241`。
`independent-terminal-audit.log`保存独立结果，不能把其中的正常产物核对标记
摘出来称为行为全绿。没有整库sanitizer或真实TLS验收证明。

### 三项实际失败与逐项修改计划

| 记录 | 入口及实际失败点 | 已确认与修改方法 | 范围/状态 |
| --- | --- | --- | --- |
| F8F-01 | analyze_null_statistics_test，测试第87行dropDatabase(db) != OK，进程abort，完整入口FAILED | 所有先前NULL统计/分区断言已执行通过；当前未打印具体DBStatus，不能猜原因或说成统计值错误。先保留日志；若以后恢复DROP DB专项，再记录状态码及目录/缓存生命周期，修复所有者路径并回归，不能删除清理断言换通过 | 落在已暂缓R51的DROP DB路径，本轮只登记，不重开/不修复，不计通过 |
| F8F-02 | array_inspection_projection_test，array_position抛SQLSTATE42883，未捕获导致abort | 该fixture把序列化数组存入普通TEXT列；先确认内部projection API与SQL类型契约，并用实际PG对照TEXT/真实array声明，再优先改为真实array fixture或修复已承诺的API行为。保留NULL/范围/多维控制，不放宽SQL签名来换通过 | 普通待复现/修复；是单数ARRAY_POSITION，不是已跳过的R109 ARRAY_POSITIONS，不混入其候选 |
| F8F-03 | pg_typeof_test第37行，pg_typeof(tick())的objectOid==21且effects==1组合断言失败 | tick的AST显式声明smallint，callback返回typed NULL；绑定和类型查询前的零副作用断言已过。先分别打印OID/effects并追踪绑定前后resolvedResultType，比较native callback和stored声明所有权；保留类型分析不执行、执行只调用一次的契约，不靠改期望OID掩盖问题 | 普通待复现/修复；组合断言未区分类型错误或调用次数，尚不猜根因 |

`full-regression.log`中该轮完整输出包含上述断言及FAILED标记；标准runner
继续并完成全部后续入口。记录失败位置不等于已证明与R51历史问题同一根因。
`normal-build.log`、`repeat-build.log`、`cache-preparation.log`、
`pre-full-fence.log`、`full-regression.log`、`driver-summary.log`及
`post-full-fence.log`均已实际产生；后者及独立输入/产物核对实际0。

接下来先对F8F-02做真实PG/内部API对照并独立提交夹具修正，再分别复现
F8F-03的OID和执行次数、修复真实声明所有权、添加永久回归并独立commit。
后续定向通过不会倒改本轮三项FAILED，也不等于重新跑过整套1431。

原273项目及勾选仍为22complete/166partial/70unverified/15deferred；
完整注册回归即使全过，也不自动补齐未实现功能、所有PG边界或暂缓专项。
