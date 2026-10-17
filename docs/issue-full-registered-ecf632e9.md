# 干净提交ecf632e9的完整注册回归

状态：完整运行已结束，实际1436个不同入口各一次、顺序一致，无漏跑/重复。
1433实际通过、2失败、1TLS环境skip；canonical/summary/driver及独立终态检查器
均实际返回1，真实失败没有被输入核对0或skip掩盖。原273总账仍未完成。

## 冻结范围

目标提交`ecf632e9bfd756e9266317b009a29802d044eefb`，包含独立提交的
[R129](issue-review-r129-native-array-projection-types.md)、
[R130](issue-review-r130-native-result-hint-precedence.md)、
[R131](issue-review-r131-timestamptz-binary-timezone.md)、
[R132](issue-review-r132-timezone-set-to.md)及
[R133](issue-review-r133-timestamp-binary-microseconds.md)。

干净detached工作树`/tmp/dbms-clean-r133.BTRQCUGk`，证据目录
`/tmp/dbms-full-r133.stgQVl08`。开始时间为2026-10-17 10:23:26 UTC，
结束时间为同日13:02:22 UTC，期间没有改动冻结输入或重启整批换绿。
全部已跟踪输入初始封印为
`0ad4e0783c7e1a59a8306525143459c911ddaf45f48114eae7715fa0e6f5eacb`。
主工作区原有候选源码/三个MD及六个运行入口、未注册draft均不复制进来。
不删除或提交用户已跳过的R109及R98/R99未接受候选。

当前新工作树compile签名为
`8c8fe094583501824c98a09b12668b3db1c6c467c4debed304c7b5d4b0cdb5cd`。
该签名还包含绝对header路径，不能跨工作目录只比较签名就猜测ABI改变。
8f至ecf的已跟踪公开头未改变；旧8f工作树的0b2签名仍只对应它当时的路径/输入。
本轮独立测量签名/receipt，不能复用Root739、R132测试TU或旧clean目录的对象
冒充本轮fresh构建。此文在Root记录，不改变干净工作树的任何tracked文件。

## 执行计划

1. 在本目录从零正常O2编译全部58个生产TU，重复构建验证无CPP重编。
2. 按本轮签名验证58 receipt，把57份匹配生产对象用于测试cache；当前头下
   fresh编译stubs，验证双缓存及57份对象字节，不借未核对对象或假stamp。
3. 运行原始`scripts/build_tests.sh`，串行899native、3真实Main和534注册Python，
   共1436个不同完整入口；显式将DBMS_MAIN指向该干净目录程序。真实Main全部
   fresh TU编译，explain/session测试TU按既有O0，window及生产对象按O2；
   不链接stubs，实际运行均在全局初始化前配置好隔离目录。
4. 运行期间冻结干净工作树全部tracked输入及Root的src/tests/scripts，不修改
   运行driver；数据库批次不并行。Root仅可记录文档，所有原候选字节保持。
5. 保留每个失败，不以compile/link错误、缺失入口、未执行或环境skip当通过。
   TLS若打印未构建OpenSSL而skip，需从PASSED退出中扣除并单列，不冒真实TLS。
6. 独立核对1436个ID的顺序/唯一性/完整性，重新核对HEAD/status、全部tracked
   输入、程序/双缓存/58 receipt及预记录57份库字节。完整运行的真实失败仍
   使runner/最终检查器返回1，不把输入核对0摘出来当行为全绿。
7. 将新发现保存为期望/实际/影响/方法/永久回归/版本记录，普通问题逐项修复
   与本地commit。若落在已暂缓R51的DROP路径，仅登记，不重开或删除断言。

## 完整实际终态

干净工作树已核对HEAD、空git status及预期899native/534Python/58生产/57库。
正常O2已实际重编全部58个生产TU、实际0，repeat为0个CPP、实际0；57份匹配
生产对象、当前头下fresh stubs、全部58 receipt/双缓存/库字节及运行前fence
实际0。899个native均已结束（897过/2失败），3个真实Main均fresh测试TU编译、
新链接57匹配生产库、无stubs并实际隔离执行通过；534Python全部结束，其中
533实际通过/1TLS环境skip，没有Python失败。合计1436个完整入口、1433实际过/
2失败/1环境skip，1434个PASSED退出中已扣除1个实际skip，不冒1436全绿。

| 阶段 | 实际终态 |
| --- | --- |
| 原始scripts/build_tests.sh | 按预记录顺序执行899native+3Main+534Python，各ID一次，实际退出1 |
| driver-summary | 1436观察、1434 PASSED退出、2 FAILED；missing/extra/duplicate均空，实际退出1 |
| 运行后输入/产物fence | HEAD/status/全部tracked输入、binary、双cache、58 receipt、57库对象字节未变，实际0 |
| 独立终态检查器 | 独立重算58 receipt和57份预记录生产对象SHA，核对1436有序/唯一/完整，输入证据检查0；因保留两项真实失败，检查器最终实际1 |

本轮干净程序SHA256为
`72ee373b9e72f1e799fcc87386d4bc06dd8aaa05e209be1221852ea7567323be`，
cache为`bba6347dff4512d5c5fccd2ce4e37783e0cbe3cb4eee5e333df59f07d299e6ae`，
本目录compile签名仍8c8fe094。Root的302c程序/R132 TU不混入本轮证明。

`driver.log`、`normal-build.log`、`repeat-build.log`、`cache-preparation.log`、
`pre-full-fence.log`、`full-regression.log`、`driver-summary.log`、
`post-full-fence.log`及`independent-terminal.log`均已实际产生；full-regression的
SHA256为`b40536e9dbcb03ce7f386c6bdd519708ba027fc088cfd726a8418349180cb806`。
本节使用实际终态，不将此前PROGRESS_ONLY观察或预期数当完成证明。
Root运行中另做只读fence及alternate-index原候选保护检查，两者实际0，分别保存
`root-midrun-fence.log`、`root-midrun-preservation.log`；原56933字节候选源码差异
及三个暂缓draft字节未变。它们不是干净整批行为通过证明，也不代替终态核对。
statement_timeout_tls协议入口实际打印binary has no OpenSSL support并skip；
虽然runner将其退出0记为PASSED，上述实际通过数已扣除它，不冒真实TLS覆盖。

### 两项实际失败及修改方法

| 记录 | 入口及实际失败 | 已确认、未确认与修改方法 | 范围/状态 |
| --- | --- | --- | --- |
| FECF-01 | background_worker_test抛出未捕获DbError：cold domain catalog read conflicts with an active DDL owner，SQLSTATE55P03，进程abort，完整入口FAILED | 不能仅凭消息猜测前台/后台线程或哪一操作失败，也未证明与旧R51同根因。若用户恢复专项，再采集线程/栈、DDL owner与冷热目录状态，验证所有者传递、锁顺序及可重试冲突策略；不得删除数据断言、吞异常或简单放宽sleep换绿 | 属于已暂缓的后台worker/checkpoint入口，本轮只登记、不重开/不修复；不把普通domain家族全部改为用户暂缓 |
| FECF-02 | stored_function_engine_owner_test在原第126行assert(local.dropDatabase(db) == DBStatus::OK)失败，进程abort，完整入口FAILED | 该断言之前的原owner/私有写入/回滚/默认值/生成列/嵌套调用及另一engine的999结果断言均已经过；不能把清理失败说成这些行为失败，也不能猜测DBStatus、锁或与历史DROP的具体同根因。恢复专项后应保留原完整入口，采集实际状态/调用栈及清理前目录与所有者，再验证释放顺序，不删除DROP断言 | 落在原已暂缓R51 DROP DATABASE路径，仅登记；不把stored routine/独立engine普通范围整体列为暂缓 |

该轮`full-regression.log`第623–626行保存原异常及FAILED；标准runner继续后续
入口，没有终止、重启整批或把失败忽略为通过。此前8f的该入口曾通过，不据此
猜为负载/竞态或新改动所致；完整跑完后最终仍是本表两项失败。
FECF-02原断言与FAILED保存在同一log第15050–15052行；失败后原runner继续，
没有修改夹具或重跑换绿。该完整入口仍计失败，不能拆取先前通过断言算入口通过。

`analyze_null_statistics_test`本轮已原样PASSED（旧失败是其末尾DROP断言），
`array_inspection_projection_test`也已PASSED。前者不关闭未归因、用户已暂缓的
旧R51问题，后者才是R129干净组合的一个真实完整入口证明；不借此代替1436。
R130新增native_function_result_hint完整入口亦已PASSED、168控制零失败，原
pg_typeof入口也实际PASSED；这两个入口的旧声明/执行次数断言未放宽。
R129新增51协议、R131的158协议、R132的112协议及R133的266协议完整入口也
在本轮干净组合中全部实际PASSED。这里确认具体回归，不把普通其余功能族升为完成。

三真实Main产物SHA分别为：explain
`405a4492c7a9bc8c63d393bcc7a38029b13eb908c53f7694f9194938f11606ac`，window
`e26ca9c6fff51696166e3741914814051814b895575a25862d9043cdbba45620`，session
`f1e359db0f2539728634b1dc977e422a4df3eb11ea01281898f2fe997dbf1fea`。
没有拿Root739或R132旧测试TU冒本轮fresh编译。

## 冻结期间只读复查队列

这些是后续验证候选，不计为已复现bug、修复完成或额外测试通过；未启动第二个
数据库/参考批次，也未改变冻结源码。完整运行已结束，下一阶段逐项复现/归因/修改。

| 候选 | 当前只读证据与不确定性 | 后续验证/修改方法 |
| --- | --- | --- |
| 普通R62名字被原生回调替换的所有权 | 干净ecf的ownsScalarSignatures仅为数组查找/四个字符串长度名/固定float8检查实际marker，其余默认按名字拥有签名；abs/length/upper/lower的replacement是否继承不属于它的参数/结果仍待原生行为核实，不能借R130的固定float8证明全部名字。注册循环也未给这四个名字独立所有权marker，length不在现有四个字符串长度名集合中 | 隔离原生测试同名替换、typed NULL/显式提示/原始参数/arity与零分析副作用；保留真正builtin/stored签名和命名空间，确认契约后才扩真实callback marker，不按熟悉名字套类型 |
| 物理时间戳微秒持久化 | R133只改接收器；固定宽度extract路径仍有formatTimestampSeconds，需跟踪真实DDL列元数据、实际编码与所有读写路径，不能只看dsize或把某个辅助微秒函数当heap已经升级 | 完整运行后按PG做INSERT/UPDATE/RETURNING、冷热扫描、索引与数组的精确微秒对照；确认单位/格式版本后设计向后兼容codec，不静默把旧秒数据解释成微秒、不操作真实用户数据 |

这些普通未完范围没有改记用户暂缓，原273状态也不因此上调。
原生所有权候选已在证据临时目录准备1320预期控制的C++草案（不是实际执行数）；
尚未编译、运行或注册，Root及干净工作树冻结输入未改变，不提前认定新bug。

## 历史与未完成范围

[旧干净8f完整回归](issue-full-registered-8f0465fc.md)实际1431入口、1427实际通过/
3失败/1TLS环境skip。其F8F-02和F8F-03分别由R129/R130修复，本轮原完整入口
在干净接受组合中均通过；F8F-01的统计断言之前已通过，旧失败发生于DROP清理，
本轮原入口虽通过也不关闭R51或证明原因已消失，R51仍按用户原要求暂缓。

原273统计/勾选保持22complete/166partial/70unverified/15deferred；完整注册运行
结束不补齐旧整秒heap、其它未实现/未验证功能、全部PG边界或用户暂缓项。
没有整库sanitizer或真实TLS证明，不push，Actions保持禁用。

后续[R134](issue-review-r134-core-native-callback-ownership.md)已对冻结时的原生所有权
候选单独复现：同一1320夹具旧922失败、修正后零失败，正常1/repeat0及37完整
有限入口全0并独立提交。上面的“尚未执行草案”属于冻结时状态；新1437未全量，
本轮1436日志/两真实失败不改写，微秒heap持久化候选仍待单独行为核实。
