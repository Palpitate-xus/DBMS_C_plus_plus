# R169：Simple Query 多语句消息的语法检查时机（有限修复）

状态：有限原始语法修复已通过Root及accepted-only clean两端完整58入口组合验收。
最终native106、wire104两端均0；PG完整104/0，冻结V3 clean native106/3、wire104/2失败。
原旧Root102/27及V3 clean整轮58/1失败均保留，下文各轮按原输入读取，不冒充完整raw grammar实现。

## 已复现的问题

R167 的完整 663 项协议回归在 PG18.6 全0；Root V2 有6项失败，
其中两项是 `DROP DOMAIN schema.guard; garbage` 及其 CASCADE 变体。
PG 直接返回 ErrorResponse(42601)、ReadyForQuery，Root 却先发出
DROP DOMAIN 的 CommandComplete，再报告语法错误。后续域存活检查通过：
既有隐式批事务已回滚 DDL，不能把本例描述为持久数据丢失。

新增完整102项对照另确认：以单独字符串、dollar字符串或右括号作尾片段时，
旧拆分器会丢弃非法片段，产品仅返回DROP的C/Z；随后再次DROP返回42704，
说明这些私有UUID域已实际删除。PG同一输入先报42601，随后DROP成功。
这与原garbage两例的回滚不同，不能把存活性结论跨用到新增输入。
数字尾片段还实际触发XX000 substr越界；nextval控制观察到前缀执行导致后续值为2而非1。

证据为 `/tmp/dbms-review-r46.ifu6SWLp/r168-v2-root-results/domain_drop_target_protocol_e2e_test.log`。
完整 Root52 终态1，50入口通过、2入口失败；原native189的空quoted目标错误另属R167，
本项不通过删除或放宽原663控制来“通过”。

## 根因和修改方法

[NetworkServer](../src/network/NetworkServer.cpp)先检查整条消息的词法错误，
随后拆分并逐句执行，没有在执行首句前识别后续已能确认的原始语法错误。
拆分器还用首个SQL关键字是否为空筛除片段，可能把单独数字、字符串等非空片段当成空语句。

新增parser的error-only语法证明接口，在Q消息开始执行、COPY处理和隐式BEGIN之前扫描片段。
范围仅词法错误、不可能的命令头、已审计的严格DROP DOMAIN语法；
其他已知SQL和项目命令头不解析，保持逐句分析、对象查找、错误与执行时机。
语法失败沿用现有Q词法错误的I/T/E和回滚边界，不另造事务所有权。
拆分器用token是否为空区分注释/空语句与非空非法片段。

这不是完整PostgreSQL raw grammar实现；尚不能证明的语法继续由既有逐句路径处理。
不能依据错误码42601一律提前报错：CREATE DOMAIN的NULL/NOT NULL冲突、UNIQUE等
在PG中由执行阶段拒绝，前面合法语句的结果必须保留。
V1的广泛42601方案因这一静态反例被否决，原草稿冻结保留；V2收窄后又发现
维护命令头可能被误拦，V3补全backup、restore、cancel、dump、clear、pg_switch_wal、switch。
这些维护命令仅做native解析不拒绝检查，没有执行备份、恢复或其他维护操作。

## V3验证计划与边界

V3固定native104、wire102；保留原两个失败批次，覆盖非法片段、词法错误、
nextval前缀不执行、CREATE后引用、函数body/注释/引用分隔、空消息、
先前CommandComplete必须保留的分析/执行错误、显式事务I/T/E及恢复。
DataRow长度、列描述、错误码、完整消息序列和清理均检查；UUID对象与私有数据目录隔离。
原V1/V2、V3草稿和收据分别保留，不把草稿数量当作测试通过。

最终夹具路径为 `tests/simple_query_raw_syntax_test.cpp` 和
`tests/simple_query_raw_syntax_protocol_e2e_test.py`；V3当时在Root候选注册，最终V4验收见末节。
V3草稿 `/tmp/dbms-r169-rawsyntax-v3.1E9Lczc9`；
新增parser头接口后，两端全部58生产CPP及三个真实Main TU需重新编译并核对，不能复用旧头对象。
R167空目标修正是独立依赖；完整原R167189/663、R164570/956、R168207/332等控制不能减少。
PG实际完整102/0，冻结旧Root完整102/27失败（含后继控制受旧错误状态影响，不等于27个独立根因）。
参考由共享runner核对server_version_num=180006；旧binary SHA256为
`a7867ec6e967cf30d84b19194aa82b45fb9fbd267f744509a9ce91721555bba2`。
日志`/tmp/dbms-review-r46.ifu6SWLp/r169-v3-{reference,old-root}-protocol.log`；
原V1/V2未运行，没有伪造旧94/96结果；该发现阶段未验收新产品104/102，后续实际结果见末节。

不涉及用户暂缓的R156、R151、R51或安全专项；原273未完成，不push，Actions保持禁用。

## V3实际运行检查点与V4纠正

V3两端均已实际完成genuine fresh58生产O2、repeat0、26native及三个真实Main和stub的新TU，
29个当前57库链接均0；每端61条记录命令全0。只有编译证据，不等于运行通过。
干净版本开始完整58入口：26项native有25入口通过，raw语法固定104有1失败，
控制9的无目标`DROP DOMAIN`未被证明为原始语法错误。其余原生控制不删除或放宽。
协议固定102实际全0；原R167的189/663、R168的207/332均实际全0。
该干净版本整轮58入口已结束，实际57通过、1失败、退出1；全部29协议及三个真实Main入口均0。

根因是通用classify依赖目标token，只有两个关键字时落入DropTable兜底而非DropDomain；
V3在进入严格DROP DOMAIN parser之前据此提前defer，遗漏了最简单的缺失目标。
V4只改这个Parser CPP的入口：按前两个未引用token识别drop/domain，
然后由原有严格parser返回缺失目标错误；不解析其他已知命令，不改header、Main或事务分支。

新V4固定native106、wire104：完整保留V3全部104/102，增加小写/注释前缀的无目标native两项，
以及`SELECT nextval(...); DROP DOMAIN`在执行前报错、currval不变的协议两项。
反向移除这四个新增控制、数量及说明文字变化后，夹具与V3逐字节相同；
原V3夹具和完整失败记录保留，不通过改变控制9的预期“通过”。
V4完整104项已在PG18.6串行执行，全0、实际退出0；相同夹具在冻结V3干净产品上
完整104项2失败、实际退出1。其旧实现native完整106项3失败、实际退出1。
新增失败控制证明无目标DROP DOMAIN仍会让nextval前缀执行，不能合并旧102参考冒称104通过。
首批对照因驱动没有等待参考端结束而重叠，不作串行证据；随后严格串行重跑。
串行首轮旧协议在初始CREATE SCHEMA超时，没有完成footer，不计为完整104；
保留原15秒超时、全部断言及私有UUID隔离重试后，才实际完成104/2。
未修改超时、预期或产品源码来掩盖该传输失败。

V3编译收据分别为`/tmp/dbms-r169-v3-root-receipts.INxdoAFe/final-receipt.json`和
`/tmp/dbms-r169-v3-clean-receipts.gVKTO4g4/final-receipt.json`，
干净版本运行日志`/tmp/dbms-review-r46.ifu6SWLp/r169-v3-clean-runtime.log`。
V4草稿`/tmp/dbms-r169-rawsyntax-v4.JH3RXJuE`冻结不再修改；
V4两端各实际fresh1 Parser O2，逐一核对后复用57个V3当前ABI生产对象；
Root另实测正常构建repeat0，clean采用显式编译/链接证明，没有声称clean执行了repeat构建。
26个native与stub重新编译，三个真实V3 Main TU核对Main/全头/原始输入/flags/编译器及字节后复用，
重新链接当前57库，不使用Main stub。Root58条、clean59条编译/链接记录命令全部0；
clean额外一条是旧V3对象与新106夹具的反证链接，不属于当前30个二进制。
清楚区分V3真实fresh58/六Main与V4CPP-only fresh1及复用，不冒称V4重新fresh全部。

串行对照日志为`r169-v4-reference-protocol-serial.log`、
`r169-v4-old-v3-clean-native-serial.log`、`r169-v4-old-v3-clean-protocol-serial-retry1.log`；
首批重叠说明、首次超时及退出2的串行包装器日志另保留，没有改写为产品SQL失败或通过。
V4 Root与干净版本完整58入口现已分别实际完成，源码/对象/二进制清单前后核对；
下述验收来自新产品完整运行，不以编译成功或旧V3其余57入口通过替代。

## V4两端最终组合验收

工作区完整58入口已实际退出0：26个native、29个协议、三个真实Main全部通过，
每个固定数量footer均核对；源码、58生产对象、server、88对象与30二进制清单前后均一致。
日志为`/tmp/dbms-review-r46.ifu6SWLp/r169-v4-root-runtime.log`，
完整结果目录为同前缀的`r169-v4-root-results/`。
独立干净版本同样58入口实际退出0，26native、29协议和三个真实Main均0，全部前后清单一致；
日志为同目录的`r169-v4-clean-runtime.log`，结果为`r169-v4-clean-results/`。
最终原始日志及外层实际退出记录在`r169-v4-final-two-sided-runtime-evidence.json`固定，
不以先前重叠/超时的包装器或部分footer作为通过依据。

同一组合中的九项修复及专项数量如下；它们是有限回归控制数，不是原273整行完成数。

| 问题 | 修改方法 | native / 协议控制 |
| --- | --- | --- |
| [R158 角度范围](issue-review-r158-angle-conversion-range.md) | 单次binary64运算、严格输入及范围检查 | 603 / 1449 |
| [R161 限定投影身份](issue-review-r161-qualified-projection-routine-identity.md) | 将真实限定callee交给完整AST绑定 | — / 637 |
| [R162 通用存储签名](issue-review-r162-generic-stored-signatures.md) | 按声明选择、保存输入类型和转换所有权 | 343 / 1705 |
| [R163 Parse校验](issue-review-r163-qualified-parse-validation.md) | 在发布statement前纯绑定受支持形状 | — / 1017 |
| [R164 自定义类型声明](issue-review-r164-routine-custom-type-declarations.md) | 验证与持久化共用实际namespace身份 | 570 / 956 |
| [R166 参数OID](issue-review-r166-scalar-protocol-parameter-inference.md) | 从真实选中输入声明回填直接$n | — / 263 |
| [R167 域删除目标](issue-review-r167-domain-drop-targets.md) | 严格单目标语法及真实域身份 | 189 / 663 |
| [R168 复合类型归属](issue-review-r168-composite-namespace-owner.md) | 同一原子sidecar保存版本化owner/name | 207 / 332 |
| R169 多语句语法时机 | 执行首句前仅证明有限原始语法错误 | 106 / 104 |

原R165数组1852、R159编码676/806、typed NULL1000及相邻签名/namespace/domain控制均保留。
这些通过来自同时包含九项候选的组合，不宣称每个中间提交单独通过整套相互依赖矩阵。
干净版本以已接受`ad36c7ad`加九项精确owned hunks构建，不接受Root原有外来改动；
逐项反向还原九个生产路径与base完全相同，17个共享夹具与Root逐字节相同。

V4编译收据分别位于`/tmp/dbms-r169-v4-root-receipts.AGp0KtH3/`和
`/tmp/dbms-r169-v4-clean-receipts.Fw18UiYK/`；三个真实Main中的explain/session沿用原O0、
window为O2，生产及native为O2。使用已有无OpenSSL测试配置与SSL stub、zlib/ICU；
没有本轮整库1478入口运行、TLS或整库sanitizer通过声明。built clean及donor树永久冻结。

九项按各自owned hunks、夹具、注册和专属报告逐项本地提交；R164先于依赖其代码块的R168。
组合接受后的清单为589MD、916native+559已注册Python+三个Main=1478入口；
Root仍为592MD、922+565+3=1490，另12个原有入口及三份文档未接受，不能混入本轮提交。
原273仍22 complete /166 partial /70 unverified /15 deferred；全目标仍未完成。
R160常量规划、quoted未限定callee、通用参数/类型/依赖及完整raw grammar仍须继续核实修复；
用户已暂缓模块不恢复，不push，GitHub Actions保持禁用。
