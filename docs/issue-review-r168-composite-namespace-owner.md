# R168：复合类型sidecar的namespace身份（归属修复与旧格式限制）

## R189：级联删除的正向夹具必须提供真实 composite owner

冻结 `2442e4c7` 原全套的 `drop_cascade_test` 实际因未捕获的 XX001 退出 134：
其正向场景用原生精确键 API 创建 `app_aux.coordinate`，却未填 schema/localName，
因此产生的正是 R168 不允许猜测归属的歧义旧记录，而不是已知属于 app_aux 的对象。
将异常改成成功或依点号猜 owner 会破坏原数据保留规则，不能这样修复。

正向夹具现在显式提供 `app_aux`/`coordinate`，并回读验证该身份；原有 13 组场景和
全部删除、邻接 schema、同前缀 quoted routine、事务/文件/对象存活断言保留。
另加独立旧记录负向场景：有意省略 owner，必须准确抛 XX001，两个 schema、完整
`.types` 字节与原记录字段均保留，事务和备份不泄漏。没有删除旧歧义行为的覆盖。

两套库均实际完成原 13 组与新增负向场景，分别达到新增 XX001 存活性 footer 和
`[DROP-CASCADE] all passed`，退出 0。每侧新编译/链接该测试，核对各自 57 个生产库
对象/stub、127 头与编译配置；生产代码/头/注册入口未变，工作区原未接受改动不借此接受。
首版新增负向场景因缺少本地文件读取 helper 编译失败，源和日志保留；V2 补齐读取，
未改 SQL、错误码、存活性或原检查。原全套也未换输入或改写其失败。

证据：`/tmp/dbms-r189-drop-cascade-v2.bkmendrs/completed-focused-receipt.json`，
SHA256 `872fde297bab06daf9e9c30a22b2adb93f68a7246f072d26a6dcdb55bd013362`。
原失败仍在 `/tmp/dbms-accepted2442-full1492.9obmw81r/canonical-full-registered.log`。
这不是修复后新全套、PG 差分、TLS/sanitizer 或原 273 项闭合证明；R187 不明退出仍开放，
R185 继续跳过，不 push、不启用 Actions。以下 R188 与 R168 记录按各自当时输入解释。

## R188：旧类型夹具必须接收精确的 checked-read 错误

冻结 `2442e4c7` 的原规范全套中，`create_type_shell_test` 在故意将 `.types`
替换成目录后，因未捕获的 `DbError(58030)` 实际退出 134，未到完整 footer。
这是旧夹具仅接收 bool 错误的契约失配：本项原实现已明确要求 checked 枚举
区分 I/O 与不存在，不能为让旧测试通过而恢复空列表或放行删除。

夹具现在要求该 DROP 精确抛出 58030，保留原 shell 存活性检查，并增加完整
shell/UDT 邻接文件字节不变、原目录仍在、事务与备份不泄漏的检查。
原 9 组测试、所有类型创建/删除、失败提交回滚、故障注入和持久化断言均保留。
accepted-only 与工作区两套库的完整原生夹具均实际退出 0，达到 `[CTYPE] all passed`；
两侧分别新编译/链接该测试，核对各自 57 个生产库对象/stub、127 头及编译配置。
工作区含原有未接受改动，不借此接受这些改动；生产代码和共享头未改。

证据：`/tmp/dbms-r188-type-shell.zaddkr3f/completed-focused-receipt.json`，
SHA256 `4129daefde4479282b075cda0730499989d5470acc29382d1e415f4f92586367`。
原失败保留在 `/tmp/dbms-accepted2442-full1492.9obmw81r/canonical-full-registered.log`。
原全套继续使用冻结输入，未删除或重写原失败；这不是修复后新全套、PG 差分、
TLS/sanitizer 或原 273 项完成证明。R187 的不明退出仍开放，R185 仍跳过；不 push，
Actions 保持禁用。下列内容是原 R168 的历史说明与证据。

状态：版本化复合类型归属修复已通过两端组合验收；旧歧义记录与物理键限制保留。

## 实测与原因

[R164](issue-review-r164-routine-custom-type-declarations.md)完整956项夹具创建了普通
enum/domain/composite，包括quoted点号及双引号类型名。其三个合法DROP SCHEMA CASCADE
在组合Root均报XX001、invalid auxiliary namespace identity；PG同一956项全0。
清理前的声明/归属控制通过，不应删掉清理SQL来宣称整批通过。

[辅助对象收集器](../src/commands/DdlExecutor.cpp)逐项把composite物理名重解析成SQL。
但[现有.types格式](../src/commands/TableManage.cpp)只保存decoded的
schema+'.'+name及字段；public不写schema前缀，也没有composite pg_type登记。
`R164.Scope.Mixed.C`或含裸双引号的物理字节不能重新当作SQL解析。
无关的一个引用复合名因此能阻止删除其他schema。

还存在需要专项实测的静态歧义：public的`"s.t"`与schema s下的t会生成相同物理名s.t。
仅凭该字符串无法证明namespace；最长前缀或“有schema就归给它”可能选错对象。
这里不把尚未运行的存活性控制说成已实测的数据误删。

## 安全修改方法与验证要求

必须由真实CREATE TYPE声明保存明确、decoded的schema/type-name，
在同一.types文件加入可区分旧记录的版本化表示，并由现有原子写及DDL快照管理。
现有物理API可以继续精确键语义；SQL声明/删除需要核对真实owner，不能造pg_type行。
CREATE、ALTER字段、rename、DROP、冷读及事务恢复均须保存并验证这对身份。
checked loader必须区分不存在、损坏与I/O，不能把坏owner当成空列表后删namespace。

旧记录没有足够信息时不能自动猜测迁移。无点号旧物理名可确认public；
dotted旧记录涉及破坏性操作时应明确失败，等待有依据的身份恢复。
仅增加collector的字符串拆分无法修复此问题。

待验证两种独立历史的public dotted survivor/真实namespaced deletion、quoted大小写、
冷重启、rename、field alter、回滚/保存点、坏V2及legacy歧义无修改。
不改变R51缓存锁，也不恢复用户暂缓的数组工厂或类型化常量。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-v6-root-results/`
下的custom_scalar_routine_declaration_protocol_e2e_test.log，三条DROP SCHEMA错误完整保留。

## 候选形成与历史验证

候选在真实CREATE TYPE中记录decoded schema/localName；同一.types文件以不会与合法旧名
混淆的`DBMS_COMPOSITE_V2:`前缀及hex字段保存该对身份，继续使用现有原子写入。
ALTER字段、改名及DDL快照保存这对身份；SQL DROP TYPE、schema收集器及R164声明入口
核对owner，旧的点号物理名在破坏性操作中明确失败，不猜测迁移或添加pg_type行。
原生物理get/drop API保留精确键语义；普通getter也保留旧的探测错误返回空值约定，
因为既有持锁调用者不能因本项新增throw而越过解锁。checked枚举明确报XX001/58030，
相关SQL破坏性路径先检查它，不把坏文件当作空列表。这不修改用户暂缓的R51锁流程。

固定207项native覆盖两种独立历史、冷读、字段及改名、回滚/保存点、坏V2与旧记录歧义；
固定332项协议夹具在PG18.6实际全0（`r168-v1-reference-protocol.log`），未减少原260项，
新增72项真实属性rename/type/drop/re-add。产品结果仍待全量构建后实际运行。

首轮Root fresh构建在第一个Main编译失败：误将全局sessionTempSchemaName写成dbms限定。
`r168-v1-root-normal-build.log`保留；仅移除该处错误限定后重新冻结V2，
没有在健康编译中修改输入或将失败日志写成通过。V1 clean投影也在编译前拒绝R167
过宽的函数hunk，因为它夹带既有NOTICE通道改动；现收窄为独立目标hunk，不接受无关修改。
两端新58个生产O2及六个真实Main TU的最终状态尚待检查，不复用旧共享头ABI对象。

同一332项在冻结旧Root二进制实际完成，199失败，
见`r167-r168-old-composite_namespace_owner_protocol_e2e_test.log`；原旧结果完整保存。
V2六个真实Main TU现已全部fresh编译成功，并核对accepted前端源码投影、Main及全部共享头；
两端各23个native TU也已编译成功。尚未将编译成功称为执行通过，
最终生产fresh58和每端52个完整入口仍待运行。

## V2/V3检查点

Root的genuine fresh58生产O2/repeat0、六个真实Main TU及输入收据实际通过；
本项207项native全0，原R164完整956协议也全0，尚不替代整轮52入口终态。
V2 clean在第38个CPP（DdlExecutor）失败：本项新分支误用未接受的`emitNotice`接口，
不能为让构建通过而接受原R98的整套通道改动。失败日志
`r168-v2-clean-normal-build.log`完整保留，原built clean永久不改。

新的V3 clean仅将该新分支改为既有NOTICE输出，并加入R167空目标Parse检查。
与V2的全部新共享头/Main及flags逐字节相同：36个本轮V2已真实fresh编译对象按源、
头、配置和原始对象收据复核，另22个CPP真实fresh O2，组成全部58个当前ABI对象。
没有使用R168共享头修改之前的生产对象，也不谎称V3单独重新fresh58。
V2首轮已成功37个CPP，其中parser因本次修正重新编译；失败的DDL没有对象可复用。
V3新链接当前57库及23个已复核native TU；三个真实V2 clean Main TU在Main/全部头/
accepted输入/flags/原对象核对后新链接当前57，不冒称V3 fresh TU。V3尚未运行52入口。
V3首次包装器因跨目录签名检查未重置source root而在首个复用前退出1；
修正包装器后的实际22/36构建通过，两个日志均保留，没有改写第一次失败。

## 最终候选的完整运行检查点

上述V2/V3为历史检查点。R169 V4工作区完整58入口实际全部通过；
本项native207、协议332及原R164完整956均0失败，字段ALTER/rename/drop/re-add等原控制均保留。
真实CREATE保存decoded owner/localName，版本化hex记录与字段在同一原子.types文件；
SQL破坏性路径使用checked枚举，坏记录XX001、I/O58030，不把错误当不存在。
旧无点号记录可证明为public；dotted旧记录拒绝猜迁移，物理键冲突也不宣称全面支持。
原生精确键及旧探测getter的不抛异常约定保留，不改用户暂缓的R51持锁流程。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合fresh58/当前ABI、真实Main来源与完整日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)。
