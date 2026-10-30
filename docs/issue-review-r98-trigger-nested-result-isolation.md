# R98：触发器子语句结果泄漏到外层协议响应

状态：前三版候选均未接受。第三版通过19个完整native、十一协议及三个真实Main，
但补充的严格17控制仍7失败；不能以先前有限门禁替代这些边界。当前已实现typed
诊断通道候选，组件12/12与ASan/UBSan通过，正式全58重编及整体验收进行中。
原始和候选失败分开保存，不提前登记产品通过或原273完成。

## 独立复现及纠正的假设

最初只读检查怀疑main中WHEN回调晚于server分支，可能使服务端忽略WHEN。
真实独立实例推翻该假设：StorageEngine构造器已有默认求值器；18种BEFORE/AFTER、
INSERT/UPDATE/DELETE、false/true/NEW或OLD数值条件的副作用计数均正确。因此
没有移动初始化或“修复”一个未复现的注册问题。

初版独立36检查中旧实现4失败、实际退出1，均是外层INSERT的command tag：
外层插入两行，WHEN为true或数值条件并实际触发嵌套INSERT后，却响应INSERT 0 1。
UPDATE/DELETE和false条件是阳性控制，不把36检查等同36个独立缺陷。

永久回归补充真实源表行数、空RowDescription/行、子SELECT和子INSERT RETURNING。
保存的原R97正式binary SHA256为
`fd69b9220cba1b1d2e0ead6498edbecb356fb0d68388659972ebaa76fa08cbb8`，
以显式DBMS_MAIN运行这个固定旧产物，完整66/6失败、实际退出1。另两处实际
响应泄漏为行1、列v/OID23和INSERT 0 1；外层确实存有两行，audit副作用也正确。
这不是漏插入或WHEN过滤错误，是调用者的结果被子命令结果覆盖。子SELECT
控制旧版已通过，不能登记为已经复现的额外SELECT错行。

PostgreSQL 18.6（180006）永久同驱动完整66、实际退出0；Root使用现有SQL-action
触发器扩展，PG使用等价的RETURNS trigger函数包装（DELETE返回OLD，其余NEW；
SELECT用PERFORM，INSERT RETURNING用局部变量接收）。比较真实共享DML的行数、
command tag、结果形状、OID和副作用，不宣称两边CREATE TRIGGER语法相同。

最终70追加UDF回调NOTICE/WARNING和调用者输出恢复四个检查。固定旧binary完整
70/7失败、实际1；一次仅增加diagnostic字段打印的完整观察仍70/7、实际1，
捕获NoticeResponse为空列表，SQL、断言和期限不变，不是重试到绿或延迟验收。

## 调用链和修改方法

main注册的triggerExecutor递归执行动作SQL，动作的INSERT RETURNING等可写入
线程局部DmlResult，也会向stdout输出行/标签。部分外层普通INSERT依赖文本
形成自己的完成标签，网络适配器从外层capture选择第一个INSERT标签/表格，
子命令输出因此被当成外层结果。UPDATE/DELETE显式发布自己的mutation count，
所以原控制未失败。下面的候选结果证明仅隔离structured槽并不足以关闭反例。

第一版仅隔离DmlResult，正式Main重编/链接及repeat成功，binary为
`c30cb2589bafd3f55ff41839e1ad6a0dabbd64efad348263264f48599a884e90`。
完整十协议实际退出1：核心66仍6失败，其余九个完整入口通过。独立终态fence0，
该实现未验收、不提交为修复。原因进一步确认：legacy网络适配器还从stdout
选择第一个INSERT标签并解析RETURNING表格，结果槽隔离不能阻止文本通道泄漏。

第二版在main.cpp触发器边界使用TriggerResultScope：入口take并清空调用者
结果槽，同时建立线程局部嵌套输出capture；退出恢复调用者结果及输出目标，
隐藏成功子命令的行/标签，但保留NOTICE/WARNING/错误诊断；失败时转发原完整
诊断文本。追加UDF早返回的NOTICE/WARNING及调用者capture恢复检查，最终70
参考全部通过。第二版正式binary为
`693c78a0089cf9f04bf3f3ebfc89b09892047b7d7c6831dc3b84bbb53e2b2281`，
完整70/1失败：六个结果错误已关闭，只剩通知断言；独立58 receipt、57 test库
字节及双stamp终态fence0，未把此版登记为全部通过。

进一步确认evalUDFBody向PlPgsql::run传入nullptr NoticeSink，原生普通调用和
trigger上下文都丢NOTICE/WARNING。新增原生完整8/3失败、实际1：返回7、22012
和事务清理正常，只有三个诊断断言失败。最终TableManage.cpp连接该sink，
向已有线程局部输出通道发送NOTICE/WARNING；触发器边界转发真正诊断而非子结果。
其它severity、client_min_messages及完整诊断字段未由本项实现或验收。
RAII覆盖普通动作、
EXECUTE FUNCTION早返回及异常离开；不丢弃真实行写入，不修改外层计数、网络
字段或断言。调用者后续仍自行发布真实RETURNING/完成标签。Session/context
的其它异常恢复、WHEN完整类型语义、trigger函数语言、provider图等未由本项
证明完成；本项也不是重启R65 mutation-child-provider调查。

## 验证记录

第三版binary为dc5371b9c946a390cdafc4be0a724131627db4c00c695d37383e57034ad511e0。
r98-final-gate-driver.log实际0：19不同完整native、11不同完整协议及3真实Main
全0（EXPLAIN/会话测试TU为O0、window TU为O2，57生产库仍正式O2、无stubs）。
原生8、R97154及协议70全通过；独立终态fence0。该轮没有被改写为失败，但之后
补充边界证据否定了“修复已经完整”的结论，所以没有commit为已接受的修复。

永久新增trigger_diagnostic_channel_protocol_e2e_test.py用运行时E字符串生成
换行，避免混入[独立的函数body元数据问题](issue-review-r99-multiline-stored-function-body.md)。
旧第三版完整17/7失败、实际1，PG18.6同源码完整17/0失败、实际0：多行消息被
拆分/截断、前后空白被trim、普通NOTICE/WARNING行被改成通知；成功子SELECT的
ERROR文本还令客户端收到XX000，实际源表行数已经增加。17个断言不是7个独立
存储故障。原raw字面量驱动12/10还包含函数调用42601及后续累计行数差异，保留
原失败但不将其全归因于消息通道。

当前候选新增StatementDiagnostic和线程局部ScopedDiagnosticCapture；协议
请求直接收集severity/SQLSTATE/完整message，不再从stdout辨认消息。CLI使用
独立stderr诊断sink；无sink的嵌入式调用仍有原可捕获打印回退。PL NativeNoticeSink
及41处既有SQL-visible NOTICE/WARNING输出点机械迁移到typed emitter，保留原
消息构造与SQL分支，只改变诊断传输，不改变角色、权限、认证、TDE或其它暂缓
模块的执行语义。成功触发器子输出全部隐藏；真实失败的旧诊断在正常bool错误
返回前转发，异常使用既有structured error路径，RAII只做无分配的通道恢复。

diagnostic_channel_test.cpp纯组件12/12、ASan/UBSan实际0，覆盖嵌套/异常恢复、
两工作线程与leader隔离、UTF8/LF/空白字节、stdout数据不生成诊断及嵌入式回退。
二进制没有StorageEngine/g_engine/ConnectionPool符号，不是全库sanitizer。
共享头变化后，正式全58重编尚在运行；只有新源码/产物完整核对并验证后才接受。

原始本机证据位于
`/tmp/dbms-review-r46.ifu6SWLp/`：r98-old-server-when.log、
r98-reference-server-when.log、r98-original-dbms-main、
r98-old-expanded-permanent.log和r98-reference-expanded-permanent.log。
初版旧36运行首尾为R97输入/binary/全部58 receipt/57 test库字节独立fence0；
扩展旧66使用前述已校验的固定旧binary，不冒充当前候选产物或全58重新编译。
第一版失败为r98-protocol-neighbors.log及r98-protocol-results，固定产物另存
r98-result-slot-only-candidate-main，原producer/fence及对象receipt一并保留。
第二版失败见r98-v2-core-wire.log、r98-notice-old-native.log及
r98-v2-gate-and-terminal-fence.log；固定产物为r98-output-scope-candidate-main。
后者包装器退出0只表示两个预期失败和fence都已核对，不是原生/协议测试通过。
这些临时路径不是可移植发行产物，永久复现在
`tests/trigger_nested_result_isolation_protocol_e2e_test.py`，已加入注册入口。

新增通道反例及组件日志为r98-text-channel-old-runtime.log、
r98-diagnostic-channel-reference.log、r98-diagnostic-channel-component-sanitizer.log；
第三版原程序保存为r98-text-diagnostic-candidate-main。当前全58编译日志为
r98-typed-channel-fresh58-build.log。上述临时路径均非可移植发行产物。

当前注册集合884 native+3真实Main+509 Python=1396，未取得该组合完整终态。
[最新1391完整终态](issue-full-registered-5091b39c.md)不包含R97/R98；原273仍
22 complete/166 partial/70 unverified/15 deferred_by_user，真实TLS等门禁缺失。
保留所有旧失败，不放宽期限或重试到绿，不恢复[用户暂缓专项](deferred-scope.md)，
只本地独立commit，不push，Actions禁用，README保持长期项目入口。
