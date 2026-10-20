# R164：存储函数自定义标量类型声明的命名空间归属（有限修复）

当前完整矩阵复验：[原72入口](issue-review-current-full72-revalidation.md)在accepted-only clean与工作区各72实际全0，
原35native/34协议/3真实Main、956及190额外raw门均保留，默认15秒/持久化未弱化。
每侧97对象39程序核对，零C++编译、38新链接，donor不冒fresh；2206accepted源码与tested clean一致，Root2325保留。
这不是全1492或原273验收：仅15条总账含本矩阵直接引用测试，258条不能据此关闭；require-complete两侧仍实际1。
R164旧超时日志和未定根因保留，新通过不倒推原因已修。原22/166/70/15状态及明确暂缓不变。
不push，Actions禁用。以下记录按当时输入和范围解释。


状态：普通自定义标量声明身份的有限修复已通过两端组合验收，不等于通用复合值codec完成。

## 2026-10-20：当前accepted-only原956独立复验

accepted源码17f06229（与R182 V2 clean逐blob相同）运行原完整协议夹具，
实际exit0、完整CUSTOM_SCALAR_ROUTINE_DECLARATION_PROTOCOL_COMPLETE 956 FAILURES0。
默认socket15秒、全部断言、956控制和清理顺序未改，没有提高超时或修改持久化。
测试约283.14秒、281次外层只读采样，仅观察本次自有fixture/backend PID；
不发送额外SQL、不停止调试线程，也不向用户既有服务器发信号。
源码2206文件、127头及当前对象/程序hash在运行前后核对；没有新编译或生产修改。

该次有jbd2_log_wait_commit等待样本，但测试完整通过；这既不证明死锁，也不能
倒推历史两次full72超时的原因。单项通过不等于完整矩阵通过，不能宣称与R176无关。
历史两次失败仍保留，下一步需在完整矩阵上下文复验并保存所有失败/终态。

复验receipt：`/tmp/dbms-r164-current-r182-runtime.u6agy187/completed-recheck-receipt.json`，SHA256 `ce4aca1dfa998b3a6a5ae0db44b2c019055a93b0a3b9509c1399d2014155b4c8`。
原日志：`/tmp/dbms-r164-current-r182-runtime.u6agy187/original-full956.log`，SHA256 `8f465088e398d4a1a2aa3749531812c68bedbd6cc188bf9769f56ac15c69f703`。
只读采样driver：`/tmp/dbms-review-r46.ifu6SWLp/r164-current-r182-monitored-driver.log`，SHA256 `43badecff3eb796e26cbbfbc334865bf396fa823e3560f121cc4f0b313e57c09`。

## 实测错误

合法的 `CREATE TYPE public.r162_color AS ENUM('red','blue')` 成功后，
`CREATE FUNCTION r162_enum(p public.r162_color) RETURNS TEXT LANGUAGE SQL
IMMUTABLE STRICT AS $$SELECT 'enum'::text$$` 被拒绝为42704。
[R162](issue-review-r162-generic-stored-signatures.md)原生343项中，第32项声明失败，
第328–333项因缺少函数继续失败；旧版、仅R162干净候选、组合Root均存在。
typed TEXT负向控制当时因缺少callee而通过，不能作为枚举类型匹配已正确的证据。

## 原因与修改方法

[DdlExecutor](../src/commands/DdlExecutor.cpp)的CREATE FUNCTION类型校验将类型名
整体转小写，再传给按物理身份精确查找的enum getter；public枚举的物理名字不含public前缀。
验证和持久化canonicalFunctionType还使用不同的原始字符串处理，不能只去掉public前缀。

应共同修复声明验证及规范化：按真实namespace/search_path解析解码后的标量类型身份，
保留引用、大小写及点号，持久化实际选中的完整身份；检查替换函数时使用同一身份。
内置类型及现有用户数组限制保持原策略，不恢复自动枚举数组。
composite仍有真实sidecar但缺少完整pg_type登记，不能用目录查找替换后丢掉旧的合法声明，
也不能伪造目录行或把DDL通过说成复合值执行已实现。

## 首轮待验收要求与证据

待验证enum/domain/composite、SQL/PLpgSQL、参数/返回声明、qualified/bare/quoted名字、
搜索路径竞争、同身份替换、失败替换原子性、缺失类型及纯metadata等边界。
函数声明typmod策略及通用复合值编解码是相邻缺口，不由本项宣称完成。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-old-clean-native.log`、
`r162-v3-{only,root,clean}-results/generic_stored_signature_test.log`。
完整343项未删除。另一个第295项是测试错误：项目既有collation规范化把C写为c，
V3逐字节断言过严，后续保留控制改为规范化比较；不是类型签名修复已通过343项。

## 最终候选的完整运行检查点

R169 V4工作区完整58入口实际全部通过；本项native570、协议956及共享签名343/1705均0失败。
声明验证和canonical持久化共同按实际namespace解析普通enum/domain/composite身份；
R167/R168/R169修复相邻删除/清理与语法时机后，完整956中原四条清理错误全部消除。
原控制全部保留，search_path仅接受精确可选Status；bare返回声明保留，初始BODY使用固定
canonical type，避免换search_path后BODY重解析改变owner，失败替换42P13控制仍在。
不以跳过DROP或放宽任意协议消息来“通过”。自动用户数组、声明typmod及通用复合值codec仍不由本项关闭。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合构建、依赖与完整日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)。
