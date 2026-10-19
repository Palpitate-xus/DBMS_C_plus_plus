# R164：存储函数自定义标量类型声明的命名空间归属（有限修复）

状态：普通自定义标量声明身份的有限修复已通过两端组合验收，不等于通用复合值codec完成。

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
