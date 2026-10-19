# R164：存储函数自定义标量类型声明的命名空间归属（开放）

状态：已复现，候选修正未验收，尚无生产修复提交。

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

## 验证及证据

待验证enum/domain/composite、SQL/PLpgSQL、参数/返回声明、qualified/bare/quoted名字、
搜索路径竞争、同身份替换、失败替换原子性、缺失类型及纯metadata等边界。
函数声明typmod策略及通用复合值编解码是相邻缺口，不由本项宣称完成。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-old-clean-native.log`、
`r162-v3-{only,root,clean}-results/generic_stored_signature_test.log`。
完整343项未删除。另一个第295项是测试错误：项目既有collation规范化把C写为c，
V3逐字节断言过严，后续保留控制改为规范化比较；不是类型签名修复已通过343项。
