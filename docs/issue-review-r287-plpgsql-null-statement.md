# R287：PL/pgSQL NULL 占位语句（依赖修复未完成）

状态：普通开放，候选未接受；不是用户暂缓项。R285 和其他明确暂缓项保持不动。
当前接受生产版本仍为 R286 `a9ef1873e886af112cd0d873f2937d56605b4632`。

## 缺陷与候选方法

| 问题 | 影响 | 候选修改方法 |
|---|---|---|
| 解析器把独立 `NULL;` 作为 SQL passthrough | 合法函数执行时报语法错误；宽松 embedding 还会收到本不应发生的 SQL 回调 | 私有 statement AST 增加 NullStmt；识别完整 NULL 关键字及分号，解释时直接返回，不调用宿主、不改 FOUND |
| NULL 后的非法 token 也落入 SQL passthrough | 原生宿主可能执行前序 SQL 后才发现错误，或错误地接受非法语句 | 在整个函数体解析阶段要求 NULL 后只有 trivia 与分号，解释前返回 42601 |

上述仅是未接受候选。没有改公共头、Main、磁盘格式、声明创建时全量验证或
标量子查询准备。不能把原生检查通过作为整个 stored-function 协议通过。
语义参照 [PostgreSQL 18 PL/pgSQL 基本语句](https://www.postgresql.org/docs/18/plpgsql-statements.html)。

## 固定完整检测

40 个合法函数体覆盖大小写、块/行/嵌套注释、普通块、IF、WHILE、FOR、LOOP、
重复占位语句、NULL 返回值与普通文本 null、quoted null 变量、标识符前缀、
FOUND 真/假、未需求分支和 RETURN 后不可达语句。
原生另检查 10 种非法 NULL 语句、严格/无/宽松宿主、零回调及普通 SQL 仍调用一次。

| 完整范围 | 旧版两侧 | 当前候选 |
|---|---|---|
| 原生 157 控制 | 各 151 失败，exit 1 | 两侧各零失败，exit 0 |
| 协议 484 控制 | 各 152 失败，exit 1 | Root 零失败；纯提交 20 失败、exit 1 |
| 实际 PG 180006，同一协议 484 控制 | 零失败，exit 0 | 独立参考 |
| 实际 PG 180006，10 个非法函数体编译 | 均为 42601，exit 0 | 只证明原生 body-parser 的语法预期，不声称产品 CREATE 错误时机已修复 |

失败数是同一缺陷下的多个断言，不是 151/152 个独立 bug。
最初一次用户进度消息误写成 135/144，随后已更正；完整日志及上表为实际值。
最终原生/协议夹具在旧版、PG 和候选使用相同 SHA，没有删案例、放宽 deadline
或把 20 个失败作为跳过通过。初版生成器的 S 占位符替换误改 ELSE，在任何
编译/执行前已修正；原草稿保留，这不是产品执行结果。

## 纯提交侧 20 失败的根因与后续

只增加案例标签追踪，不改 SQL、预期或 deadline 后，确定 20 个失败来自
五个包含 `NULL -- line comment` 后真实 LF 的函数体：每个传输各出现函数
调用错误及随后的 25P02 状态错误。完整 484 footer 仍存在，不能称整批通过。

已接受 `createUDF` 写入原始多行 expression，`getUDF` 却只以一次 getline
读取 expression，再读取 volatility/language/return/strict。真实 LF 截断 body
并移动属性边界，PL 函数可能被误当 SQL 函数执行。这与
[R99 多行函数体元数据](issue-review-r99-multiline-stored-function-body.md)的既有问题一致。
Root 集成投影带有更早的未接受例程存储修改，不能借其成功证明纯提交正确，
也不把本项升级为恢复用户暂缓的 R222 重载整合。

下一步必须先独立修复 R99 的无歧义记录边界、旧数据兼容及失败验证，再重建
R287 并重跑同一 157/484，不能删除 LF 或用空白压平改变用户函数值。
产品 DO dispatcher 属于未修改的 Main 边界，CREATE-time 全量验证属于 R279
用户暂缓范围；本项均没有通过声明。

候选源码、两个新夹具及既有 RAISE 驱动追加内容保留在工作区，尚未作为修复提交。
计划的 168 条相邻回归和两条 scoped UBSan 没有启动：early-primary 的纯提交失败
已令串联任务退出。不能将“准备好脚本”记作执行或通过。
原暂存 18 路径和其他工作区修改保持，原 273 与冻结 1492 证据不改写。
总目标未完成，不 push，Actions 禁用。

证据目录：`/tmp/dbms-r287-plpgsql-null.9OsmxZ3J`，临时目录不保证长期保存。

| 证据 | SHA-256 |
|---|---|
| `completed-preflight-receipt.json` | `8b033901dcb0cdbd8c384a9b894624c3ffa0528387c180de93c3d9e72f9bd3af` |
| `preexecution-template-correction-receipt.json` | `764626cc691d95105b3c96718f6ef821f6ddd9c0e6a771026a9a00b7132c5bcf` |
| `completed-fixed-fixture-v2-receipt.json` | `a86a76de32680f4e7bb425707270c4350b4a23ea669d5bf932f92ac48db65c43` |
| `completed-reference-receipt.json` | `132acab055e471fe6932345d4b155b1f1c7f49c9477809e6e093e779a26e2df9` |
| `completed-grammar-reference-receipt.json` | `5055e2229d5acd8e2230ee788c28c9f477db8b534f320107917da8e003156658` |
| `completed-old-receipt.json` | `b7b02f11ab89616ea8f0f11939d04b2d75b4af26f0b1405623ef84c8df0b5877` |
| `completed-integration-receipt.json` | `48d5d2f06c0cdb1000c5349d9958f20970c9d4bdf33e27443166ba911f77aace` |
| `completed-build-receipt.json` | `411c355b32d4142c35fc2fe8ac2c0a3814fa3494adad215fbd18d404795d050a` |
| `initial-candidate-primary-failure-receipt.json` | `31a832c03cb3f43a8edbef471c2aa9e1f9949a89ecfe252c8a201bc8cf15a4e4` |
| `completed-trace-observation-receipt.json` | `5bcbf31cdbdd5592cb6a5eada2b43dca48a9a234b2c0ba886777bb1b8f54b8aa` |
| `accepted-trace-primary-wire.log` | `649e59e413f0b4460f438db9bf6ceb2909d431ce97eb65a5fe223ef1ea88c6fd` |
