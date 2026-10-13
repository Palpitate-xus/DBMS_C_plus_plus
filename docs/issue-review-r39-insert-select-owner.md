# R39：INSERT SELECT 共用冲突处理和结构化 source owner

## 问题及修复

[R35](issue-review-r35-insert-select-conflict-boundary.md) 只修复分句边界。
原 23 项完整协议驱动在 PostgreSQL 18.6 全过，但 R35 候选仍有八项失败：
SELECT 来源的 DO UPDATE/带目标 DO NOTHING 被拒绝，JOIN、标量子查询及
部分表达式退回不适用的旧接收器。不能用 parser 通过代替执行兼容。

SELECT 现在通过其真正的 bound AST、输出描述符和准备游标求值一次，保留
真实 NULL bitmap、参数与行顺序；所有 source 行先物化，再进入 VALUES
已有的 arbiter、R21 BOOLEAN 条件、原子写入和 R20 RETURNING 公共循环。
删除重复且受限的旧 source 投影实现，不在部分求值后重新解析或重试。
JOIN 复用已有 `NativeBoundDmlSources` 的实际 source graph，并持有其 owner
至游标释放；没有新增猜测列值类型或手写另一套 JOIN 求值器。

集成邻居发现直接 `SELECT 30,NULL,'NULL'` 的未知输出没有声明类型，准备
游标返回 XX000。该中间候选未提交为完成修复。binder 在原纯 assignment
preflight 全部成功之后，仅对直接 UNKNOWN 投影根施加目标列上下文；同步
更新真实 AST 根、projection provenance 和输出描述符。已声明的 TEXT、
派生/CTE TEXT 不因此获得任意赋值转换，字符串 `NULL` 不变为 SQL NULL。

## 实际验证与原始证据

私树 `/tmp/dbms-review-root-conflict.Kdz38nRU/repo`，候选 parent `30c58326`
包含 R21 和 R35。未修改公共头文件，真实重编 DmlExecutor.cpp 和
query_binding.cpp 并链接；原生每次链接前核对全部 58 个当前源码、头文件、
flags 和对象签名，结束核对输入未变化。没有跨 header epoch 借对象。

| 检查 | 原始/中间失败 | 最终候选 |
| --- | --- | --- |
| 新原生 53 控制，真实 Root R17 ABI 基线 `1463b6be` | 39 失败，exit 1 | 0 失败，exit 0 |
| R35 保存的完整 23 协议控制 | R35 后八项不匹配，exit 1 | 全通过，exit 0 |
| 同一注册 wrapper 的 PostgreSQL 18.6 23 控制 | 非红证据 | 全通过，exit 0 |
| 直接 UNKNOWN 输出原有 NULL bitmap 原生/协议 | 中间候选 exit 134 / 1，XX000 | 原驱动不改断言，均 exit 0 |
| 七完整原生驱动 | 不作为七项红复现计数 | 全通过，exit 0 |
| 八完整协议驱动（完整 PostgreSQL protocol 之外） | 不作为八项红复现计数 | 全通过，exit 0 |

原生覆盖 source 冲突更新/忽略、fromless、WHERE FALSE、标量、函数、JOIN、
source ORDER BY、真实 NULL、计数/OID、隐式事务释放；后续行除零不得写入
任何 source 前缀，调用者事务及其先前写入仍保留。完整邻居还覆盖 INTERVAL
赋值的错误阶段、非 NULL/默认值、BOOLEAN、唯一索引冲突和 RETURNING 子查询。

完整 `postgres_protocol_test.py` 首次 exit 1，短 statement_timeout=123 时
`pg_settings` 没有返回 setting；保留该日志，不将其计为通过或宣称本项修复
了超时。未改 SQL/assert、未放宽 timeout、未修改取消模块；同一冻结二进制
单独完整重跑 exit 0。最终 SHA-256：
`bfce75092af2c50f19c5cd9303c15dc08ffc9f053d9b86d6adcd4465d194afb6`。

日志均在 `/tmp/dbms-review-root-conflict.Kdz38nRU/`：
`r39-native-root-baseline.log`、`r39-owned-source-candidate-protocol.log`、
`r39-owned-source-candidate-native.log`（保留失败）、`r39-neighbor-*.log`、
`r39-assignment-context-build.log`、`r39-assignment-context-native.log`、
`r39-assignment-*_test.py.log`、`r39-registered-postgres-reference.log` 及
`r39-assignment-postgres_protocol-serial-retry.log`。一次中间编译误删除公用
isStarProjection 帮助函数的失败记录也保留在 `r39-owned-source-build.log`；
恢复后真正重编成功，未使用失败时留下的旧二进制冒充通过。

协议注册 wrapper 直接运行 `tests/compat/insert_select_conflict_owner_known_gap.py`
保存的全部原 SQL/断言，没有删除或替换为缩小版。注册盘点实际 exit 0。
这不是所有 INSERT SELECT/PostgreSQL 查询形状完整兼容的声明；总账中的其他
功能缺口继续独立维护，Root 集成验证在主修复清单另行登记。

复查通用 source 接收器发现其未继承旧路径的来源表/列检查，另列
[R41](issue-review-r41-insert-source-preflight.md) 保留原始13失败及修复验证。
R39 的绿色局部驱动不替代这项后来发现的 source 合约回归。
