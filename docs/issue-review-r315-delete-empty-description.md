# R315：无 RETURNING 的 DELETE 多发零列结果描述

状态：已实际复现，尚未修复，未暂缓；下一项优先处理。

R314 的冻结 284 项协议检查保留了五个 Simple DELETE：未限定 TEMP、
`pg_temp`、quoted `pg_temp`、quoted 字面点号名称，以及默认查找顺序下的
TEMP/public 同名对象。PostgreSQL 18.6 成功帧为 `C/Z`，产品却为 `T/C/Z`；
额外 `T` 的 body 为零列描述。无 RETURNING 的操作不应宣告一个结果集。
Extended 请求未在本项观察中呈现相同多发描述，不能据此宣称全部模式正确。

旧产品及 R314 候选均实际复现，参考端相符；五项严格断言及全部原始请求
都保留，没有放宽 packet 顺序。当前证据位于
`/tmp/dbms-r314-readonly-temp-target.z04LzlSq/` 的
`reference-old-observation.log`、两侧 `*-old-observation.log`、
`*-current-observation.log` 和 `*-early-full284.log`。

源码线索：Network 对 `available && !metadataOnly` 的结构化 DML 结果直接
设置 `resultSet=true`，Simple 发送方据此发 RowDescription，即使描述为空。
应追溯真实 mutation 输出的所有者：无返回投影时应发布“仅命令结果”，
有 RETURNING 时即使零行仍应保留描述；不能在网络发送方一律丢弃零列描述，
合法零列查询仍需要自己的结果描述和数据帧。

修复时验证 INSERT/UPDATE/DELETE/MERGE 的无 RETURNING 与有 RETURNING、
零匹配/多行、Simple/Extended、命令标签和下一请求恢复，增加真实输出
生产者检查，并保持合法零列结果行为。不恢复用户已暂缓的完整结果载体迁移。
R314 的事务/目标解析修复不作为本项修复证据；须独立验证、独立 commit。

R314 最终普通/SAN 两侧的严格 284 项仍各有同样五项失败；
`completed-component-verification-receipt.json` 逐项证明只有这五个帧种类差异，
额外 T 的原始 body 精确为 `0000`，其后的 C/Z 与参考端相同。
这四条非零执行记录独立列账，未称为已暂缓或已完成。
