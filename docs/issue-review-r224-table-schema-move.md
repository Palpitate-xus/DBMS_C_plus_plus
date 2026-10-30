# R224：ALTER TABLE SET SCHEMA 未实现（候选，未验收）

原 `StorageEngine::alterTableSetSchema` 对模式标记查找后直接返回不支持，SQL 解析器还
把引用名称原样交给该查找器，导致合法目标报 XX000/找不到表。R218 长限定名称测试
中最后 48 个失败均由迁移及其后续读取/截断/删除触发，不能删除这些断言来关闭 R218。

## 候选修改方法与范围

新增 `RelationSchemaMove`，由实际 StorageEngine/database 所有者发起 DdlTransaction，
预检目标冲突后，复用现有物理关系改名原语迁移整组文件、外键引用、注释等元数据。
保持实际关系、索引、拥有的序列及已有复合/数组类型 OID；替换命名空间依赖边，
迁移拥有的序列文件并改写其所有者，普通被引用序列不迁移。

默认表达式以真实 AST 的字节范围定位 builtin 序列调用中的名称字面量，不用任意子串替换，
不执行函数体或序列。引擎的私有 schema writer 只改计划中的默认文本，保留声明来源和
改名原语已经更新的外键等字段。所有物理/目录发布均受同一快照恢复保护。
解析器消费一个规范化 ColId，并拒绝缺失、限定目标、保留词、尾随内容和复合动作。

这不是普通函数重载修复，不恢复用户暂缓的 R222。也没有补做完整权限/TDE 专项。

## 实际证据

`tests/table_schema_move_protocol_draft.py` 完整 196 项参考在 PG 18.6 退出 0，实例停止退出 0。
包含引用/大小写/带点名称、simple/Describe/Bind、同模式 no-op、OID 连续性、回滚、保存点、
public 和 pg_catalog 往返、冲突/语法、索引和拥有的序列、外键、唯一约束与序列状态。

| 实际运行 | 结果及边界 |
| --- | --- |
| 旧两侧，完整 196 项 | 各退出 1、118 项失败；包含连锁失败，不是 118 个独立缺陷 |
| 第一候选，完整 196 项 | 两侧各退出 1、37 项失败，完整记录保留 |
| 第一候选原生数据库 | 两侧主检查 34 项各 4 失败；seed 11 项通过，冷进程 12 项各 1 失败 |
| 修正序列名称解码类型后的 V2 原生数据库 | 两侧各主检查 34、真实 seed 进程 11、真实冷进程 12 项均实际退出 0、零失败 |
| V2 完整 196 项 | 两侧各退出 1、30 项失败，尚未验收 |
| 工作区 R218 完整 185 项组合 | V1、V2 各一次实际退出 0、零失败；还不是 R218 纯提交树最终验收 |

原生测试通过实际 SQL 子进程建库建表，再由不同的显式 StorageEngine 执行迁移；
不把全局引擎替换成这个实例，不伪造生产 SQL session。原生目录 API 另外写入真实的关联
复合/数组类型记录，验证已有类型的迁移；这不证明 SQL CREATE TABLE 已自动创建缺失的 row type。
覆盖命名空间依赖、真实冷启动、默认值、外键、注释、回滚、保存点及确定性目录同步失败恢复。

第一候选漏识别字面量解码产生的 `character varying`，导致序列默认值仍保留旧路径。
V2 仅扩充这里实际文本数据类型的判定，所有原生和协议断言保留；旧版与首轮失败均不覆盖。

首轮参考还纠正了两个夹具问题：总控制数应为 192 而非 206；管理连接允许把自己的表移入
pg_catalog，不能预期一律 42501。修正后增加移入读取和移回读取，完整固定为 196 项。
首轮源码及参考失败记录保留，不把夹具错误当作数据库已修 bug。

## 仍需完成

V2 的 30 项失败分为两个实际缺口：22 项 OID 控制依赖的 pg_namespace 不是 typed source；
另 8 项涉及把真实用户表移入 pg_catalog 后，现有前端把该模式一律当成特殊数据库，
导致读取及移回找不到物理对象。这两个缺口分别记录为 R225/R226，必须继续修复，
不能改写查询绕过它们或删除失败断言后提交为已完成。

已有代码完成两侧编译与链接；每侧实际重编 TableManage、parser、DdlExecutor 及新组件，
其余匹配对象复用，不是所有单元 fresh。工作区 62 单元包含未接受 R218/暂缓 R222，
纯提交候选 61 单元不含它们；各用自己的头、对象和 stub，不混用。
测试仍为未注册草稿，无 R224 commit。全部 196 项及相关回归通过后才进行独立提交。

本地资料：`/tmp/dbms-r224-table-schema-move.nGsZlR8k`。
完整参考：`/tmp/dbms-private-pg18-full-fixture.e6sxy0_v/completed-reference-receipt.json`，
SHA256 `f13f1ffcb2aebc4d4aec9b5e9e44c532b6b68998978a25540a19201a2d3e09a0`。
旧基线、首轮生产/协议/原生、V2 构建与实际运行分别保存在同目录的
`completed-old-baseline-receipt.json`、`completed-build-receipt.json`、
`completed-first-runtime-receipt.json`、`completed-first-native-receipt.json`、
`completed-v2-build-receipt.json`、`completed-v2-runtime-observations.json`。
观察驱动成功不代表其中产品都通过，每条实际产品退出码独立记录。
原 273 标签和冻结 1,492 结果不改写；不 push、不启用 Actions，总目标未完成。
