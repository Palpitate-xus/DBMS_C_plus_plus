# R309：与既有 TEMP 表同名的首次 CREATE TEMP SEQUENCE 错误成功（OPEN）

状态：实际复现、未修复、非用户暂缓；比纯通知元数据差异优先处理。

R307 完整 260 的 TEMP/table/Simple 与 TEMP/table/Extended 两个初次重名请求：
CREATE TEMP TABLE r307_temp_table_{simple,extended}(id integer) 及插入值 9 成功，
随后 CREATE TEMP SEQUENCE 同名 START WITH 999，PG 42P07；产品返回成功
CommandComplete CREATE SEQUENCE，Ready I。原表随后仍读出 9。再次创建时
产品会报重名，不能只测第二次请求来掩盖第一次错误成功。

这是没有正确拒绝名字冲突，不是 R307 的 XX000/42P07。协议 260 保留全部
请求和错误断言。尚未验证错误创建的实际存储/namespace 后果，不能凭成功
tag 推断具体落在 public、temp 或何种物理文件；也未声称两对象安全共存。

后续应追踪 Main TEMP 语句规范化与实际 DDL namespace/逻辑关系身份，核对
真实既有关系而非字符串文件名，证明首次失败、不新增序列、原表/OID/值不变、
正确命名空间中的独立名字仍可创建、IF NOT EXISTS/事务/恢复一致，再独立提交。
应遵守已暂缓 catalog/通用语法范围，不以重启相关模块作为隐含授权；如确需
恢复该范围，应先报告边界并请求指示。当前没有本项候选或修复后证据。

实际旧/PG 证据与 [R307](issue-review-r307-sequence-duplicate-sqlstate.md) 共用
完整 260 原始日志。R308 的十八个通知码另列，不混计。总目标未完成；不 push，
Actions 禁用，全部既有用户暂缓范围保持。

## 已核对的源码线索（尚待对象位置/生命周期实测）

受控接受副本 parser.cpp 的 parseCreate 读取 isTemp/isLocalTemp，随后只将
该信息赋给 CreateTableStmt；sequence 分支直接调用 parseCreateSequence，
生成的 CreateObjectStmt 没有接收该修饰。parseCreate 末尾也未统一转存 TEMP。
DdlExecutor::executeCreateSequence 对未限定名字选择普通 search_path，并
跳过 pg_temp/pg_temp_*；未见 TEMP 专门入口。此链路与上述错误成功相符。

这是实际已读源码事实及因果线索，尚未证明错误创建的具体持久对象、连接
隔离或断开清理后果。后续应先实测这些，再判断是否可仅通过现有 AST 选项
与已有临时命名空间生命周期修正；不默认恢复已暂缓的通用语法/完整 catalog
模块。本次没有 R309 生产修改。
