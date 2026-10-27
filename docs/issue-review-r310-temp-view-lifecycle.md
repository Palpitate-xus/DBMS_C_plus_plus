# R310：TEMP view 修饰/生命周期未接通（用户暂缓）

状态：按用户最新 “skip it” 要求暂缓整个 TEMP VIEW 模块；未修复，不记作完成。

R309 第一轮正确地将 TEMP 序列放进会话 namespace 后，旧 R307 完整 260 的
TEMP/view/Simple 与 TEMP/view/Extended 两个初次冲突请求变为错误成功。
先 CREATE TEMP VIEW 同名 AS SELECT 9，再 CREATE TEMP SEQUENCE；PG 42P07，
产品却 CREATE SEQUENCE 成功。TEMP/table 两个原错成功已经相符，因此仍有
20 失败并不表示失败集合不变：其中两项从 table 场景迁移到了 view 场景。
十八个 IF NOT EXISTS 通知码仍是独立 R308，不删除任何旧 260 断言。

已读 parser 的 CreateViewStmt 有 temp 字段，但 CREATE 分支只传 replace；
DDL executeCreateView 也未使用该 temp 字段。此线索与临时语义丢失相符；
实际 view namespace、跨连接可见性和退出清理仍待实测，不凭成功标签推断
已证明所有持久化后果。

方法：在真实解析声明到 view DDL、存储和会话清理路径保留临时属性；验证
TEMP/TEMPORARY/OR REPLACE、引用临时来源的视图、名字遮蔽、首次名字冲突、
public 控制不受影响、事务/保存点、DISCARD 和退出清理。不能在 TEMP 序列
创建时额外禁止 public 同名 view 来掩盖此问题，public/temp 本可合法共存。
不以更改一个 catalog 字段代替真实生命周期，也不直接采用 Root 未接受草稿。
如实现需要恢复用户已暂缓的完整 catalog/通用语法/全局缓存模块，应先报告
边界并请求指示。当前没有 R310 生产候选；R309 的记录不宣称本项已修复。

证据：/tmp/dbms-r309-temp-sequence.T03W6FZK 的两侧
root/accepted-early-sequence_duplicate_sqlstate_protocol_e2e_test.log。
全部总清单未完成；不 push，Actions 禁用，所有既有用户暂缓范围保持。


## 用户暂缓时的实际状态

本轮已经完成 PostgreSQL 18.6、Root 隔离投影及纯接受副本各 122 项初始观察，
以及各 262 项扩展观察。所有采集进程退出 0，私有 PostgreSQL 已正常停止；
采集成功不表示产品测试通过。初始观察证明 TEMP VIEW 落入 public、其他连接
可以读到、OR REPLACE TEMP 可以改到另一连接的定义，且连接退出后仍可见。
扩展原始记录包括显式 namespace、遮蔽、事务/保存点、临时来源、DML 和依赖。

证据保留在 `/tmp/dbms-r310-temp-view.LbNkOzwG/` 的
`completed-observations-receipt.json`、
`completed-expanded-observations-receipt.json` 及三侧原始日志中。
这些是观察材料，不是已验收回归或完整兼容性证明。

截至暂缓，没有 R310 生产代码、头文件、永久测试或注册修改；不启动修复。
普通 “continue” 不恢复该模块，只有用户明确要求时才重新处理。
R309 已接受的临时序列修复不回退，R308 通知 SQLSTATE 仍为独立待修问题。
原始 273 项总账的完成数保持不变；不 push，不启用 Actions。
