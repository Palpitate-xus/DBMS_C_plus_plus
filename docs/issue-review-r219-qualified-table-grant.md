# R219：GRANT/REVOKE 与读取检查使用同一张表的身份

状态：本项有限修复已完成两侧构建及相关回归验收；不是完整 ACL 或总清单完成。

## 问题与修改方法

| 问题 | 原因 | 修改方法 |
| --- | --- | --- |
| `GRANT SELECT ON schema.table TO role` 对已创建的表报 42P01，REVOKE 同样失败 | legacy Main 将 SQL 限定名称直接传给使用物理名称的 `tableExists` 和 ACL API | 在检查 grant option 或写入 ACL 之前，解析 SQL 标识符、检查显式模式并复用现有关系解析器；授权、撤权和 grant option 查询传同一个物理身份 |
| `ON TABLE schema.table` 把 `table ` 当成名称的一部分 | 未识别可选 TABLE 关键字 | 在表对象分支识别 TABLE；SCHEMA/SEQUENCE/FUNCTION 和既有通配符分支不变 |
| SELECT 仍可能拒绝已经授权的限定表 | 简单物理计划与普通单源 SELECT 已解析 FROM，却仍用原始 SQL 名称查询表级/列级 ACL | 两条读取路径将已经解析的物理表名传给现有检查函数；不修改权限集合或 admin、owner、继承判断 |
| 表/列拒绝信息退化为 XX000 | bool-error 路径的输出未携带 SQLSTATE | 保留拒绝及 bool 返回约定，明确报告 42501；不存在的关系为 42P01，不存在的显式模式为 3F000 |

最小示例：创建 `s.t`，插入数据后执行 `GRANT SELECT ON s.t TO r`。
修复前不能找到关系；仅让 GRANT 返回成功也不足以验收，必须让角色 r 实际读取 s.t，
同时不能读取另一个模式的同名表，并在 REVOKE 后重新拒绝读取。

## 验证

新增 `tests/qualified_table_grant_protocol_e2e_test.py` 完整 403 项，覆盖：

- 两个模式的同名表及 public 控制表；显式路径与目标模式相反时不串对象。
- 未引用名称的大小写折叠、引用大小写、名称内部的点与双引号；可选 TABLE。
- Simple Query、Extended text/binary 命令；Parse/Describe 不提前授权或撤权。
- 非 owner 角色的实际整数读取、授权后允许、撤权后拒绝、未授权同名表拒绝。
- 列级 SELECT(n)、其他列拒绝、WITH GRANT OPTION 的角色转授权及撤销。
- 不存在关系/模式的精确错误码、无部分成功、ReadyForQuery 状态和私有对象清理。

完整同字节夹具在新建私有 PostgreSQL 18.6 实例实际退出 0、403 项零失败，实例停止退出 0。
参考收据：`/tmp/dbms-private-pg18-full-fixture.f58rc_ny/completed-reference-receipt.json`，
SHA256 `43925527843d26e7b8c53cc35b53e18269b5b70577786ead4ff0dd6a2ee20ebd`。
修复前纯提交树和工作区分别完整执行，均为 403 项中 140 项失败、进程退出 1；
这些包含后续读取失败，不是 140 个独立根因。

本地证据目录：`/tmp/dbms-r219-grant-target.WboWurjZ`。
基线记录 `completed-old-baseline-receipt.json` 保留实际失败，不修改测试为兼容错误结果。

工作区与纯提交投影各有 60 个生产对象、129 个自有头文件，输入逐一匹配。
两侧各只重编实际改变的 Main，其余 59 个源文件/头文件/选项匹配的对象复用；
不能称本轮全 60 fresh。Root 正常构建实际一次编译/一次链接，重复零编译零链接。
两侧不混用 OutputCapture 等不同头文件对应的对象或 test stub。

最终完整运行 30 次，全部实际退出 0：每侧 4 个原生程序、11 个协议驱动。
两侧完整新增 403 项均零失败，断言及参考夹具字节未削弱；另覆盖既有 pg_stat_activity、
限定/引用函数、普通引用标量、DEFAULT、DELETE ONLY、CASE、投影描述及拼接回归。
原生 auth_privileges/create_role_full 是解析回归，不能冒充实际授权语义证据；
授权/撤权效果由连接真实 Main 的新协议驱动中非 owner 角色读取验证。
这些是有限相关回归，不是完整测试清单运行。纯提交树注册清单为 935 native、
587 Python 和 3 real-main，共 1525 个入口；工作区其他既有/暂缓草稿不计入接受树。

版本/文档状态/兼容声明/进度/注册检查通过后，通过独立 index 提交本项 4 个路径。
生产代码和构建注册使用纯提交树上的对应修复字节，不夹带工作区其他修改或 R217。
staged 18 路径的二进制 diff SHA256 保持
`a92dc922d20160fca4e2042d6c88fa1ea5672be45500478dce72935303084556`。

| 本地记录 | SHA256 |
| --- | --- |
| 修复前完整基线 | `f5b7c523269920de439de1112c41e6e2fce1138d307b3b78fd4ac7597f96b9b2` |
| 生产构建 | `ff1981838f03304eaff69a1c27a9d90559ac7c4b021001d2c42904e39714941a` |
| 最终 30 次运行 | `398afa9c82c781b2f86cf739abb6661ad2759415688af39633c1fd69276c274e` |


## 边界与版本管理

这是表对象定位及已解析单源读取身份的有限修复，不是全部 ACL、完整 GRANT 语法或
所有 DML/JOIN 权限消费者的等价性声明。不改变 ACL 持久化格式、认证或权限政策。
带空白的物理标识符在现有空白分隔 ACL 记录中的往返、多个对象/权限/被授权者等
不由这 403 项证明；需要单独复现和验证。既有权限检查必须继续保留。

最新用户要求跳过 R217；其未通过的 CTE/派生表 Describe 等现场保留、不恢复、不提交。
R218 长组合标识符、R222 函数重载等仍开放，其他历史暂缓项也不恢复。
保留 staged 18 路径及工作区其他修改；只提交本项经过测试的纯投影源文件、注册、测试和本说明。
不 push，不启用 GitHub Actions。原 273 个标签和冻结 1,492 项记录不改写；总清单尚未完成。
