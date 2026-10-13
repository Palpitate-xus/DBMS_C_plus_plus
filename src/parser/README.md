# Parser（SQL 解析器）

`src/parser/` 提供 SQL parser、typed AST 和查询绑定。主入口 `src/main.cpp` 仍有尚未迁移的字符串解析及 legacy 路由；能构建 AST 不代表所有消费者已使用完整绑定结果。

基础 DDL 通过 `DdlExecutor` 和 `StorageEngine` 执行。类型名称和修饰符必须验证成功后才能进入存储；ALTER SEQUENCE 的变更由存储层同步物理状态与目录身份。DML 的真实支持范围应结合 `DmlExecutor`、查询绑定及对应协议测试判断。

## 已知边界

[代码审查报告](../../docs/code-review-2026-10-13.md)记录了严格模式仍接受缺少 FROM/INTO 的语句、ON CONFLICT 条件缺少 BOOLEAN 上下文，以及 INSERT/UPDATE RETURNING 消费未绑定子查询的问题。
窗口两端 frame 的方向需要同时扩展 AST、parser 和执行器，不能只修改最终结果格式。
协议Parse/Describe的元数据分析还需显式建立连接会话上下文；否则search_path和临时schema查找可与普通执行不同，详见报告R30。

后续迁移应让各执行消费者使用同一份完整绑定 AST，保留类型、SQL NULL、名字作用域和错误阶段；分析失败时不得进入写入路径。先增加能复现当前错误的 native/协议控制，再逐项修复。

产品接口与项目扩展的区分见[兼容性契约](../../docs/compatibility-contract.md)，完整功能状态见[总清单](../../docs/postgresql-18-gap-audit.md)。
