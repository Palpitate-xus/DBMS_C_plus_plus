# Parser（SQL 解析器）

`src/parser/` 提供 SQL parser、typed AST 和查询绑定。主入口 `src/main.cpp` 仍有尚未迁移的字符串解析及 legacy 路由；能构建 AST 不代表所有消费者已使用完整绑定结果。

基础 DDL 通过 `DdlExecutor` 和 `StorageEngine` 执行。类型名称和修饰符必须验证成功后才能进入存储；ALTER SEQUENCE 的变更由存储层同步物理状态与目录身份。DML 的真实支持范围应结合 `DmlExecutor`、查询绑定及对应协议测试判断。

## 已知边界

[代码审查报告](../../docs/code-review-2026-10-13.md)保留原始发现；FROM/INTO、
ON CONFLICT BOOLEAN、RETURNING 子查询和协议会话上下文的当前修复结果见
[逐项进度](../../docs/review-fixes-2026-10-13.md)，不能将原报告当成当前未修列表。
窗口 frame AST 保留两端方向及原始语法区别，纯绑定与执行期 offset 初始化
分别验证；其数值 RANGE 和尚不支持的日期/间隔边界见
[R04](../../docs/issue-review-r04-window-frame-boundaries.md)。

INSERT来源可交给既有WITH查询parser处理，ON CONFLICT/RETURNING仍归INSERT。
WITH必须保留合法AS(query)及实际主语句；不能将DML主语句当成SELECT来源。
实际AST/执行控制及剩余边界见[R49](../../docs/issue-review-r49-materialized-insert-sources.md)。

INSERT SELECT 的直接 UNKNOWN 标量 INTERVAL 输出在第一次输入解析前保留目标
范围/精度，不能先按秒解析再截断；已类型化来源和数组输入阶段不被重写。
13种范围、NULL、无行/派生来源及数组差异见[R53](../../docs/issue-review-r53-insert-interval-context.md)。

后续迁移应让各执行消费者使用同一份完整绑定 AST，保留类型、SQL NULL、名字作用域和错误阶段；分析失败时不得进入写入路径。先增加能复现当前错误的 native/协议控制，再逐项修复。

产品接口与项目扩展的区分见[兼容性契约](../../docs/compatibility-contract.md)，完整功能状态见[总清单](../../docs/postgresql-18-gap-audit.md)。
