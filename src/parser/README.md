# Parser（SQL 解析器）

## 当前状态

SQL 解析主入口已拆到 `src/parser/`，由递归下降 parser 生成 typed AST；`src/main.cpp::execute()` 仍保留未迁移命令的路由和 legacy fallback：

- `DdlExecutor` 消费已迁移的 DDL AST 子集
- `DmlExecutor` 消费普通单表 `INSERT VALUES/DEFAULT VALUES`、简单单表 `UPDATE` 和 `DELETE`
- `execute()` 中的字符串解析仅保留尚未完成 AST 迁移的高级 DML/DQL 与兼容路径

## 未来迁移计划

继续将 `INSERT SELECT/ON CONFLICT/RETURNING`、多表 DML、`MERGE` 和复杂表达式迁移到结构化执行器，并在每个边界删除对应 legacy 分支。
