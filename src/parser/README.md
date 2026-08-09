# Parser（SQL 解析器）

## 当前状态

SQL 解析主入口已拆到 `src/parser/`，由递归下降 parser 生成 typed AST；`src/main.cpp::execute()` 仍保留未迁移命令的路由和 legacy fallback：

- `DdlExecutor` 消费已迁移的 DDL AST 子集
- `DmlExecutor` 消费普通单表 `INSERT VALUES/DEFAULT VALUES`、简单单表 `UPDATE` 和 `DELETE`
- `execute()` 中的字符串解析仅保留尚未完成 AST 迁移的高级 DML/DQL 与兼容路径

## 未来迁移计划

继续将复杂 `INSERT SELECT`、复合/部分/索引推断 conflict target、引用 `excluded` 或列的 `ON CONFLICT DO UPDATE` 表达式、`DO UPDATE WHERE`、复杂/子查询/窗口 `RETURNING`、多表 DML、`MERGE` 和复杂表达式迁移到结构化执行器，并在每个边界删除对应 legacy 分支；简单单表 INSERT SELECT、无 target 的 `ON CONFLICT DO NOTHING`、单列主键/唯一列 target 的常量 `DO UPDATE`、列投影和 evaluator 支持的受限标量表达式 RETURNING 已接入。
