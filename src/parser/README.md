# Parser（SQL 解析器）

## 当前状态

SQL 解析主入口已拆到 `src/parser/`，由递归下降 parser 生成 typed AST；`src/main.cpp::execute()` 仍保留未迁移命令的路由和 legacy fallback：

typed DDL 的列类型转换必须成功才会进入存储层；未知类型和非法类型修饰符 fail-closed，
不会再由执行器静默降级为 `varchar`。

- `DdlExecutor` 消费已迁移的 DDL AST 子集
- `DmlExecutor` 消费普通单表 `INSERT VALUES/DEFAULT VALUES`、受限行级表达式 UPDATE、来源 INNER/CROSS JOIN 的结构化 `UPDATE ... FROM`/`DELETE ... USING`、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING` 和简单单表 `DELETE`
- `execute()` 中的字符串解析仅保留尚未完成 AST 迁移的高级 DML/DQL 与兼容路径

## 未来迁移计划

继续将复杂 `INSERT SELECT`、部分/索引推断 conflict target、引用子查询或其他关系的 `ON CONFLICT DO UPDATE` 表达式/`WHERE`、复杂/子查询/窗口 `RETURNING`、外连接/复杂 `UPDATE ... FROM`/`DELETE ... USING`、多表 DML、`MERGE` 和复杂表达式迁移到结构化执行器，并在每个边界删除对应 legacy 分支；简单单表 INSERT SELECT、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、显式匹配单列或复合主键/唯一约束 target 的常量或只引用 `excluded` 的 evaluator 受限标量表达式 `DO UPDATE` 及目标行/`excluded` 的受限 `WHERE`、以当前目标行列值为输入的受限标量表达式 UPDATE、来源 INNER/CROSS JOIN 的 `UPDATE ... FROM`/`DELETE ... USING`、列投影和 evaluator 支持的受限标量表达式 RETURNING 已接入。
