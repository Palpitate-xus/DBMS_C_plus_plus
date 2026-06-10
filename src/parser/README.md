# Parser（SQL 解析器）

## 当前状态

SQL 解析当前内嵌在 `src/main.cpp` 的 `execute()` 函数中，采用手工字符串解析方式：

- 关键字匹配（`startsWithKeyword`, `findTopLevelKeyword`）
- 子句拆分（`parseColumns`, `parseConditions` 等）
- 表达式简单求值

## 未来迁移计划

Phase 4 将引入独立的词法/语法分析器，支持完整的 SQL 语法树（AST）生成，并迁移至此目录。
