# 第 986 项：SQL-14 query rewrite capability audit

## 结论

`SQL-14` 要求提供 query-tree copy、权限标记和依赖失效，以支持 PostgreSQL rewrite system。源码检索没有发现可用于规则 rewrite 的 query-tree clone/copy 与执行 runtime；目前 rules/event triggers 只有 parser/AST 路径和通用兼容对象处理，没有查询改写或 DDL event 执行。

## 已核验边界

第 983 项的 `python3 tests/div14_feature_gate_test.py` 覆盖 PostgreSQL mode 和 extended mode 的 `CREATE/ALTER/DROP RULE` 与 `CREATE/ALTER/DROP EVENT TRIGGER`，均返回 `0A000`，并确认没有落盘 `.pg_compat_objects` 假对象。这验证了不支持的语法不会静默假成功，但不实现 `SQL-14` 所需的 query tree 复制、权限上下文或依赖失效。

基于此证据，`SQL-14` 从 `unverified` 调整为 `partial`，仍保持清单未勾选。`CAT-19` 同样继续 partial；两个条目相关但不互相替代。

## 未完成

没有 query-tree deep copy、rewrite rule application、权限标记传播或 rewrite 依赖失效。未运行 PostgreSQL 18.6 rewrite oracle/differential，也没有 production code change。

证据：测试提交 `64ab219f`（第 983 项）；本条为审核/总账更新。
