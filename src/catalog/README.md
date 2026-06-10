# Catalog（系统目录）

## 当前状态

系统目录信息当前内嵌在 `src/commands/TableManage.cpp` 中，通过以下方式管理：

- `information_schema` 与 `pg_catalog` 查询接口
- 表结构序列化到 `.sch` 文件
- 权限存储在 `user.dat` / `role.dat`

## 未来迁移计划

Phase 3 将引入真正的系统表（`pg_class`, `pg_attribute`, `pg_index` 等），并迁移至此目录，实现自举（bootstrap）目录。
