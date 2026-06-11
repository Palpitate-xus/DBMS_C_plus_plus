#pragma once

#include "catalog.h"
#include "../commands/TableManage.h"
#include <string>

namespace dbms {

// ============================================================================
// 现有元数据迁移：从 StorageEngine 的 .stc / .dt 文件到 Catalog 系统表
//
// 调用时机：
//   - 数据库首次启动且 Catalog 文件不存在时
//   - 显式执行 MIGRATE 命令时
//
// 迁移策略：
//   1. 遍历 dbPath 下所有 .stc 文件
//   2. 解析表名（schema__tablename 格式）
//   3. 调用 StorageEngine::getTableSchema() 读取结构
//   4. 在 Catalog 中创建 pg_namespace、pg_class、pg_attribute、pg_type 条目
//   5. 为 FK / 约束创建 pg_depend 条目
// ============================================================================

struct MigrateResult {
    size_t tablesMigrated = 0;
    size_t namespacesCreated = 0;
    size_t attributesCreated = 0;
    size_t typesCreated = 0;
    size_t dependsCreated = 0;
    std::vector<std::string> errors;

    bool ok() const { return errors.empty(); }
};

// 将单个数据库的所有表结构迁移到 Catalog
MigrateResult migrateDatabaseToCatalog(
    CatalogManager& cat,
    const StorageEngine& engine,
    const std::string& dbname);

} // namespace dbms
