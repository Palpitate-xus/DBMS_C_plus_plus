#pragma once

#include "dbms_defs.h"
#include <string>

namespace dbms {

// ============================================================================
// PostgreSQL Cluster Layout 管理
//
// 职责：
//   1. 初始化标准 PG 数据目录结构
//   2. 管理数据库 OID -> 物理路径映射
//   3. 管理表空间符号链接
//
// 目录结构：
//   <dataDir>/
//     ├── base/          — 数据库目录（按 OID 命名）
//     ├── global/        — 共享系统表
//     ├── pg_wal/        — WAL 日志
//     ├── pg_xact/       — 事务状态（CLOG）
//     ├── pg_tblspc/     — 表空间符号链接
//     └── PG_VERSION     — 版本文件
// ============================================================================

class ClusterLayout {
public:
    explicit ClusterLayout(const std::string& dataDir);

    // 初始化完整的数据目录结构（如果不存在）
    bool initCluster();

    // 数据库路径
    std::string databasePath(Oid dbOid) const;
    bool createDatabaseDir(Oid dbOid);
    bool dropDatabaseDir(Oid dbOid);

    // 全局系统表路径
    std::string globalPath() const;

    // WAL 路径
    std::string walPath() const;

    // CLOG 路径
    std::string xactPath() const;

    // 表空间路径
    std::string tablespacePath() const;
    std::string tablespaceLinkPath(Oid spcOid) const;
    bool createTablespaceLink(Oid spcOid, const std::string& realPath);

    // 版本文件
    bool writeVersion(const std::string& version);
    std::string readVersion() const;

    // 关系文件路径（fork：main, fsm, vm）
    enum class ForkType { Main, FSM, VM, Init };
    std::string relationPath(Oid dbOid, Oid relOid, ForkType fork = ForkType::Main) const;

private:
    std::string dataDir_;
    bool ensureDir(const std::string& path);
};

} // namespace dbms
