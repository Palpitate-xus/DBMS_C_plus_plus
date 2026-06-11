#pragma once

#include "oid.h"
#include "systables.h"
#include <memory>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <string>

namespace dbms {

// ============================================================================
// Catalog 管理器
//
// 职责：
//   1. 管理所有系统表（pg_namespace, pg_class, pg_attribute, pg_type,
//      pg_proc, pg_depend）的内存缓存
//   2. 提供按 OID / 名称的查询索引
//   3. 维护 OID 分配器
//   4. 持久化/加载系统表数据
//   5. 提供依赖追踪接口（CASCADE / RESTRICT 基础）
//
// 当前简化假设：
//   - 单线程访问（或外部加锁）
//   - 持久化格式：每表一个 CSV 文件
//   - 后续 Phase 3/4 迁移到真正的 HeapTuple/页面格式
// ============================================================================

class CatalogManager {
public:
    // dbPath: 数据库数据目录（如 data/base/mydb/）
    explicit CatalogManager(const std::string& dbPath);
    ~CatalogManager();

    // 禁止拷贝
    CatalogManager(const CatalogManager&) = delete;
    CatalogManager& operator=(const CatalogManager&) = delete;

    // =====================================================================
    // pg_namespace
    // =====================================================================
    Oid createNamespace(const std::string& nspname, Oid owner);
    const PgNamespaceRow* findNamespace(Oid oid) const;
    const PgNamespaceRow* findNamespaceByName(const std::string& name) const;
    bool dropNamespace(Oid oid);

    // =====================================================================
    // pg_class
    // =====================================================================
    Oid createClass(const PgClassRow& row);
    const PgClassRow* findClass(Oid oid) const;
    const PgClassRow* findClassByName(const std::string& name, Oid nspOid) const;
    bool updateClass(Oid oid, const PgClassRow& row);
    bool dropClass(Oid oid);

    // =====================================================================
    // pg_attribute
    // =====================================================================
    void addAttribute(const PgAttributeRow& row);
    std::vector<PgAttributeRow> findAttributes(Oid relOid) const;
    std::vector<PgAttributeRow> findAttributesByNum(Oid relOid) const; // 按 attnum 排序
    const PgAttributeRow* findAttribute(Oid relOid, const std::string& attname) const;
    bool dropAttributes(Oid relOid);

    // =====================================================================
    // pg_type
    // =====================================================================
    Oid createType(const PgTypeRow& row);
    const PgTypeRow* findType(Oid oid) const;
    const PgTypeRow* findTypeByName(const std::string& name, Oid nspOid) const;
    bool dropType(Oid oid);

    // =====================================================================
    // pg_proc
    // =====================================================================
    Oid createProc(const PgProcRow& row);
    const PgProcRow* findProc(Oid oid) const;
    std::vector<const PgProcRow*> findProcsByName(const std::string& name, Oid nspOid) const;
    bool dropProc(Oid oid);

    // =====================================================================
    // pg_depend
    // =====================================================================
    void addDepend(const PgDependRow& row);
    std::vector<PgDependRow> findDepends(Oid classid, Oid objid, int32_t objsubid = 0) const;
    std::vector<PgDependRow> findRefs(Oid refclassid, Oid refobjid, int32_t refobjsubid = 0) const;
    bool removeDepend(Oid classid, Oid objid, int32_t objsubid,
                      Oid refclassid, Oid refobjid, int32_t refobjsubid);

    // 级联/限制删除检查：返回依赖此对象的所有记录
    std::vector<PgDependRow> findAllDependents(Oid refclassid, Oid refobjid) const;

    // =====================================================================
    // 持久化
    // =====================================================================
    void persistAll();
    void loadAll();

    // 获取下一个可用 OID（用于外部手动分配）
    Oid allocateOid();

private:
    std::string dbPath_;
    std::unique_ptr<OidGenerator> oidGen_;
    mutable std::mutex mutex_;

    // 内存存储
    std::vector<PgNamespaceRow> namespaces_;
    std::vector<PgClassRow>     classes_;
    std::vector<PgAttributeRow>  attributes_;
    std::vector<PgTypeRow>       types_;
    std::vector<PgProcRow>       procs_;
    std::vector<PgDependRow>     depends_;

    // OID 索引: oid -> vector 下标
    std::unordered_map<Oid, size_t> nsByOid_;
    std::unordered_map<Oid, size_t> classByOid_;
    std::unordered_map<Oid, size_t> typeByOid_;
    std::unordered_map<Oid, size_t> procByOid_;

    // 名称索引
    std::unordered_map<std::string, Oid> nsByName_;   // nspname -> oid

    // 辅助函数
    std::string catalogFilePath(const std::string& tablename) const;
    void rebuildIndexes();
};

} // namespace dbms
