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
    // 名称解析（search_path）
    // =====================================================================
    struct QualifiedName {
        std::string schema; // 为空表示未限定
        std::string name;
    };

    // 解析 schema.name 或 name；返回是否成功
    static bool parseQualifiedName(const std::string& input, QualifiedName& out);

    // 将 "public, pg_catalog" 风格的 search_path 拆分为列表
    static std::vector<std::string> parseSearchPath(const std::string& searchPathStr);

    // 按 search_path 解析关系名（表/视图/索引/序列等）
    // name 可为 "rel" 或 "schema.rel"；searchPath 为空时仅尝试 public schema
    const PgClassRow* resolveRelation(const std::string& name,
                                      const std::vector<std::string>& searchPath) const;

    // 按 search_path 解析列：table.col 或 schema.table.col
    const PgAttributeRow* resolveAttribute(const std::string& tableName,
                                           const std::string& colName,
                                           const std::vector<std::string>& searchPath) const;

    // =====================================================================
    // 临时 schema（会话隔离）
    // =====================================================================
    Oid createTempNamespace(uint64_t sessionId);
    bool dropTempNamespace(uint64_t sessionId);
    const PgNamespaceRow* findTempNamespace(uint64_t sessionId) const;
    void dropAllTempNamespaces(); // 服务器关闭时清理所有临时 schema

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

    // =====================================================================
    // 依赖追踪：CASCADE / RESTRICT 删除计划
    // =====================================================================
    enum class DropBehavior { Restrict, Cascade };

    struct DropPlan {
        std::vector<std::pair<Oid, Oid>> objectsToDrop; // (classid, objid) 按依赖顺序
        std::string error;
        bool ok() const { return error.empty(); }
    };

    // 生成删除计划（不实际执行删除）
    DropPlan planDrop(Oid classid, Oid objid, DropBehavior behavior) const;

    // 执行 CASCADE / RESTRICT 删除（按 classid + objid）
    bool dropObject(Oid classid, Oid objid, DropBehavior behavior,
                    std::string* error = nullptr);

    // =====================================================================
    // pg_authid / pg_auth_members — 角色与用户
    // =====================================================================
    Oid createAuthId(const PgAuthIdRow& row);
    const PgAuthIdRow* findAuthId(Oid oid) const;
    const PgAuthIdRow* findAuthIdByName(const std::string& name) const;
    bool updateAuthId(Oid oid, const PgAuthIdRow& row);
    bool dropAuthId(Oid oid);
    std::vector<PgAuthIdRow> listAuthIds() const;

    void addAuthMember(const PgAuthMembersRow& row);
    std::vector<PgAuthMembersRow> findAuthMembers(Oid roleid) const;
    std::vector<PgAuthMembersRow> findAuthMemberships(Oid member) const;
    bool removeAuthMember(Oid roleid, Oid member);

    // =====================================================================
    // pg_description — COMMENT ON
    // =====================================================================
    void setDescription(Oid objoid, Oid classoid, int32_t objsubid,
                        const std::string& description);
    std::string getDescription(Oid objoid, Oid classoid, int32_t objsubid) const;
    bool removeDescription(Oid objoid, Oid classoid, int32_t objsubid);

    // 按对象类型 + 名称设置注释（支持 SCHEMA / TYPE / FUNCTION / PROCEDURE）
    bool setComment(const std::string& objType,
                    const std::string& qualifiedName,
                    const std::string& comment,
                    const std::vector<std::string>& searchPath = {});

    // =====================================================================
    // Bootstrap: 初始化标准 schema / 类型
    // =====================================================================
    void bootstrapSystemTypes();
    void bootstrapSystemNamespaces();

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
    std::vector<PgAuthIdRow>     authIds_;
    std::vector<PgAuthMembersRow> authMembers_;
    std::vector<PgDescriptionRow> descriptions_;

    // OID 索引: oid -> vector 下标
    std::unordered_map<Oid, size_t> nsByOid_;
    std::unordered_map<Oid, size_t> classByOid_;
    std::unordered_map<Oid, size_t> typeByOid_;
    std::unordered_map<Oid, size_t> procByOid_;
    std::unordered_map<Oid, size_t> authIdByOid_;
    std::unordered_map<Oid, size_t> authMemberByOid_;

    // 名称索引
    std::unordered_map<std::string, Oid> nsByName_;   // nspname -> oid
    std::unordered_map<std::string, Oid> authIdByName_; // rolname -> oid
    std::unordered_map<std::string, size_t> classByName_; // nspOid+relname -> idx

    // 辅助函数
    std::string catalogFilePath(const std::string& tablename) const;
    void rebuildIndexes();
    static std::string classNameKey(Oid nspOid, const std::string& relname);

    // 在已持有 mutex_ 的情况下使用，避免公共接口死锁
    const PgNamespaceRow* findNamespaceUnlocked(Oid oid) const;
    const PgClassRow*     findClassUnlocked(Oid oid) const;
    const PgTypeRow*      findTypeUnlocked(Oid oid) const;
    const PgProcRow*      findProcUnlocked(Oid oid) const;
    const PgAuthIdRow*    findAuthIdUnlocked(Oid oid) const;

    std::vector<PgDependRow> findDependsUnlocked(Oid classid, Oid objid, int32_t objsubid = 0) const;
    std::vector<PgDependRow> findRefsUnlocked(Oid refclassid, Oid refobjid, int32_t refobjsubid = 0) const;
    std::vector<PgDependRow> findAllDependentsUnlocked(Oid refclassid, Oid refobjid) const;

    bool dropAttributesUnlocked(Oid relOid);
    bool dropClassUnlocked(Oid oid);
    bool dropNamespaceUnlocked(Oid oid);
    bool dropProcUnlocked(Oid oid);
    bool dropTypeUnlocked(Oid oid);

    void removeDependRecordsUnlocked(Oid classid, Oid objid);
    DropPlan planDropUnlocked(Oid classid, Oid objid, DropBehavior behavior) const;
    bool executeDropPlanUnlocked(const DropPlan& plan);

    const PgTypeRow* findTypeByNameUnlocked(const std::string& name, Oid nspOid) const;
    const PgProcRow* findProcByNameUnlocked(const std::string& name, Oid nspOid) const;
    void setDescriptionUnlocked(Oid objoid, Oid classoid, int32_t objsubid,
                                const std::string& description);
};

} // namespace dbms
