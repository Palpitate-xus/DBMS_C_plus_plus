#include "catalog.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <set>
#include <functional>

namespace dbms {

// ============================================================================
// 辅助：CSV 风格持久化
// ============================================================================

static void writeOid(std::ostream& out, Oid oid) {
    out << oid;
}

static void writeString(std::ostream& out, const std::string& s) {
    out << '"';
    for (char c : s) {
        if (c == '"' || c == '\\') out << '\\';
        out << c;
    }
    out << '"';
}

static std::string readString(std::istringstream& in) {
    std::string result;
    in >> std::ws;
    if (in.peek() == '"') {
        in.get(); // skip opening quote
        char c;
        while (in.get(c)) {
            if (c == '\\') {
                if (in.get(c)) result += c;
            } else if (c == '"') {
                break;
            } else {
                result += c;
            }
        }
    } else {
        std::string word;
        if (std::getline(in, word, ',')) {
            result = word;
        }
    }
    return result;
}

static std::string oidPath(const std::string& dbPath) {
    return dbPath + "/.oid_counter";
}

// ============================================================================
// 构造函数 / 析构函数
// ============================================================================

CatalogManager::CatalogManager(const std::string& dbPath)
    : dbPath_(dbPath) {
    oidGen_ = std::make_unique<OidGenerator>(oidPath(dbPath));
    loadAll();
}

CatalogManager::~CatalogManager() {
    persistAll();
}

// ============================================================================
// pg_namespace
// ============================================================================

Oid CatalogManager::createNamespace(const std::string& nspname, Oid owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = oidGen_->allocate();
    PgNamespaceRow row;
    row.oid = oid;
    row.nspname = nspname;
    row.nspowner = owner;
    size_t idx = namespaces_.size();
    namespaces_.push_back(row);
    nsByOid_[oid] = idx;
    nsByName_[nspname] = oid;
    return oid;
}

const PgNamespaceRow* CatalogManager::findNamespace(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nsByOid_.find(oid);
    if (it != nsByOid_.end() && it->second < namespaces_.size()) {
        return &namespaces_[it->second];
    }
    return nullptr;
}

const PgNamespaceRow* CatalogManager::findNamespaceByName(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nsByName_.find(name);
    if (it != nsByName_.end()) {
        return findNamespace(it->second);
    }
    return nullptr;
}

bool CatalogManager::dropNamespace(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nsByOid_.find(oid);
    if (it == nsByOid_.end() || it->second >= namespaces_.size()) return false;
    // Check dependencies
    auto deps = findAllDependents(PgClassOid_Namespace, oid);
    if (!deps.empty()) return false;
    // Mark as dropped by swapping with last and updating index
    size_t idx = it->second;
    nsByName_.erase(namespaces_[idx].nspname);
    if (idx + 1 < namespaces_.size()) {
        namespaces_[idx] = std::move(namespaces_.back());
        nsByOid_[namespaces_[idx].oid] = idx;
    }
    namespaces_.pop_back();
    nsByOid_.erase(oid);
    return true;
}

// ============================================================================
// 临时 schema（会话隔离）
// ============================================================================

Oid CatalogManager::createTempNamespace(uint64_t sessionId) {
    std::string name = "pg_temp_" + std::to_string(sessionId);
    auto existing = findNamespaceByName(name);
    if (existing) return existing->oid;
    return createNamespace(name, 10); // owner = bootstrap superuser
}

bool CatalogManager::dropTempNamespace(uint64_t sessionId) {
    std::string name = "pg_temp_" + std::to_string(sessionId);
    auto ns = findNamespaceByName(name);
    if (!ns) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    // Drop all objects in this temp namespace
    std::vector<Oid> objectsToDrop;
    for (const auto& c : classes_) {
        if (c.relnamespace == ns->oid) {
            objectsToDrop.push_back(c.oid);
        }
    }
    for (Oid oid : objectsToDrop) {
        dropClass(oid);
    }
    return dropNamespace(ns->oid);
}

const PgNamespaceRow* CatalogManager::findTempNamespace(uint64_t sessionId) const {
    return findNamespaceByName("pg_temp_" + std::to_string(sessionId));
}

void CatalogManager::dropAllTempNamespaces() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Oid> tempNsOids;
    for (const auto& ns : namespaces_) {
        if (ns.nspname.size() > 8 && ns.nspname.substr(0, 8) == "pg_temp_") {
            tempNsOids.push_back(ns.oid);
        }
    }
    for (Oid oid : tempNsOids) {
        // Drop all objects in the temp namespace first
        std::vector<Oid> objectsToDrop;
        for (const auto& c : classes_) {
            if (c.relnamespace == oid) {
                objectsToDrop.push_back(c.oid);
            }
        }
        for (Oid objOid : objectsToDrop) {
            dropClass(objOid);
        }
        dropNamespace(oid);
    }
}

// ============================================================================
// pg_class
// ============================================================================

Oid CatalogManager::createClass(const PgClassRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgClassRow r = row;
    r.oid = oid;
    size_t idx = classes_.size();
    classes_.push_back(r);
    classByOid_[oid] = idx;
    return oid;
}

const PgClassRow* CatalogManager::findClass(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = classByOid_.find(oid);
    if (it != classByOid_.end() && it->second < classes_.size()) {
        return &classes_[it->second];
    }
    return nullptr;
}

const PgClassRow* CatalogManager::findClassByName(const std::string& name, Oid nspOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& c : classes_) {
        if (c.relname == name && c.relnamespace == nspOid) {
            return &c;
        }
    }
    return nullptr;
}

bool CatalogManager::updateClass(Oid oid, const PgClassRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = classByOid_.find(oid);
    if (it == classByOid_.end() || it->second >= classes_.size()) return false;
    classes_[it->second] = row;
    classes_[it->second].oid = oid;
    return true;
}

bool CatalogManager::dropClass(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = classByOid_.find(oid);
    if (it == classByOid_.end() || it->second >= classes_.size()) return false;
    size_t idx = it->second;
    if (idx + 1 < classes_.size()) {
        classes_[idx] = std::move(classes_.back());
        classByOid_[classes_[idx].oid] = idx;
    }
    classes_.pop_back();
    classByOid_.erase(oid);
    // Also drop attributes
    dropAttributes(oid);
    return true;
}

// ============================================================================
// pg_attribute
// ============================================================================

void CatalogManager::addAttribute(const PgAttributeRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    attributes_.push_back(row);
}

std::vector<PgAttributeRow> CatalogManager::findAttributes(Oid relOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgAttributeRow> result;
    for (const auto& a : attributes_) {
        if (a.attrelid == relOid) result.push_back(a);
    }
    return result;
}

std::vector<PgAttributeRow> CatalogManager::findAttributesByNum(Oid relOid) const {
    auto result = findAttributes(relOid);
    std::sort(result.begin(), result.end(),
              [](const PgAttributeRow& a, const PgAttributeRow& b) {
                  return a.attnum < b.attnum;
              });
    return result;
}

const PgAttributeRow* CatalogManager::findAttribute(Oid relOid, const std::string& attname) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& a : attributes_) {
        if (a.attrelid == relOid && a.attname == attname) {
            return &a;
        }
    }
    return nullptr;
}

bool CatalogManager::dropAttributes(Oid relOid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::remove_if(attributes_.begin(), attributes_.end(),
                             [relOid](const PgAttributeRow& a) {
                                 return a.attrelid == relOid;
                             });
    attributes_.erase(it, attributes_.end());
    return true;
}

// ============================================================================
// pg_type
// ============================================================================

Oid CatalogManager::createType(const PgTypeRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgTypeRow r = row;
    r.oid = oid;
    size_t idx = types_.size();
    types_.push_back(r);
    typeByOid_[oid] = idx;
    return oid;
}

const PgTypeRow* CatalogManager::findType(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = typeByOid_.find(oid);
    if (it != typeByOid_.end() && it->second < types_.size()) {
        return &types_[it->second];
    }
    return nullptr;
}

const PgTypeRow* CatalogManager::findTypeByName(const std::string& name, Oid nspOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& t : types_) {
        if (t.typname == name && t.typnamespace == nspOid) {
            return &t;
        }
    }
    return nullptr;
}

bool CatalogManager::dropType(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = typeByOid_.find(oid);
    if (it == typeByOid_.end() || it->second >= types_.size()) return false;
    size_t idx = it->second;
    if (idx + 1 < types_.size()) {
        types_[idx] = std::move(types_.back());
        typeByOid_[types_[idx].oid] = idx;
    }
    types_.pop_back();
    typeByOid_.erase(oid);
    return true;
}

// ============================================================================
// pg_proc
// ============================================================================

Oid CatalogManager::createProc(const PgProcRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgProcRow r = row;
    r.oid = oid;
    size_t idx = procs_.size();
    procs_.push_back(r);
    procByOid_[oid] = idx;
    return oid;
}

const PgProcRow* CatalogManager::findProc(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = procByOid_.find(oid);
    if (it != procByOid_.end() && it->second < procs_.size()) {
        return &procs_[it->second];
    }
    return nullptr;
}

std::vector<const PgProcRow*> CatalogManager::findProcsByName(const std::string& name, Oid nspOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<const PgProcRow*> result;
    for (const auto& p : procs_) {
        if (p.proname == name && p.pronamespace == nspOid) {
            result.push_back(&p);
        }
    }
    return result;
}

bool CatalogManager::dropProc(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = procByOid_.find(oid);
    if (it == procByOid_.end() || it->second >= procs_.size()) return false;
    size_t idx = it->second;
    if (idx + 1 < procs_.size()) {
        procs_[idx] = std::move(procs_.back());
        procByOid_[procs_[idx].oid] = idx;
    }
    procs_.pop_back();
    procByOid_.erase(oid);
    return true;
}

// ============================================================================
// pg_authid
// ============================================================================

Oid CatalogManager::createAuthId(const PgAuthIdRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgAuthIdRow r = row;
    r.oid = oid;
    size_t idx = authIds_.size();
    authIds_.push_back(r);
    authIdByOid_[oid] = idx;
    authIdByName_[r.rolname] = oid;
    return oid;
}

const PgAuthIdRow* CatalogManager::findAuthId(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authIdByOid_.find(oid);
    if (it != authIdByOid_.end() && it->second < authIds_.size()) {
        return &authIds_[it->second];
    }
    return nullptr;
}

const PgAuthIdRow* CatalogManager::findAuthIdByName(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authIdByName_.find(name);
    if (it != authIdByName_.end()) {
        return findAuthId(it->second);
    }
    return nullptr;
}

bool CatalogManager::updateAuthId(Oid oid, const PgAuthIdRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authIdByOid_.find(oid);
    if (it == authIdByOid_.end() || it->second >= authIds_.size()) return false;
    authIdByName_.erase(authIds_[it->second].rolname);
    authIds_[it->second] = row;
    authIds_[it->second].oid = oid;
    authIdByName_[row.rolname] = oid;
    return true;
}

bool CatalogManager::dropAuthId(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authIdByOid_.find(oid);
    if (it == authIdByOid_.end() || it->second >= authIds_.size()) return false;
    size_t idx = it->second;
    authIdByName_.erase(authIds_[idx].rolname);
    // Remove all memberships involving this role
    auto amIt = std::remove_if(authMembers_.begin(), authMembers_.end(),
                               [oid](const PgAuthMembersRow& m) {
                                   return m.roleid == oid || m.member == oid || m.grantor == oid;
                               });
    authMembers_.erase(amIt, authMembers_.end());
    authMemberByOid_.clear();
    for (size_t i = 0; i < authMembers_.size(); ++i) {
        authMemberByOid_[authMembers_[i].oid] = i;
    }
    if (idx + 1 < authIds_.size()) {
        authIds_[idx] = std::move(authIds_.back());
        authIdByOid_[authIds_[idx].oid] = idx;
    }
    authIds_.pop_back();
    authIdByOid_.erase(oid);
    return true;
}

std::vector<PgAuthIdRow> CatalogManager::listAuthIds() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return authIds_;
}

// ============================================================================
// pg_auth_members
// ============================================================================

void CatalogManager::addAuthMember(const PgAuthMembersRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    PgAuthMembersRow r = row;
    if (r.oid == INVALID_OID) r.oid = oidGen_->allocate();
    size_t idx = authMembers_.size();
    authMembers_.push_back(r);
    authMemberByOid_[r.oid] = idx;
}

std::vector<PgAuthMembersRow> CatalogManager::findAuthMembers(Oid roleid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgAuthMembersRow> result;
    for (const auto& m : authMembers_) {
        if (m.roleid == roleid) result.push_back(m);
    }
    return result;
}

std::vector<PgAuthMembersRow> CatalogManager::findAuthMemberships(Oid member) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgAuthMembersRow> result;
    for (const auto& m : authMembers_) {
        if (m.member == member) result.push_back(m);
    }
    return result;
}

bool CatalogManager::removeAuthMember(Oid roleid, Oid member) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::remove_if(authMembers_.begin(), authMembers_.end(),
                             [roleid, member](const PgAuthMembersRow& m) {
                                 return m.roleid == roleid && m.member == member;
                             });
    if (it == authMembers_.end()) return false;
    authMembers_.erase(it, authMembers_.end());
    authMemberByOid_.clear();
    for (size_t i = 0; i < authMembers_.size(); ++i) {
        authMemberByOid_[authMembers_[i].oid] = i;
    }
    return true;
}

// ============================================================================
// pg_depend
// ============================================================================

void CatalogManager::addDepend(const PgDependRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    depends_.push_back(row);
}

std::vector<PgDependRow> CatalogManager::findDepends(Oid classid, Oid objid, int32_t objsubid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgDependRow> result;
    for (const auto& d : depends_) {
        if (d.classid == classid && d.objid == objid &&
            (objsubid < 0 || d.objsubid == objsubid)) {
            result.push_back(d);
        }
    }
    return result;
}

std::vector<PgDependRow> CatalogManager::findRefs(Oid refclassid, Oid refobjid, int32_t refobjsubid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgDependRow> result;
    for (const auto& d : depends_) {
        if (d.refclassid == refclassid && d.refobjid == refobjid &&
            (refobjsubid < 0 || d.refobjsubid == refobjsubid)) {
            result.push_back(d);
        }
    }
    return result;
}

std::vector<PgDependRow> CatalogManager::findAllDependents(Oid refclassid, Oid refobjid) const {
    return findRefs(refclassid, refobjid, -1);
}

bool CatalogManager::removeDepend(Oid classid, Oid objid, int32_t objsubid,
                                  Oid refclassid, Oid refobjid, int32_t refobjsubid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::remove_if(depends_.begin(), depends_.end(),
                             [&](const PgDependRow& d) {
                                 return d.classid == classid && d.objid == objid &&
                                        d.objsubid == objsubid &&
                                        d.refclassid == refclassid &&
                                        d.refobjid == refobjid &&
                                        d.refobjsubid == refobjsubid;
                             });
    if (it == depends_.end()) return false;
    depends_.erase(it, depends_.end());
    return true;
}

// ============================================================================
// pg_description — COMMENT ON
// ============================================================================

void CatalogManager::setDescription(Oid objoid, Oid classoid, int32_t objsubid,
                                    const std::string& description) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Update existing or append new
    for (auto& d : descriptions_) {
        if (d.objoid == objoid && d.classoid == classoid && d.objsubid == objsubid) {
            d.description = description;
            return;
        }
    }
    PgDescriptionRow r;
    r.objoid = objoid;
    r.classoid = classoid;
    r.objsubid = objsubid;
    r.description = description;
    descriptions_.push_back(r);
}

std::string CatalogManager::getDescription(Oid objoid, Oid classoid, int32_t objsubid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& d : descriptions_) {
        if (d.objoid == objoid && d.classoid == classoid && d.objsubid == objsubid) {
            return d.description;
        }
    }
    return "";
}

bool CatalogManager::removeDescription(Oid objoid, Oid classoid, int32_t objsubid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::remove_if(descriptions_.begin(), descriptions_.end(),
                             [objoid, classoid, objsubid](const PgDescriptionRow& d) {
                                 return d.objoid == objoid && d.classoid == classoid
                                        && d.objsubid == objsubid;
                             });
    if (it == descriptions_.end()) return false;
    descriptions_.erase(it, descriptions_.end());
    return true;
}

// ============================================================================
// 依赖追踪：CASCADE / RESTRICT 删除计划
// ============================================================================

CatalogManager::DropPlan CatalogManager::planDrop(Oid classid, Oid objid,
                                                   DropBehavior behavior) const {
    std::lock_guard<std::mutex> lock(mutex_);
    DropPlan plan;

    std::vector<std::pair<Oid, Oid>> toDrop;       // 后序遍历结果
    std::set<std::pair<Oid, Oid>> visited;
    std::set<std::pair<Oid, Oid>> inStack;

    std::function<bool(Oid, Oid)> dfs = [&](Oid cid, Oid oid) -> bool {
        auto key = std::make_pair(cid, oid);
        if (visited.count(key)) return true;
        if (inStack.count(key)) return true; // 循环依赖理论上不应发生
        inStack.insert(key);

        auto deps = findRefs(cid, oid, -1);
        for (const auto& d : deps) {
            if (d.deptype == 'p') {
                plan.error = "cannot drop object because it is required by the database system";
                return false;
            }
            auto depKey = std::make_pair(d.classid, d.objid);
            if (!dfs(d.classid, d.objid)) return false;
        }

        inStack.erase(key);
        visited.insert(key);
        toDrop.push_back(key);
        return true;
    };

    if (!dfs(classid, objid)) return plan;

    if (behavior == DropBehavior::Restrict && toDrop.size() > 1) {
        plan.error = "cannot drop object because other objects depend on it";
        return plan;
    }

    plan.objectsToDrop = std::move(toDrop);
    return plan;
}

// ============================================================================
// Bootstrap: 初始化标准 namespace / 类型
// ============================================================================

void CatalogManager::bootstrapSystemNamespaces() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto ensureNs = [&](Oid oid, const std::string& name, Oid owner) {
        if (nsByOid_.count(oid)) return;
        PgNamespaceRow row;
        row.oid = oid;
        row.nspname = name;
        row.nspowner = owner;
        size_t idx = namespaces_.size();
        namespaces_.push_back(row);
        nsByOid_[oid] = idx;
        nsByName_[name] = oid;
    };
    ensureNs(11,    "pg_catalog",   10);  // PG 标准 OID
    ensureNs(99,    "pg_toast",     10);
    ensureNs(2200,  "public",       10);
    ensureNs(1213,  "pg_temp_1",    10);
}

void CatalogManager::bootstrapSystemTypes() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto ensureType = [&](Oid oid, const std::string& name, int16_t len,
                         char typtype, char category) {
        if (typeByOid_.count(oid)) return;
        PgTypeRow row;
        row.oid = oid;
        row.typname = name;
        row.typnamespace = 11; // pg_catalog
        row.typlen = len;
        row.typtype = typtype;
        row.typcategory = category;
        size_t idx = types_.size();
        types_.push_back(row);
        typeByOid_[oid] = idx;
    };
    // PG 标准类型 OID（bootstrap 固定值）
    ensureType(16,    "bool",        1,   'b', 'B');
    ensureType(17,    "bytea",      -1,   'b', 'U');
    ensureType(18,    "char",        1,   'b', 'S');
    ensureType(19,    "name",       64,   'b', 'S');
    ensureType(20,    "int8",        8,   'b', 'N');
    ensureType(21,    "int2",        2,   'b', 'N');
    ensureType(23,    "int4",        4,   'b', 'N');
    ensureType(24,    "regproc",     4,   'b', 'N');
    ensureType(25,    "text",       -1,   'b', 'S');
    ensureType(26,    "oid",         4,   'b', 'N');
    ensureType(27,    "tid",         6,   'b', 'U');
    ensureType(28,    "xid",         4,   'b', 'U');
    ensureType(29,    "cid",         4,   'b', 'U');
    ensureType(30,    "oidvector",  -1,   'b', 'A');
    ensureType(700,   "float4",      4,   'b', 'N');
    ensureType(701,   "float8",      8,   'b', 'N');
    ensureType(1042,  "bpchar",     -1,   'b', 'S');
    ensureType(1043,  "varchar",    -1,   'b', 'S');
    ensureType(1082,  "date",        4,   'b', 'D');
    ensureType(1083,  "time",        8,   'b', 'D');
    ensureType(1114,  "timestamp",   8,   'b', 'D');
    ensureType(1184,  "timestamptz", 8,   'b', 'D');
    ensureType(1186,  "interval",   16,   'b', 'D');
    ensureType(1266,  "timetz",     12,   'b', 'D');
    ensureType(1700,  "numeric",    -1,   'b', 'N');
    ensureType(2950,  "uuid",       16,   'b', 'U');
    ensureType(3802,  "jsonb",      -1,   'b', 'U');
}

// ============================================================================
// OID 分配
// ============================================================================

Oid CatalogManager::allocateOid() {
    return oidGen_->allocate();
}

// ============================================================================
// 持久化 / 加载
// ============================================================================

std::string CatalogManager::catalogFilePath(const std::string& tablename) const {
    return dbPath_ + "/pg_" + tablename + ".cat";
}

void CatalogManager::persistAll() {
    std::lock_guard<std::mutex> lock(mutex_);

    // pg_namespace
    {
        std::ofstream out(catalogFilePath("namespace"));
        for (const auto& r : namespaces_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.nspname); out << ',';
            writeOid(out, r.nspowner); out << '\n';
        }
    }

    // pg_class
    {
        std::ofstream out(catalogFilePath("class"));
        for (const auto& r : classes_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.relname); out << ',';
            writeOid(out, r.relnamespace); out << ',';
            writeOid(out, r.reltype); out << ',';
            writeOid(out, r.relowner); out << ',';
            writeOid(out, r.relam); out << ',';
            writeOid(out, r.relfilenode); out << ',';
            out << r.relkind << ',';
            out << r.relnatts << ',';
            out << (r.relhasindex ? 't' : 'f') << ',';
            out << (r.relpersistence) << ',';
            out << (r.relisshared ? 't' : 'f');
            out << '\n';
        }
    }

    // pg_attribute
    {
        std::ofstream out(catalogFilePath("attribute"));
        for (const auto& r : attributes_) {
            writeOid(out, r.attrelid); out << ',';
            writeString(out, r.attname); out << ',';
            writeOid(out, r.atttypid); out << ',';
            out << r.attnum << ',';
            out << r.attlen << ',';
            out << r.atttypmod << ',';
            out << (r.attnotnull ? 't' : 'f') << ',';
            out << (r.atthasdef ? 't' : 'f') << ',';
            out << r.attstorage << ',';
            out << r.attalign;
            out << '\n';
        }
    }

    // pg_type
    {
        std::ofstream out(catalogFilePath("type"));
        for (const auto& r : types_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.typname); out << ',';
            writeOid(out, r.typnamespace); out << ',';
            out << r.typlen << ',';
            out << r.typtype << ',';
            out << r.typcategory << ',';
            writeOid(out, r.typelem); out << ',';
            writeOid(out, r.typarray);
            out << '\n';
        }
    }

    // pg_proc
    {
        std::ofstream out(catalogFilePath("proc"));
        for (const auto& r : procs_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.proname); out << ',';
            writeOid(out, r.pronamespace); out << ',';
            out << r.prokind << ',';
            writeOid(out, r.prorettype); out << ',';
            out << r.pronargs << ',';
            writeString(out, r.prosrc);
            out << '\n';
        }
    }

    // pg_depend
    {
        std::ofstream out(catalogFilePath("depend"));
        for (const auto& r : depends_) {
            writeOid(out, r.classid); out << ',';
            writeOid(out, r.objid); out << ',';
            out << r.objsubid << ',';
            writeOid(out, r.refclassid); out << ',';
            writeOid(out, r.refobjid); out << ',';
            out << r.refobjsubid << ',';
            out << r.deptype;
            out << '\n';
        }
    }

    // pg_authid
    {
        std::ofstream out(catalogFilePath("authid"));
        for (const auto& r : authIds_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.rolname); out << ',';
            out << (r.rolsuper ? 't' : 'f') << ',';
            out << (r.rolinherit ? 't' : 'f') << ',';
            out << (r.rolcreaterole ? 't' : 'f') << ',';
            out << (r.rolcreatedb ? 't' : 'f') << ',';
            out << (r.rolcanlogin ? 't' : 'f') << ',';
            out << (r.rolreplication ? 't' : 'f') << ',';
            out << (r.rolbypassrls ? 't' : 'f') << ',';
            out << r.rolconnlimit << ',';
            writeString(out, r.rolpassword); out << ',';
            writeString(out, r.rolvaliduntil);
            out << '\n';
        }
    }

    // pg_auth_members
    {
        std::ofstream out(catalogFilePath("authmembers"));
        for (const auto& r : authMembers_) {
            writeOid(out, r.oid); out << ',';
            writeOid(out, r.roleid); out << ',';
            writeOid(out, r.member); out << ',';
            writeOid(out, r.grantor); out << ',';
            out << (r.admin_option ? 't' : 'f');
            out << '\n';
        }
    }

    // pg_description
    {
        std::ofstream out(catalogFilePath("description"));
        for (const auto& r : descriptions_) {
            writeOid(out, r.objoid); out << ',';
            writeOid(out, r.classoid); out << ',';
            out << r.objsubid << ',';
            writeString(out, r.description);
            out << '\n';
        }
    }

    oidGen_->persist();
}

void CatalogManager::loadAll() {
    std::lock_guard<std::mutex> lock(mutex_);

    // pg_namespace
    {
        std::ifstream in(catalogFilePath("namespace"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgNamespaceRow r;
            iss >> r.oid; iss.ignore(1);
            r.nspname = readString(iss); iss.ignore(1);
            iss >> r.nspowner;
            namespaces_.push_back(r);
        }
    }

    // pg_class
    {
        std::ifstream in(catalogFilePath("class"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgClassRow r;
            iss >> r.oid; iss.ignore(1);
            r.relname = readString(iss); iss.ignore(1);
            iss >> r.relnamespace; iss.ignore(1);
            iss >> r.reltype; iss.ignore(1);
            iss >> r.relowner; iss.ignore(1);
            iss >> r.relam; iss.ignore(1);
            iss >> r.relfilenode; iss.ignore(1);
            iss >> r.relkind; iss.ignore(1);
            iss >> r.relnatts; iss.ignore(1);
            char flag;
            iss >> flag; r.relhasindex = (flag == 't'); iss.ignore(1);
            iss >> r.relpersistence; iss.ignore(1);
            iss >> flag; r.relisshared = (flag == 't');
            classes_.push_back(r);
        }
    }

    // pg_attribute
    {
        std::ifstream in(catalogFilePath("attribute"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgAttributeRow r;
            iss >> r.attrelid; iss.ignore(1);
            r.attname = readString(iss); iss.ignore(1);
            iss >> r.atttypid; iss.ignore(1);
            iss >> r.attnum; iss.ignore(1);
            iss >> r.attlen; iss.ignore(1);
            iss >> r.atttypmod; iss.ignore(1);
            char flag;
            iss >> flag; r.attnotnull = (flag == 't'); iss.ignore(1);
            iss >> flag; r.atthasdef = (flag == 't'); iss.ignore(1);
            iss >> r.attstorage; iss.ignore(1);
            iss >> r.attalign;
            attributes_.push_back(r);
        }
    }

    // pg_type
    {
        std::ifstream in(catalogFilePath("type"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgTypeRow r;
            iss >> r.oid; iss.ignore(1);
            r.typname = readString(iss); iss.ignore(1);
            iss >> r.typnamespace; iss.ignore(1);
            iss >> r.typlen; iss.ignore(1);
            iss >> r.typtype; iss.ignore(1);
            iss >> r.typcategory; iss.ignore(1);
            iss >> r.typelem; iss.ignore(1);
            iss >> r.typarray;
            types_.push_back(r);
        }
    }

    // pg_proc
    {
        std::ifstream in(catalogFilePath("proc"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgProcRow r;
            iss >> r.oid; iss.ignore(1);
            r.proname = readString(iss); iss.ignore(1);
            iss >> r.pronamespace; iss.ignore(1);
            iss >> r.prokind; iss.ignore(1);
            iss >> r.prorettype; iss.ignore(1);
            iss >> r.pronargs; iss.ignore(1);
            r.prosrc = readString(iss);
            procs_.push_back(r);
        }
    }

    // pg_depend
    {
        std::ifstream in(catalogFilePath("depend"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgDependRow r;
            iss >> r.classid; iss.ignore(1);
            iss >> r.objid; iss.ignore(1);
            iss >> r.objsubid; iss.ignore(1);
            iss >> r.refclassid; iss.ignore(1);
            iss >> r.refobjid; iss.ignore(1);
            iss >> r.refobjsubid; iss.ignore(1);
            iss >> r.deptype;
            depends_.push_back(r);
        }
    }

    // pg_authid
    {
        std::ifstream in(catalogFilePath("authid"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgAuthIdRow r;
            iss >> r.oid; iss.ignore(1);
            r.rolname = readString(iss); iss.ignore(1);
            char flag;
            iss >> flag; r.rolsuper = (flag == 't'); iss.ignore(1);
            iss >> flag; r.rolinherit = (flag == 't'); iss.ignore(1);
            iss >> flag; r.rolcreaterole = (flag == 't'); iss.ignore(1);
            iss >> flag; r.rolcreatedb = (flag == 't'); iss.ignore(1);
            iss >> flag; r.rolcanlogin = (flag == 't'); iss.ignore(1);
            iss >> flag; r.rolreplication = (flag == 't'); iss.ignore(1);
            iss >> flag; r.rolbypassrls = (flag == 't'); iss.ignore(1);
            iss >> r.rolconnlimit; iss.ignore(1);
            r.rolpassword = readString(iss); iss.ignore(1);
            r.rolvaliduntil = readString(iss);
            authIds_.push_back(r);
        }
    }

    // pg_auth_members
    {
        std::ifstream in(catalogFilePath("authmembers"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgAuthMembersRow r;
            iss >> r.oid; iss.ignore(1);
            iss >> r.roleid; iss.ignore(1);
            iss >> r.member; iss.ignore(1);
            iss >> r.grantor; iss.ignore(1);
            char flag;
            iss >> flag; r.admin_option = (flag == 't');
            authMembers_.push_back(r);
        }
    }

    // pg_description
    {
        std::ifstream in(catalogFilePath("description"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgDescriptionRow r;
            iss >> r.objoid; iss.ignore(1);
            iss >> r.classoid; iss.ignore(1);
            iss >> r.objsubid; iss.ignore(1);
            r.description = readString(iss);
            descriptions_.push_back(r);
        }
    }

    rebuildIndexes();
}

void CatalogManager::rebuildIndexes() {
    nsByOid_.clear();
    nsByName_.clear();
    for (size_t i = 0; i < namespaces_.size(); ++i) {
        nsByOid_[namespaces_[i].oid] = i;
        nsByName_[namespaces_[i].nspname] = namespaces_[i].oid;
    }

    classByOid_.clear();
    for (size_t i = 0; i < classes_.size(); ++i) {
        classByOid_[classes_[i].oid] = i;
    }

    typeByOid_.clear();
    for (size_t i = 0; i < types_.size(); ++i) {
        typeByOid_[types_[i].oid] = i;
    }

    procByOid_.clear();
    for (size_t i = 0; i < procs_.size(); ++i) {
        procByOid_[procs_[i].oid] = i;
    }

    authIdByOid_.clear();
    authIdByName_.clear();
    for (size_t i = 0; i < authIds_.size(); ++i) {
        authIdByOid_[authIds_[i].oid] = i;
        authIdByName_[authIds_[i].rolname] = authIds_[i].oid;
    }

    authMemberByOid_.clear();
    for (size_t i = 0; i < authMembers_.size(); ++i) {
        authMemberByOid_[authMembers_[i].oid] = i;
    }
}

} // namespace dbms
