#include "catalog.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <set>
#include <functional>
#include <fcntl.h>
#include <unistd.h>

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

static std::string hexEncode(const std::string& value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for (const unsigned char byte : value) {
        encoded.push_back(digits[byte >> 4]);
        encoded.push_back(digits[byte & 0x0f]);
    }
    return encoded;
}

static bool hexDecode(const std::string& encoded, std::string& value) {
    if ((encoded.size() & 1U) != 0) return false;
    const auto nibble = [](char digit) -> int {
        if (digit >= '0' && digit <= '9') return digit - '0';
        if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
        if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
        return -1;
    };
    value.clear();
    value.reserve(encoded.size() / 2);
    for (size_t offset = 0; offset < encoded.size(); offset += 2) {
        const int high = nibble(encoded[offset]);
        const int low = nibble(encoded[offset + 1]);
        if (high < 0 || low < 0) return false;
        value.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
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

template <typename T>
static bool readCatalogCsvField(std::istringstream& input, T& value) {
    if (!(input >> value)) return false;
    char delimiter = 0;
    return input.get(delimiter) && delimiter == ',';
}

template <typename T>
static bool readCatalogCsvLastField(std::istringstream& input, T& value) {
    if (!(input >> value)) return false;
    input >> std::ws;
    return input.eof();
}

static std::string oidPath(const std::string& dbPath) {
    return dbPath + "/.oid_counter";
}

static std::string trimString(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

static std::string toLowerString(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(),
                   [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return r;
}

static std::vector<std::string> splitByComma(const std::string& s) {
    std::vector<std::string> parts;
    std::string cur;
    for (char c : s) {
        if (c == ',') {
            parts.push_back(trimString(cur));
            cur.clear();
        } else {
            cur += c;
        }
    }
    if (!cur.empty() || parts.empty()) {
        parts.push_back(trimString(cur));
    }
    return parts;
}

static bool writeCatalogFileAtomically(
    const std::filesystem::path& target,
    const std::function<void(std::ostream&)>& writer) {
    const std::filesystem::path temp = target.string() + ".tmp." +
        std::to_string(static_cast<unsigned long long>(::getpid()));
    std::ofstream out(temp, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    writer(out);
    out.flush();
    if (!out.good()) {
        out.close();
        std::error_code ec;
        std::filesystem::remove(temp, ec);
        return false;
    }
    out.close();
    if (!out) {
        std::error_code ec;
        std::filesystem::remove(temp, ec);
        return false;
    }

    const int fileFd = ::open(temp.c_str(), O_RDONLY | O_CLOEXEC);
    if (fileFd < 0 || ::fsync(fileFd) != 0) {
        if (fileFd >= 0) ::close(fileFd);
        std::error_code ec;
        std::filesystem::remove(temp, ec);
        return false;
    }
    ::close(fileFd);

    std::error_code ec;
    std::filesystem::rename(temp, target, ec);
    if (ec) {
        std::filesystem::remove(temp, ec);
        return false;
    }

    const int dirFd = ::open(target.parent_path().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirFd < 0 || ::fsync(dirFd) != 0) {
        if (dirFd >= 0) ::close(dirFd);
        return false;
    }
    ::close(dirFd);
    return true;
}

// ============================================================================
// 构造函数 / 析构函数
// ============================================================================

CatalogManager::CatalogManager(const std::string& dbPath)
    : dbPath_(dbPath) {
    std::filesystem::create_directories(dbPath_);
    oidGen_ = std::make_unique<OidGenerator>(oidPath(dbPath));
    loadAll();
}

CatalogManager::~CatalogManager() {
    (void)persistAll();
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

const PgNamespaceRow* CatalogManager::findNamespaceUnlocked(Oid oid) const {
    auto it = nsByOid_.find(oid);
    if (it != nsByOid_.end() && it->second < namespaces_.size()) {
        return &namespaces_[it->second];
    }
    return nullptr;
}

const PgNamespaceRow* CatalogManager::findNamespace(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findNamespaceUnlocked(oid);
}

const PgNamespaceRow* CatalogManager::findNamespaceByName(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nsByName_.find(name);
    if (it != nsByName_.end()) {
        return findNamespaceUnlocked(it->second);
    }
    return nullptr;
}

bool CatalogManager::dropNamespaceUnlocked(Oid oid) {
    auto it = nsByOid_.find(oid);
    if (it == nsByOid_.end() || it->second >= namespaces_.size()) return false;
    auto deps = findAllDependentsUnlocked(PgClassOid_Namespace, oid);
    if (!deps.empty()) return false;
    size_t idx = it->second;
    nsByName_.erase(namespaces_[idx].nspname);
    removeDependRecordsUnlocked(PgClassOid_Namespace, oid);
    if (idx + 1 < namespaces_.size()) {
        namespaces_[idx] = std::move(namespaces_.back());
        nsByOid_[namespaces_[idx].oid] = idx;
    }
    namespaces_.pop_back();
    nsByOid_.erase(oid);
    return true;
}

bool CatalogManager::dropNamespace(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropNamespaceUnlocked(oid);
}

std::vector<PgNamespaceRow> CatalogManager::listNamespaces() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return namespaces_;
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
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nsByName_.find(name);
    if (it == nsByName_.end()) return false;
    Oid nspOid = it->second;
    std::vector<Oid> objectsToDrop;
    for (const auto& c : classes_) {
        if (c.relnamespace == nspOid) {
            objectsToDrop.push_back(c.oid);
        }
    }
    for (Oid oid : objectsToDrop) {
        dropClassUnlocked(oid);
    }
    return dropNamespaceUnlocked(nspOid);
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
        std::vector<Oid> objectsToDrop;
        for (const auto& c : classes_) {
            if (c.relnamespace == oid) {
                objectsToDrop.push_back(c.oid);
            }
        }
        for (Oid objOid : objectsToDrop) {
            dropClassUnlocked(objOid);
        }
        dropNamespaceUnlocked(oid);
    }
}

// ============================================================================
// pg_class
// ============================================================================

const PgClassRow* CatalogManager::findClassUnlocked(Oid oid) const {
    auto it = classByOid_.find(oid);
    if (it != classByOid_.end() && it->second < classes_.size()) {
        return &classes_[it->second];
    }
    return nullptr;
}

Oid CatalogManager::createClass(const PgClassRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgClassRow r = row;
    r.oid = oid;
    size_t idx = classes_.size();
    classes_.push_back(r);
    classByOid_[oid] = idx;
    classByName_[classNameKey(r.relnamespace, r.relname)] = idx;
    // 表/视图/索引/函数/类型均依赖其所属 schema
    if (r.relnamespace != INVALID_OID) {
        PgDependRow dep;
        dep.classid = PgClassOid_Class;
        dep.objid = oid;
        dep.objsubid = 0;
        dep.refclassid = PgClassOid_Namespace;
        dep.refobjid = r.relnamespace;
        dep.refobjsubid = 0;
        dep.deptype = 'n';
        depends_.push_back(dep);
    }
    return oid;
}

const PgClassRow* CatalogManager::findClass(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findClassUnlocked(oid);
}

const PgClassRow* CatalogManager::findClassByName(const std::string& name, Oid nspOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = classByName_.find(classNameKey(nspOid, name));
    if (it != classByName_.end() && it->second < classes_.size()) {
        return &classes_[it->second];
    }
    return nullptr;
}

bool CatalogManager::updateClass(Oid oid, const PgClassRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = classByOid_.find(oid);
    if (it == classByOid_.end() || it->second >= classes_.size()) return false;
    size_t idx = it->second;
    classByName_.erase(classNameKey(classes_[idx].relnamespace, classes_[idx].relname));
    classes_[idx] = row;
    classes_[idx].oid = oid;
    classByName_[classNameKey(classes_[idx].relnamespace, classes_[idx].relname)] = idx;
    return true;
}

bool CatalogManager::renameClass(Oid oid, const std::string& newName) {
    if (newName.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = classByOid_.find(oid);
    if (it == classByOid_.end() || it->second >= classes_.size()) return false;
    const size_t idx = it->second;
    const PgClassRow& current = classes_[idx];
    const std::string newKey = classNameKey(current.relnamespace, newName);
    auto collision = classByName_.find(newKey);
    if (collision != classByName_.end() && collision->second != idx) return false;
    classByName_.erase(classNameKey(current.relnamespace, current.relname));
    classes_[idx].relname = newName;
    classByName_[newKey] = idx;
    return true;
}

bool CatalogManager::dropClassUnlocked(Oid oid) {
    auto it = classByOid_.find(oid);
    if (it == classByOid_.end() || it->second >= classes_.size()) return false;
    size_t idx = it->second;
    classByName_.erase(classNameKey(classes_[idx].relnamespace, classes_[idx].relname));
    removeDependRecordsUnlocked(PgClassOid_Class, oid);
    if (idx + 1 < classes_.size()) {
        classes_[idx] = std::move(classes_.back());
        classByOid_[classes_[idx].oid] = idx;
        classByName_[classNameKey(classes_[idx].relnamespace, classes_[idx].relname)] = idx;
    }
    classes_.pop_back();
    classByOid_.erase(oid);
    dropAttributesUnlocked(oid);
    return true;
}

bool CatalogManager::dropClass(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropClassUnlocked(oid);
}

std::vector<PgClassRow> CatalogManager::listClasses() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return classes_;
}

// ============================================================================
// 名称解析（search_path）
// ============================================================================

std::string CatalogManager::classNameKey(Oid nspOid, const std::string& relname) {
    return std::to_string(nspOid) + "." + relname;
}

bool CatalogManager::parseQualifiedName(const std::string& input, QualifiedName& out) {
    out.schema.clear();
    out.name = input;
    size_t dot = input.rfind('.');
    if (dot == std::string::npos) return true;
    if (dot == 0 || dot + 1 >= input.size()) return false;
    out.schema = input.substr(0, dot);
    out.name = input.substr(dot + 1);
    return true;
}

std::vector<std::string> CatalogManager::parseSearchPath(const std::string& searchPathStr) {
    if (searchPathStr.empty()) return { "public" };
    return splitByComma(searchPathStr);
}

const PgClassRow* CatalogManager::resolveRelation(
        const std::string& name,
        const std::vector<std::string>& searchPath) const {
    QualifiedName qn;
    if (!parseQualifiedName(name, qn)) return nullptr;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!qn.schema.empty()) {
        auto nsIt = nsByName_.find(qn.schema);
        if (nsIt == nsByName_.end()) return nullptr;
        auto it = classByName_.find(classNameKey(nsIt->second, qn.name));
        if (it != classByName_.end() && it->second < classes_.size()) {
            return &classes_[it->second];
        }
        return nullptr;
    }

    for (const auto& nspname : searchPath) {
        auto nsIt = nsByName_.find(nspname);
        if (nsIt == nsByName_.end()) continue;
        auto it = classByName_.find(classNameKey(nsIt->second, qn.name));
        if (it != classByName_.end() && it->second < classes_.size()) {
            return &classes_[it->second];
        }
    }
    return nullptr;
}

const PgAttributeRow* CatalogManager::resolveAttribute(
        const std::string& tableName,
        const std::string& colName,
        const std::vector<std::string>& searchPath) const {
    auto rel = resolveRelation(tableName, searchPath);
    if (!rel) return nullptr;
    return findAttribute(rel->oid, colName);
}

// ============================================================================
// pg_attribute
// ============================================================================

bool CatalogManager::dropAttributesUnlocked(Oid relOid) {
    auto it = std::remove_if(attributes_.begin(), attributes_.end(),
                             [relOid](const PgAttributeRow& a) {
                                 return a.attrelid == relOid;
                             });
    attributes_.erase(it, attributes_.end());
    return true;
}

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

bool CatalogManager::renameAttribute(
    Oid relOid, const std::string& oldName, const std::string& newName) {
    if (relOid == INVALID_OID || oldName.empty() || newName.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    PgAttributeRow* target = nullptr;
    for (auto& attribute : attributes_) {
        if (attribute.attrelid != relOid) continue;
        if (attribute.attname == newName && attribute.attname != oldName) {
            return false;
        }
        if (attribute.attname == oldName) target = &attribute;
    }
    if (!target) return false;
    target->attname = newName;
    return true;
}

bool CatalogManager::replaceAttributes(
    Oid relOid, const std::vector<PgAttributeRow>& attributes) {
    if (relOid == INVALID_OID ||
        attributes.size() >
            static_cast<size_t>(std::numeric_limits<int16_t>::max())) {
        return false;
    }

    std::set<std::string> names;
    std::set<int16_t> numbers;
    for (const auto& attribute : attributes) {
        if (attribute.attrelid != relOid || attribute.attname.empty() ||
            attribute.attnum <= 0 ||
            !names.insert(attribute.attname).second ||
            !numbers.insert(attribute.attnum).second) {
            return false;
        }
    }
    for (size_t index = 0; index < attributes.size(); ++index) {
        if (numbers.count(static_cast<int16_t>(index + 1)) == 0) {
            return false;
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto classIt = classByOid_.find(relOid);
    if (classIt == classByOid_.end() ||
        classIt->second >= classes_.size()) {
        return false;
    }
    std::vector<PgAttributeRow> replacement;
    replacement.reserve(attributes_.size() + attributes.size());
    for (const auto& current : attributes_) {
        if (current.attrelid != relOid) replacement.push_back(current);
    }
    replacement.insert(
        replacement.end(), attributes.begin(), attributes.end());
    attributes_.swap(replacement);
    classes_[classIt->second].relnatts =
        static_cast<int16_t>(attributes.size());
    return true;
}

bool CatalogManager::remapColumnMetadataAfterDrop(
    Oid relOid, int32_t droppedAttnum) {
    if (relOid == INVALID_OID || droppedAttnum <= 0) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    if (classByOid_.count(relOid) == 0) return false;
    const bool attributeExists = std::any_of(
        attributes_.begin(), attributes_.end(),
        [&](const PgAttributeRow& attribute) {
            return attribute.attrelid == relOid &&
                   attribute.attnum == droppedAttnum;
        });
    if (!attributeExists) return false;

    std::vector<PgDependRow> remappedDependencies;
    remappedDependencies.reserve(depends_.size());
    for (const auto& current : depends_) {
        PgDependRow dependency = current;
        const bool droppedColumnOwnsDependency =
            dependency.classid == PgClassOid_Class &&
            dependency.objid == relOid &&
            dependency.objsubid == droppedAttnum;
        if (droppedColumnOwnsDependency) continue;

        if (dependency.refclassid == PgClassOid_Class &&
            dependency.refobjid == relOid &&
            dependency.refobjsubid == droppedAttnum) {
            // The dependent object must be removed by the DDL caller before
            // catalog positions are compacted. Silently retaining or erasing
            // it would either retarget it or bypass RESTRICT semantics.
            return false;
        }
        if (dependency.classid == PgClassOid_Class &&
            dependency.objid == relOid &&
            dependency.objsubid > droppedAttnum) {
            --dependency.objsubid;
        }
        if (dependency.refclassid == PgClassOid_Class &&
            dependency.refobjid == relOid &&
            dependency.refobjsubid > droppedAttnum) {
            --dependency.refobjsubid;
        }
        remappedDependencies.push_back(std::move(dependency));
    }

    std::vector<PgDescriptionRow> remappedDescriptions;
    remappedDescriptions.reserve(descriptions_.size());
    for (const auto& current : descriptions_) {
        PgDescriptionRow description = current;
        if (description.classoid == PgClassOid_Class &&
            description.objoid == relOid) {
            if (description.objsubid == droppedAttnum) continue;
            if (description.objsubid > droppedAttnum) {
                --description.objsubid;
            }
        }
        remappedDescriptions.push_back(std::move(description));
    }

    depends_ = std::move(remappedDependencies);
    descriptions_ = std::move(remappedDescriptions);
    return true;
}

bool CatalogManager::dropAttributes(Oid relOid) {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropAttributesUnlocked(relOid);
}

// ============================================================================
// pg_type
// ============================================================================

const PgTypeRow* CatalogManager::findTypeUnlocked(Oid oid) const {
    auto it = typeByOid_.find(oid);
    if (it != typeByOid_.end() && it->second < types_.size()) {
        return &types_[it->second];
    }
    return nullptr;
}

Oid CatalogManager::createType(const PgTypeRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgTypeRow r = row;
    r.oid = oid;
    size_t idx = types_.size();
    types_.push_back(r);
    typeByOid_[oid] = idx;
    if (r.typnamespace != INVALID_OID) {
        PgDependRow dep;
        dep.classid = PgClassOid_Type;
        dep.objid = oid;
        dep.objsubid = 0;
        dep.refclassid = PgClassOid_Namespace;
        dep.refobjid = r.typnamespace;
        dep.refobjsubid = 0;
        dep.deptype = 'n';
        depends_.push_back(dep);
    }
    return oid;
}

const PgTypeRow* CatalogManager::findType(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findTypeUnlocked(oid);
}

const PgTypeRow* CatalogManager::findTypeByName(const std::string& name, Oid nspOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findTypeByNameUnlocked(name, nspOid);
}

bool CatalogManager::updateType(Oid oid, const PgTypeRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = typeByOid_.find(oid);
    if (found == typeByOid_.end() || found->second >= types_.size())
        return false;
    PgTypeRow replacement = row;
    replacement.oid = oid;
    types_[found->second] = std::move(replacement);
    return true;
}

bool CatalogManager::dropTypeUnlocked(Oid oid) {
    auto it = typeByOid_.find(oid);
    if (it == typeByOid_.end() || it->second >= types_.size()) return false;
    size_t idx = it->second;
    removeDependRecordsUnlocked(PgClassOid_Type, oid);
    if (idx + 1 < types_.size()) {
        types_[idx] = std::move(types_.back());
        typeByOid_[types_[idx].oid] = idx;
    }
    types_.pop_back();
    typeByOid_.erase(oid);
    enums_.erase(
        std::remove_if(enums_.begin(), enums_.end(),
                       [oid](const PgEnumRow& label) {
                           return label.enumtypid == oid;
                       }),
        enums_.end());
    return true;
}

bool CatalogManager::dropType(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropTypeUnlocked(oid);
}

std::vector<PgTypeRow> CatalogManager::listTypes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return types_;
}

// ============================================================================
// pg_enum
// ============================================================================

bool CatalogManager::replaceEnumLabels(
    Oid typeOid, const std::vector<std::string>& labels) {
    if (typeOid == INVALID_OID || labels.empty()) return false;
    std::set<std::string> distinct;
    for (const auto& label : labels) {
        if (label.find('\0') != std::string::npos ||
            !distinct.insert(label).second) {
            return false;
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const PgTypeRow* type = findTypeUnlocked(typeOid);
    if (!type || type->typtype != 'e') return false;

    std::vector<PgEnumRow> existing;
    for (const auto& label : enums_) {
        if (label.enumtypid == typeOid) existing.push_back(label);
    }
    std::sort(existing.begin(), existing.end(),
              [](const PgEnumRow& left, const PgEnumRow& right) {
                  if (left.enumsortorder != right.enumsortorder)
                      return left.enumsortorder < right.enumsortorder;
                  return left.oid < right.oid;
              });

    std::optional<size_t> renamedPosition;
    if (existing.size() == labels.size()) {
        for (size_t position = 0; position < labels.size(); ++position) {
            if (existing[position].enumlabel == labels[position]) continue;
            if (renamedPosition) {
                renamedPosition.reset();
                break;
            }
            renamedPosition = position;
        }
    }
    std::unordered_map<std::string, Oid> oidByLabel;
    for (const auto& label : existing)
        oidByLabel.emplace(label.enumlabel, label.oid);

    std::vector<PgEnumRow> replacement;
    replacement.reserve(labels.size());
    for (size_t position = 0; position < labels.size(); ++position) {
        PgEnumRow label;
        label.enumtypid = typeOid;
        label.enumsortorder = static_cast<float>(position + 1);
        label.enumlabel = labels[position];
        const auto unchanged = oidByLabel.find(label.enumlabel);
        if (unchanged != oidByLabel.end()) {
            label.oid = unchanged->second;
        } else if (renamedPosition && *renamedPosition == position) {
            label.oid = existing[position].oid;
        } else {
            label.oid = oidGen_->allocate();
        }
        replacement.push_back(std::move(label));
    }

    enums_.erase(
        std::remove_if(enums_.begin(), enums_.end(),
                       [typeOid](const PgEnumRow& label) {
                           return label.enumtypid == typeOid;
                       }),
        enums_.end());
    enums_.insert(enums_.end(), replacement.begin(), replacement.end());
    return true;
}

std::vector<PgEnumRow> CatalogManager::findEnumLabels(Oid typeOid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgEnumRow> result;
    for (const auto& label : enums_) {
        if (label.enumtypid == typeOid) result.push_back(label);
    }
    std::sort(result.begin(), result.end(),
              [](const PgEnumRow& left, const PgEnumRow& right) {
                  if (left.enumsortorder != right.enumsortorder)
                      return left.enumsortorder < right.enumsortorder;
                  return left.oid < right.oid;
              });
    return result;
}

std::vector<PgEnumRow> CatalogManager::listEnumLabels() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PgEnumRow> result = enums_;
    std::sort(result.begin(), result.end(),
              [](const PgEnumRow& left, const PgEnumRow& right) {
                  if (left.enumtypid != right.enumtypid)
                      return left.enumtypid < right.enumtypid;
                  if (left.enumsortorder != right.enumsortorder)
                      return left.enumsortorder < right.enumsortorder;
                  return left.oid < right.oid;
              });
    return result;
}

// ============================================================================
// pg_proc
// ============================================================================

const PgProcRow* CatalogManager::findProcUnlocked(Oid oid) const {
    auto it = procByOid_.find(oid);
    if (it != procByOid_.end() && it->second < procs_.size()) {
        return &procs_[it->second];
    }
    return nullptr;
}

Oid CatalogManager::createProc(const PgProcRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    Oid oid = (row.oid != INVALID_OID) ? row.oid : oidGen_->allocate();
    PgProcRow r = row;
    r.oid = oid;
    size_t idx = procs_.size();
    procs_.push_back(r);
    procByOid_[oid] = idx;
    if (r.pronamespace != INVALID_OID) {
        PgDependRow dep;
        dep.classid = PgClassOid_Proc;
        dep.objid = oid;
        dep.objsubid = 0;
        dep.refclassid = PgClassOid_Namespace;
        dep.refobjid = r.pronamespace;
        dep.refobjsubid = 0;
        dep.deptype = 'n';
        depends_.push_back(dep);
    }
    return oid;
}

const PgProcRow* CatalogManager::findProc(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findProcUnlocked(oid);
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

const PgTypeRow* CatalogManager::findTypeByNameUnlocked(const std::string& name, Oid nspOid) const {
    for (const auto& t : types_) {
        if (t.typname == name && t.typnamespace == nspOid) {
            return &t;
        }
    }
    return nullptr;
}

const PgProcRow* CatalogManager::findProcByNameUnlocked(const std::string& name, Oid nspOid) const {
    for (const auto& p : procs_) {
        if (p.proname == name && p.pronamespace == nspOid) {
            return &p;
        }
    }
    return nullptr;
}

bool CatalogManager::dropProcUnlocked(Oid oid) {
    auto it = procByOid_.find(oid);
    if (it == procByOid_.end() || it->second >= procs_.size()) return false;
    size_t idx = it->second;
    removeDependRecordsUnlocked(PgClassOid_Proc, oid);
    if (idx + 1 < procs_.size()) {
        procs_[idx] = std::move(procs_.back());
        procByOid_[procs_[idx].oid] = idx;
    }
    procs_.pop_back();
    procByOid_.erase(oid);
    return true;
}

bool CatalogManager::dropProc(Oid oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropProcUnlocked(oid);
}

// ============================================================================
// pg_authid
// ============================================================================

const PgAuthIdRow* CatalogManager::findAuthIdUnlocked(Oid oid) const {
    auto it = authIdByOid_.find(oid);
    if (it != authIdByOid_.end() && it->second < authIds_.size()) {
        return &authIds_[it->second];
    }
    return nullptr;
}

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
    return findAuthIdUnlocked(oid);
}

const PgAuthIdRow* CatalogManager::findAuthIdByName(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authIdByName_.find(name);
    if (it != authIdByName_.end()) {
        return findAuthIdUnlocked(it->second);
    }
    return nullptr;
}

std::optional<PgAuthIdRow> CatalogManager::getAuthId(Oid oid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto* row = findAuthIdUnlocked(oid);
    return row ? std::optional<PgAuthIdRow>(*row) : std::nullopt;
}

std::optional<PgAuthIdRow> CatalogManager::getAuthIdByName(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authIdByName_.find(name);
    if (it == authIdByName_.end()) return std::nullopt;
    const auto* row = findAuthIdUnlocked(it->second);
    return row ? std::optional<PgAuthIdRow>(*row) : std::nullopt;
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

bool CatalogManager::updateAuthMember(Oid oid, const PgAuthMembersRow& row) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = authMemberByOid_.find(oid);
    if (it == authMemberByOid_.end() || it->second >= authMembers_.size()) return false;
    authMembers_[it->second] = row;
    authMembers_[it->second].oid = oid;
    return true;
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

std::vector<PgDependRow> CatalogManager::findDependsUnlocked(Oid classid, Oid objid, int32_t objsubid) const {
    std::vector<PgDependRow> result;
    for (const auto& d : depends_) {
        if (d.classid == classid && d.objid == objid &&
            (objsubid < 0 || d.objsubid == objsubid)) {
            result.push_back(d);
        }
    }
    return result;
}

std::vector<PgDependRow> CatalogManager::findDepends(Oid classid, Oid objid, int32_t objsubid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findDependsUnlocked(classid, objid, objsubid);
}

std::vector<PgDependRow> CatalogManager::findRefsUnlocked(Oid refclassid, Oid refobjid, int32_t refobjsubid) const {
    std::vector<PgDependRow> result;
    for (const auto& d : depends_) {
        if (d.refclassid == refclassid && d.refobjid == refobjid &&
            (refobjsubid < 0 || d.refobjsubid == refobjsubid)) {
            result.push_back(d);
        }
    }
    return result;
}

std::vector<PgDependRow> CatalogManager::findRefs(Oid refclassid, Oid refobjid, int32_t refobjsubid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findRefsUnlocked(refclassid, refobjid, refobjsubid);
}

std::vector<PgDependRow> CatalogManager::findAllDependentsUnlocked(Oid refclassid, Oid refobjid) const {
    return findRefsUnlocked(refclassid, refobjid, -1);
}

std::vector<PgDependRow> CatalogManager::findAllDependents(Oid refclassid, Oid refobjid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return findAllDependentsUnlocked(refclassid, refobjid);
}

void CatalogManager::removeDependRecordsUnlocked(Oid classid, Oid objid) {
    auto it = std::remove_if(depends_.begin(), depends_.end(),
                             [classid, objid](const PgDependRow& d) {
                                 return (d.classid == classid && d.objid == objid) ||
                                        (d.refclassid == classid && d.refobjid == objid);
                             });
    depends_.erase(it, depends_.end());
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

void CatalogManager::setDescriptionUnlocked(Oid objoid, Oid classoid, int32_t objsubid,
                                            const std::string& description) {
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

void CatalogManager::setDescription(Oid objoid, Oid classoid, int32_t objsubid,
                                    const std::string& description) {
    std::lock_guard<std::mutex> lock(mutex_);
    setDescriptionUnlocked(objoid, classoid, objsubid, description);
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

bool CatalogManager::setComment(const std::string& objType,
                                const std::string& qualifiedName,
                                const std::string& comment,
                                const std::vector<std::string>& searchPath) {
    QualifiedName qn;
    if (!parseQualifiedName(qualifiedName, qn)) return false;
    std::string type = toLowerString(objType);

    std::lock_guard<std::mutex> lock(mutex_);
    if (type == "schema") {
        auto it = nsByName_.find(qn.name);
        if (it == nsByName_.end()) return false;
        setDescriptionUnlocked(it->second, PgClassOid_Namespace, 0, comment);
        return true;
    }

    if (type == "type") {
        const PgTypeRow* t = nullptr;
        if (!qn.schema.empty()) {
            auto nsIt = nsByName_.find(qn.schema);
            if (nsIt == nsByName_.end()) return false;
            t = findTypeByNameUnlocked(qn.name, nsIt->second);
        } else {
            for (const auto& nsp : searchPath.empty() ? std::vector<std::string>{"public"} : searchPath) {
                auto nsIt = nsByName_.find(nsp);
                if (nsIt == nsByName_.end()) continue;
                t = findTypeByNameUnlocked(qn.name, nsIt->second);
                if (t) break;
            }
        }
        if (!t) return false;
        setDescriptionUnlocked(t->oid, PgClassOid_Type, 0, comment);
        return true;
    }

    if (type == "function" || type == "procedure") {
        const PgProcRow* p = nullptr;
        if (!qn.schema.empty()) {
            auto nsIt = nsByName_.find(qn.schema);
            if (nsIt == nsByName_.end()) return false;
            p = findProcByNameUnlocked(qn.name, nsIt->second);
        } else {
            for (const auto& nsp : searchPath.empty() ? std::vector<std::string>{"public"} : searchPath) {
                auto nsIt = nsByName_.find(nsp);
                if (nsIt == nsByName_.end()) continue;
                p = findProcByNameUnlocked(qn.name, nsIt->second);
                if (p) break;
            }
        }
        if (!p) return false;
        setDescriptionUnlocked(p->oid, PgClassOid_Proc, 0, comment);
        return true;
    }

    // TABLE / COLUMN / INDEX / VIEW / SEQUENCE 等不在 CatalogManager 中解析，
    // 由 StorageEngine 现有路径处理。
    return false;
}

// ============================================================================
// 依赖追踪：CASCADE / RESTRICT 删除计划
// ============================================================================

CatalogManager::DropPlan CatalogManager::planDropUnlocked(Oid classid, Oid objid,
                                                           DropBehavior behavior) const {
    DropPlan plan;

    std::vector<std::pair<Oid, Oid>> toDrop;       // 后序遍历结果
    std::set<std::pair<Oid, Oid>> visited;
    std::set<std::pair<Oid, Oid>> inStack;

    std::function<bool(Oid, Oid)> dfs = [&](Oid cid, Oid oid) -> bool {
        auto key = std::make_pair(cid, oid);
        if (visited.count(key)) return true;
        if (inStack.count(key)) return true; // 循环依赖理论上不应发生
        inStack.insert(key);

        auto deps = findRefsUnlocked(cid, oid, -1);
        for (const auto& d : deps) {
            if (d.deptype == 'p') {
                plan.error = "cannot drop object because it is required by the database system";
                return false;
            }
            // Automatic and internal dependents are implementation details of
            // the referenced object.  They must follow it even for RESTRICT;
            // only independently created (normal/unknown) dependents require
            // the caller to request CASCADE.  Extension membership has the
            // same referenced-object behavior as an internal dependency.
            const bool dropsAutomatically =
                d.deptype == 'a' || d.deptype == 'i' || d.deptype == 'e';
            if (behavior == DropBehavior::Restrict && !dropsAutomatically) {
                plan.error = "cannot drop object because other objects depend on it";
                return false;
            }
            if (!dfs(d.classid, d.objid)) return false;
        }

        inStack.erase(key);
        visited.insert(key);
        toDrop.push_back(key);
        return true;
    };

    if (!dfs(classid, objid)) return plan;

    plan.objectsToDrop = std::move(toDrop);
    return plan;
}

CatalogManager::DropPlan CatalogManager::planDrop(Oid classid, Oid objid,
                                                   DropBehavior behavior) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return planDropUnlocked(classid, objid, behavior);
}

bool CatalogManager::executeDropPlanUnlocked(const DropPlan& plan) {
    for (const auto& obj : plan.objectsToDrop) {
        Oid classid = obj.first;
        Oid objid = obj.second;
        bool ok = false;
        if (classid == PgClassOid_Namespace) {
            ok = dropNamespaceUnlocked(objid);
        } else if (classid == PgClassOid_Class) {
            ok = dropClassUnlocked(objid);
        } else if (classid == PgClassOid_Type) {
            ok = dropTypeUnlocked(objid);
        } else if (classid == PgClassOid_Proc) {
            ok = dropProcUnlocked(objid);
        }
        if (!ok) return false;
    }
    return true;
}

bool CatalogManager::dropObject(Oid classid, Oid objid, DropBehavior behavior,
                                std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    DropPlan plan = planDropUnlocked(classid, objid, behavior);
    if (!plan.ok()) {
        if (error) *error = plan.error;
        return false;
    }
    if (!executeDropPlanUnlocked(plan)) {
        if (error) *error = "drop execution failed";
        return false;
    }
    return true;
}

bool CatalogManager::applyDropPlan(const DropPlan& plan, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!plan.ok()) {
        if (error) *error = plan.error;
        return false;
    }
    if (!executeDropPlanUnlocked(plan)) {
        if (error) *error = "drop execution failed";
        return false;
    }
    return true;
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
    ensureType(650,   "cidr",       -1,   'b', 'I');
    ensureType(651,   "_cidr",      -1,   'b', 'A');
    ensureType(774,   "macaddr8",    8,   'b', 'U');
    ensureType(775,   "_macaddr8",  -1,   'b', 'A');
    ensureType(790,   "money",       8,   'b', 'N');
    ensureType(791,   "_money",     -1,   'b', 'A');
    ensureType(829,   "macaddr",     6,   'b', 'U');
    ensureType(869,   "inet",       -1,   'b', 'I');
    ensureType(1040,  "_macaddr",   -1,   'b', 'A');
    ensureType(1041,  "_inet",      -1,   'b', 'A');
    ensureType(1042,  "bpchar",     -1,   'b', 'S');
    ensureType(1043,  "varchar",    -1,   'b', 'S');
    ensureType(1082,  "date",        4,   'b', 'D');
    ensureType(1083,  "time",        8,   'b', 'D');
    ensureType(1114,  "timestamp",   8,   'b', 'D');
    ensureType(1184,  "timestamptz", 8,   'b', 'D');
    ensureType(1186,  "interval",   16,   'b', 'D');
    ensureType(1266,  "timetz",     12,   'b', 'D');
    ensureType(1560,  "bit",        -1,   'b', 'V');
    ensureType(1561,  "_bit",       -1,   'b', 'A');
    ensureType(1562,  "varbit",     -1,   'b', 'V');
    ensureType(1563,  "_varbit",    -1,   'b', 'A');
    ensureType(1700,  "numeric",    -1,   'b', 'N');
    ensureType(2950,  "uuid",       16,   'b', 'U');
    ensureType(2951,  "_uuid",      -1,   'b', 'A');
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

bool CatalogManager::persistAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    bool ok = true;
    auto persist = [&](const char* name,
                       const std::function<void(std::ostream&)>& writer) {
        if (!writeCatalogFileAtomically(catalogFilePath(name), writer)) ok = false;
    };

    // pg_namespace
    persist("namespace", [&](std::ostream& out) {
        for (const auto& r : namespaces_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.nspname); out << ',';
            writeOid(out, r.nspowner); out << '\n';
        }
    });

    // pg_class
    persist("class", [&](std::ostream& out) {
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
            // Optional extension fields follow the legacy prefix so current
            // readers can still load old rows and old readers can ignore the
            // new tail.
            out << ',' << r.reloftype;
            out << ',' << r.reltablespace;
            out << ',' << r.relpages;
            out << ',' << std::setprecision(
                std::numeric_limits<float>::max_digits10) << r.reltuples;
            out << ',' << r.relallvisible;
            out << ',' << r.reltoastrelid;
            out << ',' << r.relchecks;
            out << ',' << (r.relhasrules ? 't' : 'f');
            out << ',' << (r.relhastriggers ? 't' : 'f');
            out << ',' << (r.relhassubclass ? 't' : 'f');
            out << ',' << (r.relrowsecurity ? 't' : 'f');
            out << ',' << (r.relforcerowsecurity ? 't' : 'f');
            out << ',' << (r.relispopulated ? 't' : 'f');
            out << ',' << r.relreplident;
            out << ',' << (r.relispartition ? 't' : 'f');
            out << ',' << r.relrewrite;
            out << ',' << r.relfrozenxid;
            out << ',' << r.relminmxid;
            out << '\n';
        }
    });

    // pg_attribute
    persist("attribute", [&](std::ostream& out) {
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
            // Keep the original prefix readable by older catalogs and append
            // every PgAttributeRow field that was historically omitted.
            // Character flags are encoded numerically because their unset
            // representation is NUL, which is unsafe in the text format.
            out << ',' << r.attstattarget;
            out << ',' << r.attndims;
            out << ',' << r.attcacheoff;
            out << ',' << (r.attbyval ? 't' : 'f');
            out << ',' << static_cast<unsigned int>(
                static_cast<unsigned char>(r.attidentity));
            out << ',' << static_cast<unsigned int>(
                static_cast<unsigned char>(r.attgenerated));
            out << ',' << (r.attisdropped ? 't' : 'f');
            out << ',' << (r.attislocal ? 't' : 'f');
            out << ',' << r.attinhcount;
            out << ',' << r.attcollation;
            out << '\n';
        }
    });

    // pg_type
    persist("type", [&](std::ostream& out) {
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
    });

    // pg_enum. Labels are hex encoded because PostgreSQL enum labels may
    // contain commas, quotes, backslashes, and newlines while this bootstrap
    // catalog is still line-oriented.
    persist("enum", [&](std::ostream& out) {
        for (const auto& r : enums_) {
            writeOid(out, r.oid); out << ',';
            writeOid(out, r.enumtypid); out << ',';
            out << std::setprecision(
                std::numeric_limits<float>::max_digits10)
                << r.enumsortorder << ',' << hexEncode(r.enumlabel) << '\n';
        }
    });

    // pg_proc
    persist("proc", [&](std::ostream& out) {
        for (const auto& r : procs_) {
            writeOid(out, r.oid); out << ',';
            writeString(out, r.proname); out << ',';
            writeOid(out, r.pronamespace); out << ',';
            out << r.prokind << ',';
            writeOid(out, r.prorettype); out << ',';
            out << r.pronargs << ',';
            writeString(out, r.prosrc); out << ',';
            out << r.provolatile;
            out << '\n';
        }
    });

    // pg_depend
    persist("depend", [&](std::ostream& out) {
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
    });

    // pg_authid
    persist("authid", [&](std::ostream& out) {
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
    });

    // pg_auth_members
    persist("authmembers", [&](std::ostream& out) {
        for (const auto& r : authMembers_) {
            writeOid(out, r.oid); out << ',';
            writeOid(out, r.roleid); out << ',';
            writeOid(out, r.member); out << ',';
            writeOid(out, r.grantor); out << ',';
            out << (r.admin_option ? 't' : 'f');
            out << '\n';
        }
    });

    // pg_description
    persist("description", [&](std::ostream& out) {
        for (const auto& r : descriptions_) {
            writeOid(out, r.objoid); out << ',';
            writeOid(out, r.classoid); out << ',';
            out << r.objsubid << ',';
            writeString(out, r.description);
            out << '\n';
        }
    });

    if (!oidGen_->persist()) ok = false;
    return ok;
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

            // Rows written before the pg_class extension end here. Keep
            // their in-struct defaults; if a tail exists it must be complete.
            if (iss.peek() == ',') {
                iss.ignore(1);
                char hasRules = 'f';
                char hasTriggers = 'f';
                char hasSubclass = 'f';
                char rowSecurity = 'f';
                char forceRowSecurity = 'f';
                char isPopulated = 't';
                char isPartition = 'f';
                if (!readCatalogCsvField(iss, r.reloftype) ||
                    !readCatalogCsvField(iss, r.reltablespace) ||
                    !readCatalogCsvField(iss, r.relpages) ||
                    !readCatalogCsvField(iss, r.reltuples) ||
                    !readCatalogCsvField(iss, r.relallvisible) ||
                    !readCatalogCsvField(iss, r.reltoastrelid) ||
                    !readCatalogCsvField(iss, r.relchecks) ||
                    !readCatalogCsvField(iss, hasRules) ||
                    !readCatalogCsvField(iss, hasTriggers) ||
                    !readCatalogCsvField(iss, hasSubclass) ||
                    !readCatalogCsvField(iss, rowSecurity) ||
                    !readCatalogCsvField(iss, forceRowSecurity) ||
                    !readCatalogCsvField(iss, isPopulated) ||
                    !readCatalogCsvField(iss, r.relreplident) ||
                    !readCatalogCsvField(iss, isPartition) ||
                    !readCatalogCsvField(iss, r.relrewrite) ||
                    !readCatalogCsvField(iss, r.relfrozenxid) ||
                    !readCatalogCsvLastField(iss, r.relminmxid) ||
                    (hasRules != 't' && hasRules != 'f') ||
                    (hasTriggers != 't' && hasTriggers != 'f') ||
                    (hasSubclass != 't' && hasSubclass != 'f') ||
                    (rowSecurity != 't' && rowSecurity != 'f') ||
                    (forceRowSecurity != 't' && forceRowSecurity != 'f') ||
                    (isPopulated != 't' && isPopulated != 'f') ||
                    (isPartition != 't' && isPartition != 'f') ||
                    (r.relreplident != 'd' && r.relreplident != 'n' &&
                     r.relreplident != 'f' && r.relreplident != 'i')) {
                    continue;
                }
                r.relhasrules = hasRules == 't';
                r.relhastriggers = hasTriggers == 't';
                r.relhassubclass = hasSubclass == 't';
                r.relrowsecurity = rowSecurity == 't';
                r.relforcerowsecurity = forceRowSecurity == 't';
                r.relispopulated = isPopulated == 't';
                r.relispartition = isPartition == 't';
            }
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

            // Attribute rows written before the extension end at attalign.
            // Preserve the in-struct defaults for those rows.  A present tail
            // must be complete so a torn catalog record cannot be accepted as
            // a subtly altered column definition.
            if (iss.peek() == ',') {
                iss.ignore(1);
                char byValue = 'f';
                unsigned int identity = 0;
                unsigned int generated = 0;
                char isDropped = 'f';
                char isLocal = 't';
                if (!readCatalogCsvField(iss, r.attstattarget) ||
                    !readCatalogCsvField(iss, r.attndims) ||
                    !readCatalogCsvField(iss, r.attcacheoff) ||
                    !readCatalogCsvField(iss, byValue) ||
                    !readCatalogCsvField(iss, identity) ||
                    !readCatalogCsvField(iss, generated) ||
                    !readCatalogCsvField(iss, isDropped) ||
                    !readCatalogCsvField(iss, isLocal) ||
                    !readCatalogCsvField(iss, r.attinhcount) ||
                    !readCatalogCsvLastField(iss, r.attcollation) ||
                    (byValue != 't' && byValue != 'f') ||
                    (isDropped != 't' && isDropped != 'f') ||
                    (isLocal != 't' && isLocal != 'f') ||
                    (identity != 0 && identity != 'a' && identity != 'd') ||
                    (generated != 0 && generated != 's' &&
                     generated != 'v')) {
                    continue;
                }
                r.attbyval = byValue == 't';
                r.attidentity = static_cast<char>(identity);
                r.attgenerated = static_cast<char>(generated);
                r.attisdropped = isDropped == 't';
                r.attislocal = isLocal == 't';
            }
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

    // pg_enum
    {
        std::ifstream in(catalogFilePath("enum"));
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            PgEnumRow r;
            std::string encodedLabel;
            const bool prefixValid =
                readCatalogCsvField(iss, r.oid) &&
                readCatalogCsvField(iss, r.enumtypid) &&
                readCatalogCsvField(iss, r.enumsortorder);
            const bool labelRead = prefixValid &&
                static_cast<bool>(std::getline(iss, encodedLabel));
            if (!prefixValid || (!labelRead && !iss.eof()) ||
                r.oid == INVALID_OID || r.enumtypid == INVALID_OID ||
                !std::isfinite(r.enumsortorder) ||
                !hexDecode(encodedLabel, r.enumlabel)) {
                continue;
            }
            enums_.push_back(std::move(r));
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
            if (iss.peek() == ',') {
                iss.ignore(1);
                iss >> r.provolatile;
            } else {
                r.provolatile = 'v';
            }
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
    classByName_.clear();
    for (size_t i = 0; i < classes_.size(); ++i) {
        classByOid_[classes_[i].oid] = i;
        classByName_[classNameKey(classes_[i].relnamespace, classes_[i].relname)] = i;
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
