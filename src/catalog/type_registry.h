#pragma once

#include "interfaces/table_schema.h"
#include <map>
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// TypeRegistry — 集中式类型注册表
//
// 职责：
//   1. 注册所有内置类型及其别名（如 int/int4/integer -> integer）
//   2. 解析类型修饰符（如 VARCHAR(255)、NUMERIC(10,2)）
//   3. 根据类型名与修饰符填充 Column 的 dataType / dsize / isVariableLength
//   4. 为后续 CatalogManager::bootstrapSystemTypes() 提供元数据来源
//
// 设计原则：
//   - 单例，启动时 bootstrap() 注册全部类型
//   - 不保存状态，线程安全只读（构造后不再修改）
//   - 优先兼容现有 Column 构造函数语义，再扩展 Phase 4 新类型
// ============================================================================

enum class TypeCategory {
    Boolean,
    Numeric,
    String,
    Binary,
    DateTime,
    Enum,
    Geometric,
    Network,
    BitString,
    TextSearch,
    UUID,
    XML,
    JSON,
    JSONPath,
    Array,
    Composite,
    Range,
    Domain,
    Pseudo,
    Unknown
};

struct TypeEntry {
    std::string canonicalName;   // 规范名，如 "character varying"
    int16_t     typlen = -1;     // 固定长度字节数；-1 表示变长
    char        typalign = 'i';  // 对齐：c=char, s=short, i=int, d=double
    char        typstorage = 'p';// p=plain, e=external, m=main, x=extended
    TypeCategory category = TypeCategory::Unknown;
    bool        hasTypeMod = false;      // 是否接受 (n) / (p,s) 修饰符
    int32_t     defaultTypeMod = -1;     // 未指定修饰符时的默认值
    size_t      maxLength = 0;           // 变长类型最大允许长度（0 表示无限制或固定）
    bool        isArrayBase = false;     // 是否可作为数组基类型
};

struct TypeModResult {
    int32_t typmod = -1;          // PostgreSQL 风格 typmod；无修饰符 = -1
    size_t  dsize = 0;            // 列存储宽度
    bool    isVariableLength = false;
    std::string error;            // 空表示成功
    bool    ok() const { return error.empty(); }
};

class TypeRegistry {
public:
    static TypeRegistry& instance();

    // 注册一个规范类型及其别名
    void registerType(const TypeEntry& entry, const std::vector<std::string>& aliases = {});

    // 将类型名归一化为规范名；未知类型返回空字符串
    std::string normalizeTypeName(const std::string& name) const;

    // 根据规范名或别名查找类型条目；未知返回 nullptr
    const TypeEntry* findType(const std::string& canonicalOrAlias) const;

    // 计算类型修饰符对存储宽度的影响
    TypeModResult applyTypeMods(const std::string& canonicalName,
                                const std::vector<std::string>& mods) const;

    // 主入口：用类型名与修饰符填充 Column 类型相关字段
    // 返回空字符串表示成功，否则返回错误信息
    std::string resolveColumnType(Column& col,
                                  const std::string& typeName,
                                  const std::vector<std::string>& typeMods,
                                  bool isArray) const;

    // 校验已构造 Column 的类型字段是否合法；必要时补齐 dsize
    // 返回空字符串表示成功，否则返回错误信息
    std::string validateColumn(Column& col) const;

    // 注册所有内置类型；在进程启动时调用一次
    void bootstrap();

private:
    TypeRegistry() = default;

    std::map<std::string, std::string> aliasToCanonical_; // 小写别名 -> 规范名
    std::map<std::string, TypeEntry>   canonicalToEntry_;

    // 注册一组 Phase 4 要求的内置类型
    void registerNumericTypes();
    void registerStringTypes();
    void registerBinaryTypes();
    void registerDateTimeTypes();
    void registerGeometricTypes();
    void registerNetworkTypes();
    void registerBitTypes();
    void registerTextSearchTypes();
    void registerUuidTypes();
    void registerXmlJsonTypes();
    void registerRangeTypes();
    void registerEnumCompositeDomainPseudoTypes();
};

} // namespace dbms
