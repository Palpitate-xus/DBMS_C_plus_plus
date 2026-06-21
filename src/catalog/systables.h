#pragma once

#include "dbms_defs.h"
#include <string>
#include <vector>
#include <cstdint>

namespace dbms {

// ============================================================================
// 系统表 OID 常量（对标 PostgreSQL bootstrap OIDs）
// ============================================================================

constexpr Oid PgClassOid_Namespace = 2615;   // pg_namespace
constexpr Oid PgClassOid_Class     = 1259;   // pg_class
constexpr Oid PgClassOid_Attribute = 1249;   // pg_attribute
constexpr Oid PgClassOid_Type      = 1247;   // pg_type
constexpr Oid PgClassOid_Proc      = 1255;   // pg_proc
constexpr Oid PgClassOid_Depend    = 2608;   // pg_depend

// ============================================================================
// 系统表行格式定义（PostgreSQL 18 兼容子集）
//
// 设计原则：
//   1. 字段名和类型对标 PG 系统表
//   2. 先用基础 C++ 类型，后续可替换为 Datum/HeapTuple
//   3. 每个结构提供 toString() 用于调试
// ============================================================================

// ---------------------------------------------------------------------------
// pg_namespace — 命名空间（schema）
// ---------------------------------------------------------------------------
struct PgNamespaceRow {
    Oid      oid = INVALID_OID;
    std::string nspname;       // schema 名称
    Oid      nspowner = INVALID_OID; // 所有者
    // nspacl 省略（Phase 7 再补 ACL）

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_class — 表、索引、序列、视图、物化视图等
// ---------------------------------------------------------------------------
struct PgClassRow {
    Oid      oid = INVALID_OID;
    std::string relname;       // 对象名
    Oid      relnamespace = INVALID_OID; // 所属 schema
    Oid      reltype = INVALID_OID;      // 对应 pg_type 条目（表/复合类型）
    Oid      reloftype = INVALID_OID;    // 底层类型（typed table）
    Oid      relowner = INVALID_OID;     // 所有者
    Oid      relam = INVALID_OID;        // access method（索引）
    Oid      relfilenode = INVALID_OID;  // 磁盘文件名对应的 OID
    Oid      reltablespace = INVALID_OID;// 表空间
    int32_t  relpages = 0;     // 页数（估计）
    float    reltuples = 0;    // 行数（估计）
    int32_t  relallvisible = 0;// 所有行都可见的页数
    Oid      reltoastrelid = INVALID_OID; // TOAST 表
    bool     relhasindex = false;
    bool     relisshared = false;
    char     relpersistence = 'p'; // p=permanent, u=unlogged, t=temp
    char     relkind = 'r';    // r=table, i=index, S=sequence, v=view,
                               // m=matview, c=composite, t=TOAST, f=foreign
    int16_t  relnatts = 0;     // 用户可见列数
    int16_t  relchecks = 0;    // CHECK 约束数
    bool     relhasrules = false;
    bool     relhastriggers = false;
    bool     relhassubclass = false;
    bool     relrowsecurity = false;
    bool     relforcerowsecurity = false;
    bool     relispopulated = true; // 物化视图是否已填充
    char     relreplident = 'd'; // d=default, n=nothing, f=full, i=index
    bool     relispartition = false;
    Oid      relrewrite = INVALID_OID;
    uint32_t relfrozenxid = 0;
    uint32_t relminmxid = 0;

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_attribute — 列/属性定义
// ---------------------------------------------------------------------------
struct PgAttributeRow {
    Oid      attrelid = INVALID_OID;  // 所属表 OID
    std::string attname;              // 列名
    Oid      atttypid = INVALID_OID;  // 类型 OID
    int32_t  attstattarget = -1;      // ANALYZE 统计目标
    int16_t  attlen = -1;             // 类型长度（-1 = varlena）
    int16_t  attnum = 0;              // 列号（>0 用户列，<0 系统列）
    int32_t  attndims = 0;            // 数组维度
    int32_t  attcacheoff = -1;        // 缓存偏移
    int32_t  atttypmod = -1;          // 类型修饰符（如 varchar(255)）
    bool     attbyval = false;        // 是否按值传递
    char     attstorage = 'p';        // p=plain, e=external, m=main, x=extended
    char     attalign = 'i';          // 对齐方式：c=char, s=short, i=int, d=double
    bool     attnotnull = false;
    bool     atthasdef = false;       // 是否有默认值
    char     attidentity = '\0';      // a=always, d=default
    char     attgenerated = '\0';     // s=stored
    bool     attisdropped = false;
    bool     attislocal = true;
    int32_t  attinhcount = 0;         // 直接继承祖先数
    Oid      attcollation = INVALID_OID;

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_type — 数据类型
// ---------------------------------------------------------------------------
struct PgTypeRow {
    Oid      oid = INVALID_OID;
    std::string typname;
    Oid      typnamespace = INVALID_OID;
    Oid      typowner = INVALID_OID;
    int16_t  typlen = -1;
    bool     typbyval = false;
    char     typtype = 'b';    // b=base, c=composite, d=domain, e=enum,
                               // p=pseudo, r=range, m=multirange
    char     typcategory = 'U'; // 见 PG 文档
    bool     typispreferred = false;
    bool     typisdefined = true;
    char     typdelim = ',';
    Oid      typrelid = INVALID_OID;   // 复合类型对应的 pg_class
    Oid      typelem = INVALID_OID;    // 数组元素类型
    Oid      typarray = INVALID_OID;   // 对应的数组类型
    Oid      typinput = INVALID_OID;   // 输入函数
    Oid      typoutput = INVALID_OID;  // 输出函数
    Oid      typreceive = INVALID_OID;
    Oid      typsend = INVALID_OID;
    Oid      typmodin = INVALID_OID;
    Oid      typmodout = INVALID_OID;
    Oid      typanalyze = INVALID_OID;
    char     typalign = 'i';
    char     typstorage = 'p';
    bool     typnotnull = false;
    Oid      typbasetype = INVALID_OID; // domain 的基础类型
    int32_t  typtypmod = -1;
    int32_t  typndims = 0;
    Oid      typcollation = INVALID_OID;

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_proc — 函数/过程
// ---------------------------------------------------------------------------
struct PgProcRow {
    Oid      oid = INVALID_OID;
    std::string proname;
    Oid      pronamespace = INVALID_OID;
    Oid      proowner = INVALID_OID;
    Oid      prolang = INVALID_OID;    // 语言（SQL=14, plpgsql=16393...）
    float    procost = 100;
    float    prorows = 0;
    Oid      provariadic = INVALID_OID;
    char     prokind = 'f';    // f=function, p=procedure, a=aggregate, w=window
    bool     prosecdef = false;
    bool     proleakproof = false;
    bool     proisstrict = false;
    bool     proretset = false;
    char     provolatile = 'v'; // v=volatile, s=stable, i=immutable
    char     proparallel = 'u'; // u=unsafe, r=restricted, s=safe
    int16_t  pronargs = 0;
    int16_t  pronargdefaults = 0;
    Oid      prorettype = INVALID_OID;
    std::vector<Oid> proargtypes;     // 参数类型 OID
    std::vector<Oid> proallargtypes;  // 全部参数类型（含 OUT）
    std::vector<char> proargmodes;    // i=in, o=out, b=inout, v=variadic, t=table
    std::vector<std::string> proargnames;
    std::string prosrc;               // 函数体源码
    std::string probin;               // 共享库路径（C 函数）

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_depend — 对象依赖关系
// ---------------------------------------------------------------------------
struct PgDependRow {
    Oid      classid = INVALID_OID;    // 被依赖对象所在系统表 OID
    Oid      objid = INVALID_OID;      // 被依赖对象 OID
    int32_t  objsubid = 0;             // 子对象号（列号等）
    Oid      refclassid = INVALID_OID; // 依赖对象所在系统表 OID
    Oid      refobjid = INVALID_OID;   // 依赖对象 OID
    int32_t  refobjsubid = 0;
    char     deptype = 'n';            // n=normal, a=auto, i=internal, e=extension, p=pin

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_authid — 角色/用户认证信息
// ---------------------------------------------------------------------------
struct PgAuthIdRow {
    Oid      oid = INVALID_OID;
    std::string rolname;          // 角色名
    bool     rolsuper = false;    // 超级用户
    bool     rolinherit = true;   // 自动继承成员角色的权限
    bool     rolcreaterole = false; // 可创建角色
    bool     rolcreatedb = false; // 可创建数据库
    bool     rolcanlogin = true;  // 可登录
    bool     rolreplication = false; // 可复制
    bool     rolbypassrls = false;   // 可绕过 RLS
    int32_t  rolconnlimit = -1;   // 最大并发连接数（-1 = 无限制）
    std::string rolpassword;      // 密码哈希
    std::string rolvaliduntil;    // 密码有效期（ISO 时间字符串，空 = 永久）

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_auth_members — 角色成员关系
// ---------------------------------------------------------------------------
struct PgAuthMembersRow {
    Oid      oid = INVALID_OID;
    Oid      roleid = INVALID_OID;   // 被赋予成员资格的角色
    Oid      member = INVALID_OID;   // 成员角色
    Oid      grantor = INVALID_OID;  // 授权者
    bool     admin_option = false;   // 是否有 ADMIN OPTION

    std::string toString() const;
};

// ---------------------------------------------------------------------------
// pg_description — 对象注释（COMMENT ON）
// ---------------------------------------------------------------------------
struct PgDescriptionRow {
    Oid      objoid = INVALID_OID;      // 对象 OID
    Oid      classoid = INVALID_OID;    // 对象所在系统表 OID
    int32_t  objsubid = 0;              // 子对象号（0 = 对象本身，>0 = 列号）
    std::string description;            // 注释文本

    std::string toString() const;
};

// ============================================================================
// 常用工具函数
// ============================================================================

// Map a canonical/alias type name to its standard PostgreSQL bootstrap OID.
// Returns INVALID_OID if the name is not a known built-in type.
Oid mapBuiltinTypeNameToOid(const std::string& typeName);

} // namespace dbms
