#include "systables.h"
#include <sstream>
#include <unordered_map>

namespace dbms {

static const std::unordered_map<std::string, Oid> kBuiltinTypeMap = {
    {"bool", 16}, {"boolean", 16},
    {"bytea", 17},
    {"char", 18},
    {"name", 19},
    {"bigint", 20}, {"int8", 20},
    {"smallint", 21}, {"int2", 21},
    {"integer", 23}, {"int", 23}, {"int4", 23},
    {"regproc", 24},
    {"text", 25},
    {"oid", 26},
    {"tid", 27},
    {"xid", 28},
    {"cid", 29},
    {"oidvector", 30},
    {"real", 700}, {"float4", 700},
    {"double precision", 701}, {"float8", 701}, {"float", 701},
    {"bpchar", 1042}, {"char", 1042},
    {"varchar", 1043}, {"character varying", 1043},
    {"date", 1082},
    {"time", 1083},
    {"timestamp", 1114}, {"timestamp without time zone", 1114},
    {"timestamptz", 1184}, {"timestamp with time zone", 1184},
    {"interval", 1186},
    {"timetz", 1266}, {"time with time zone", 1266},
    {"numeric", 1700}, {"decimal", 1700},
    {"uuid", 2950},
    {"jsonb", 3802},
};

Oid mapBuiltinTypeNameToOid(const std::string& typeName) {
    auto it = kBuiltinTypeMap.find(typeName);
    if (it != kBuiltinTypeMap.end()) return it->second;
    return INVALID_OID;
}

std::string PgNamespaceRow::toString() const {
    std::ostringstream oss;
    oss << "PgNamespace(oid=" << oid << ", name=" << nspname << ", owner=" << nspowner << ")";
    return oss.str();
}

std::string PgClassRow::toString() const {
    std::ostringstream oss;
    oss << "PgClass(oid=" << oid << ", name=" << relname
        << ", ns=" << relnamespace << ", kind=" << relkind << ")";
    return oss.str();
}

std::string PgAttributeRow::toString() const {
    std::ostringstream oss;
    oss << "PgAttribute(rel=" << attrelid << ", num=" << attnum
        << ", name=" << attname << ", type=" << atttypid << ")";
    return oss.str();
}

std::string PgTypeRow::toString() const {
    std::ostringstream oss;
    oss << "PgType(oid=" << oid << ", name=" << typname
        << ", category=" << typcategory << ", len=" << typlen << ")";
    return oss.str();
}

std::string PgProcRow::toString() const {
    std::ostringstream oss;
    oss << "PgProc(oid=" << oid << ", name=" << proname
        << ", kind=" << prokind << ", ret=" << prorettype << ")";
    return oss.str();
}

std::string PgDependRow::toString() const {
    std::ostringstream oss;
    oss << "PgDepend(class=" << classid << ", obj=" << objid
        << ", refclass=" << refclassid << ", refobj=" << refobjid
        << ", type=" << deptype << ")";
    return oss.str();
}

std::string PgAuthIdRow::toString() const {
    std::ostringstream oss;
    oss << "PgAuthId(oid=" << oid << ", name=" << rolname
        << ", super=" << rolsuper << ", login=" << rolcanlogin << ")";
    return oss.str();
}

std::string PgAuthMembersRow::toString() const {
    std::ostringstream oss;
    oss << "PgAuthMembers(oid=" << oid << ", role=" << roleid
        << ", member=" << member << ", grantor=" << grantor << ")";
    return oss.str();
}

std::string PgDescriptionRow::toString() const {
    std::ostringstream oss;
    oss << "PgDescription(obj=" << objoid << ", class=" << classoid
        << ", sub=" << objsubid << ", desc=" << description << ")";
    return oss.str();
}

} // namespace dbms
