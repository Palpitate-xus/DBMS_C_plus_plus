#include "systables.h"
#include <sstream>
#include <unordered_map>

namespace dbms {

static const std::unordered_map<std::string, Oid> kBuiltinTypeMap = {
    {"bool", 16}, {"boolean", 16},
    {"bytea", 17},
    {"bytea[]", 1001},
    {"char", 18},
    {"name", 19},
    {"name[]", 1003},
    {"bigint", 20}, {"int8", 20},
    {"bigint[]", 1016}, {"int8[]", 1016},
    {"smallint", 21}, {"int2", 21},
    {"smallint[]", 1005}, {"int2[]", 1005},
    {"integer", 23}, {"int", 23}, {"int4", 23},
    {"integer[]", 1007}, {"int[]", 1007}, {"int4[]", 1007},
    {"regproc", 24},
    {"text", 25},
    {"text[]", 1009},
    {"oid", 26},
    {"oid[]", 1028},
    {"tid", 27},
    {"xid", 28},
    {"cid", 29},
    {"oidvector", 30},
    {"real", 700}, {"float4", 700},
    {"real[]", 1021}, {"float4[]", 1021},
    {"double precision", 701}, {"float8", 701}, {"float", 701},
    {"double precision[]", 1022}, {"float8[]", 1022}, {"float[]", 1022},
    {"bpchar", 1042}, {"char", 1042},
    {"bpchar[]", 1014}, {"char[]", 1014},
    {"varchar", 1043}, {"character varying", 1043},
    {"varchar[]", 1015}, {"character varying[]", 1015},
    {"date", 1082},
    {"date[]", 1182},
    {"time", 1083},
    {"time[]", 1183},
    {"timestamp", 1114}, {"timestamp without time zone", 1114},
    {"timestamp[]", 1115}, {"timestamp without time zone[]", 1115},
    {"timestamptz", 1184}, {"timestamp with time zone", 1184},
    {"timestamptz[]", 1185}, {"timestamp with time zone[]", 1185},
    {"interval", 1186},
    {"interval[]", 1187},
    {"timetz", 1266}, {"time with time zone", 1266},
    {"numeric", 1700}, {"decimal", 1700},
    {"numeric[]", 1231}, {"decimal[]", 1231},
    {"regtype", 2206},
    {"void", 2278},
    {"uuid", 2950},
    {"uuid[]", 2951},
    {"json", 114}, {"json[]", 199},
    {"jsonb", 3802},
    {"jsonb[]", 3807},
    {"xml", 142}, {"xml[]", 143},
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

std::string PgEnumRow::toString() const {
    std::ostringstream oss;
    oss << "PgEnum(oid=" << oid << ", type=" << enumtypid
        << ", order=" << enumsortorder << ", label=" << enumlabel << ")";
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
