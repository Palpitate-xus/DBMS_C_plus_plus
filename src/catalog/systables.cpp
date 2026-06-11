#include "systables.h"
#include <sstream>

namespace dbms {

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

} // namespace dbms
