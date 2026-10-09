#pragma once
#include "catalog/catalog.h"
#include "parser/query_binding.h"

namespace dbms {
// Columns with actual owned PgTypeRow storage. The additional PostgreSQL
// typsubscript/default/defaultbin/ACL fields are not fabricated here.
inline const QueryRowDescriptor& ownedTypeCatalogDescriptor() {
    static const QueryRowDescriptor columns={
        {"oid","oid"},{"typname","name"},{"typnamespace","oid"},{"typowner","oid"},
        {"typlen","smallint"},{"typbyval","boolean"},{"typtype","\"char\""},
        {"typcategory","\"char\""},{"typispreferred","boolean"},{"typisdefined","boolean"},
        {"typdelim","\"char\""},{"typrelid","oid"},{"typelem","oid"},{"typarray","oid"},
        {"typinput","regproc"},{"typoutput","regproc"},{"typreceive","regproc"},
        {"typsend","regproc"},{"typmodin","regproc"},{"typmodout","regproc"},
        {"typanalyze","regproc"},{"typalign","\"char\""},{"typstorage","\"char\""},
        {"typnotnull","boolean"},{"typbasetype","oid"},{"typtypmod","integer"},
        {"typndims","integer"},{"typcollation","oid"}
    };
    return columns;
}

inline std::vector<ExprValue> ownedTypeCatalogCells(const PgTypeRow& row) {
    const std::vector<std::string> values={
        std::to_string(row.oid),row.typname,std::to_string(row.typnamespace),std::to_string(row.typowner),
        std::to_string(row.typlen),row.typbyval?"t":"f",std::string(1,row.typtype),
        std::string(1,row.typcategory),row.typispreferred?"t":"f",row.typisdefined?"t":"f",
        std::string(1,row.typdelim),std::to_string(row.typrelid),std::to_string(row.typelem),
        std::to_string(row.typarray),std::to_string(row.typinput),std::to_string(row.typoutput),
        std::to_string(row.typreceive),std::to_string(row.typsend),std::to_string(row.typmodin),
        std::to_string(row.typmodout),std::to_string(row.typanalyze),std::string(1,row.typalign),
        std::string(1,row.typstorage),row.typnotnull?"t":"f",std::to_string(row.typbasetype),
        std::to_string(row.typtypmod),std::to_string(row.typndims),std::to_string(row.typcollation)
    };
    const auto& descriptor=ownedTypeCatalogDescriptor();
    std::vector<ExprValue> cells;
    cells.reserve(descriptor.size());
    for(size_t i=0;i<descriptor.size();++i)cells.emplace_back(descriptor[i].type,values[i],false);
    return cells;
}
} // namespace dbms
