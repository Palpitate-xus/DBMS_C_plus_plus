#pragma once
#include "catalog/catalog.h"
#include "parser/ast.h"
#include "parser/query_binding.h"
#include "common/DbError.h"
#include <functional>

namespace dbms {
// A metadata-only admission probe for FROM trees. CTE names/aliases are not
// source identities: the caller must confirm the actual bound sourceRanges.
inline bool namespaceCatalogSourceCandidate(const Stmt* root) {
    std::function<bool(const Stmt*,size_t)> statement;
    std::function<bool(const FromItem*,size_t)> source;
    source=[&](const FromItem* item,size_t depth) {
        if(!item)return false;
        if(depth>128)throw DbError("54001","catalog source nesting limit exceeded");
        if(item->type==FromItem::Type::Table) {
            CatalogManager::QualifiedName name;
            return CatalogManager::parseQualifiedName(item->tableName,name,true) &&
                name.name=="pg_namespace" && (name.schema.empty() || name.schema=="pg_catalog");
        }
        return statement(item->subquery.get(),depth+1) ||
            source(item->left.get(),depth+1) || source(item->right.get(),depth+1);
    };
    statement=[&](const Stmt* node,size_t depth) {
        if(!node)return false;
        if(depth>128)throw DbError("54001","catalog query nesting limit exceeded");
        if(const auto* envelope=dynamic_cast<const WithStmt*>(node)) {
            for(const auto& cte:envelope->ctes)if(statement(cte.query.get(),depth+1))return true;
            return statement(envelope->statement.get(),depth+1);
        }
        const auto* select=dynamic_cast<const SelectStmt*>(node);
        if(!select)return false;
        for(const auto& cte:select->ctes)if(statement(cte.query.get(),depth+1))return true;
        return source(select->fromClause.get(),depth+1) ||
            statement(select->setOpLhs.get(),depth+1) || statement(select->setOpRhs.get(),depth+1);
    };
    return statement(root,0);
}

inline bool ownsNamespaceCatalogSource(const PreparedQuery& query) {
    for(const auto& range:query.sourceRanges)
        if(range.relationSchema=="pg_catalog" && range.relationName=="pg_namespace" &&
           !range.cteStatement && !range.viewQuery)return true;
    return false;
}
} // namespace dbms
