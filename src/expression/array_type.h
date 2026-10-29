#pragma once
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include "expression/regtype_cast.h"

namespace dbms::array_detail {
inline bool isArray(const std::string& type) {
    return type.size()>=2 && type.compare(type.size()-2,2,"[]")==0;
}
inline std::string elementType(const std::string& type) {
    return ExprHelper::canonicalResultTypeName(isArray(type) ? type.substr(0,type.size()-2) : type);
}
inline std::string commonElement(const std::vector<std::string>& inputs) {
    if(inputs.empty())throw DbError("42P18","cannot determine type of empty array");
    std::vector<std::string> elements;
    bool arrays=false, scalars=false;
    for(const auto& type:inputs){
        if(isArray(type)) arrays=true;
        else if(!type.empty() && type!="unknown") scalars=true;
        elements.push_back(elementType(type));
    }
    if(arrays&&scalars)throw DbError("42804","ARRAY scalar and array element types cannot be matched");
    std::string result,error;
    if(!ExprHelper::resolveValuesResultType(elements,result,error))throw DbError("42804","ARRAY "+error);
    return result;
}
// Contextual ARRAY casts follow explicit element casts, not an arbitrary
// passthrough cast. Unknown Const input is checked by the primitive codec.
inline void checkExplicitElementCast(const std::string& sourceRaw,const std::string& targetRaw) {
    const auto castElement=[](const std::string& raw) {
        const auto type=elementType(raw);
        CatalogManager::QualifiedName name;
        if(CatalogManager::parseQualifiedName(type,name,true) &&
           (name.schema.empty() || name.schema=="pg_catalog")) {
            // Only these exact builtin catalog identities own the BIT cast
            // edges below. Quoted case/custom schemas must not inherit them.
            if(name.name=="bit")return std::string("bit");
            if(name.name=="varbit")return std::string("bit varying");
            if(name.name=="int4")return std::string("integer");
            if(name.name=="int8")return std::string("bigint");
        }
        return type;
    };
    const auto source=castElement(sourceRaw),target=castElement(targetRaw);
    if(source=="regtype" || target=="regtype") {
        regtype_cast_detail::validate(source,target);
        return;
    }
    if(source.empty() || source=="unknown" || source==target)return;
    const auto* from=TypeRegistry::instance().findType(source);
    const auto* to=TypeRegistry::instance().findType(target);
    if(from&&to){
        if(from->category==TypeCategory::String || to->category==TypeCategory::String)return;
        if(from->category==TypeCategory::Numeric && to->category==TypeCategory::Numeric)return;
        if(from->category==TypeCategory::BitString && to->category==TypeCategory::BitString)return;
        // pg_cast has BIT <-> INT4/INT8, not INT2 or VARBIT numeric casts.
        // Check declarations even for SQL NULL; the scalar codec owns data,
        // explicit width adjustment and integer overflow after admission.
        if((target=="bit" && (source=="integer" || source=="bigint")) ||
           (source=="bit" && (target=="integer" || target=="bigint")))return;
        if(from->category==TypeCategory::DateTime && to->category==TypeCategory::DateTime)return;
        if((source=="boolean" && target=="integer") || (source=="integer" && target=="boolean"))return;
    }
    throw DbError("42846","cannot cast type "+source+" to "+target);
}
} // namespace dbms::array_detail
