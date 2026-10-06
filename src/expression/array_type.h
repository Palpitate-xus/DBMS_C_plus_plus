#pragma once
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"

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
    const auto source=elementType(sourceRaw),target=elementType(targetRaw);
    if(source.empty() || source=="unknown" || source==target)return;
    const auto* from=TypeRegistry::instance().findType(source);
    const auto* to=TypeRegistry::instance().findType(target);
    if(from&&to){
        if(from->category==TypeCategory::String || to->category==TypeCategory::String)return;
        if(from->category==TypeCategory::Numeric && to->category==TypeCategory::Numeric)return;
        if(from->category==TypeCategory::DateTime && to->category==TypeCategory::DateTime)return;
        if((source=="boolean" && target=="integer") || (source=="integer" && target=="boolean"))return;
    }
    throw DbError("42846","cannot cast type "+source+" to "+target);
}
} // namespace dbms::array_detail
