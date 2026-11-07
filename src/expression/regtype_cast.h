#pragma once
#include "expression/common_type.h"

namespace dbms::regtype_cast_detail {
inline bool reference(const std::string& type) {
    return type=="regtype" || type=="regclass";
}
inline void validateScalar(const std::string& source,const std::string& target) {
    static const std::set<std::string> strings={"unknown","text","varchar","bpchar","name"};
    if(reference(target) && !strings.count(source) && source!=target && source!="oid" &&
       source!="smallint" && source!="integer" && source!="bigint")
        throw DbError("42846","cannot cast type "+source+" to "+target);
    if(reference(source) && !strings.count(target) && target!=source && target!="oid" &&
       target!="integer" && target!="bigint")
        throw DbError("42846","cannot cast type "+source+" to "+target);
}
inline void validate(const std::string& sourceRaw,const std::string& targetRaw) {
    const auto source=common_type_detail::canonical(sourceRaw),target=common_type_detail::canonical(targetRaw);
    const bool fromArray=common_type_detail::array(source),toArray=common_type_detail::array(target);
    if(fromArray && toArray) {
        validateScalar(source.substr(0,source.size()-2),target.substr(0,target.size()-2));
        return;
    }
    validateScalar(source,target);
}
} // namespace dbms::regtype_cast_detail
