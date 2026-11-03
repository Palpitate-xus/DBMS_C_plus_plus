#pragma once

#include "expression/common_type.h"

namespace dbms::array_push_type_detail {

// anycompatiblearray/anycompatible select one common element in actual SQL
// argument order. An UNKNOWN array input contributes no known element; a
// typed empty or NULL array still contributes its genuine declaration.
inline std::string element(bool prepend,const std::string& leftRaw,const std::string& rightRaw) {
    const auto normalize=[](const std::string& raw) {
        const auto type=common_type_detail::canonical(raw);
        // The legacy public native literal adapter spells its untyped SQL
        // NULL declaration "null"; this is metadata, never a value probe.
        return type=="null"?std::string("unknown"):type;
    };
    auto left=normalize(leftRaw),right=normalize(rightRaw);
    auto& array=prepend?right:left;
    if(common_type_detail::array(array))array.resize(array.size()-2);
    else if(array!="unknown")throw DbError("42883","array push requires an array argument");
    return selectCommonType({left,right},"anycompatible");
}

// These are unconstrained resolved codec declarations, not explicit SQL
// CHAR(1)/BIT(1) casts. Preserve their original bytes and variable bit width.
inline std::string implicitDeclaration(const std::string& type) {
    bool array=common_type_detail::array(type);
    auto scalar=array?type.substr(0,type.size()-2):type;
    if(scalar=="bpchar")scalar="pg_catalog.bpchar";
    else if(scalar=="bit")scalar="pg_catalog.\"bit\"";
    return scalar+(array?"[]":"");
}

} // namespace dbms::array_push_type_detail
