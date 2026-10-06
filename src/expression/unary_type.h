#pragma once

#include "expression/common_type.h"

namespace dbms {
// Actual pg_catalog prefix +/- signatures. Resolution is pure metadata;
// operand values, NULLness and routine bodies never choose an overload.
inline std::string resolveBuiltinUnary(const std::string& op,
                                      const std::string& inputType) {
    using namespace common_type_detail;
    const auto input=canonical(inputType);
    static const std::vector<std::string> numeric = {
        "smallint","integer","bigint","real","double precision","numeric"
    };
    std::vector<std::string> signatures=numeric;
    if(op=="-") signatures.emplace_back("interval");
    const auto missing=[&]() -> std::string {
        throw DbError("42883","operator does not exist: "+op+" "+input);
    };
    if(op!="+" && op!="-")return missing();
    for(const auto& target:signatures)if(input==target)return target;
    signatures.erase(std::remove_if(signatures.begin(),signatures.end(),
        [&](const auto& target){return !implicit(input,target);}),signatures.end());
    if(signatures.empty())return missing();
    if(signatures.size()==1)return signatures.front();
    if(input!="unknown") {
        const bool hasPreferred=std::any_of(signatures.begin(),signatures.end(),
            [&](const auto& target){return category(input)==category(target) && preferred(target);});
        if(hasPreferred)signatures.erase(std::remove_if(signatures.begin(),signatures.end(),
            [&](const auto& target){return category(input)!=category(target) || !preferred(target);}),signatures.end());
    } else {
        char chosen=0;bool conflict=false;
        for(const auto& target:signatures) {
            const auto next=category(target);
            if(next=='S'){chosen='S';conflict=false;break;}
            if(!chosen)chosen=next;else if(next!=chosen)conflict=true;
        }
        if(conflict)throw DbError("42725","operator is not unique: "+op+" unknown");
        const bool hasPreferred=std::any_of(signatures.begin(),signatures.end(),
            [&](const auto& target){return category(target)==chosen && preferred(target);});
        signatures.erase(std::remove_if(signatures.begin(),signatures.end(),
            [&](const auto& target){return category(target)!=chosen || (hasPreferred && !preferred(target));}),signatures.end());
    }
    if(signatures.size()==1)return signatures.front();
    throw DbError("42725","operator is not unique: "+op+" "+input);
}
} // namespace dbms
