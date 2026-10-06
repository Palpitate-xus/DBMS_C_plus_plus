#pragma once

#include "expression/common_type.h"
#include <utility>

namespace dbms {
// Builtin binary '=' signatures, not CASE result common types. Resolution is
// pure: exact signatures precede implicit candidates, then exact argument
// count and same-category preferred argument count select an overload. The
// caller retains both declared operand types because cross-type operators
// (notably float4/float8 and int2/int4/int8) are genuine signatures.
inline std::pair<std::string,std::string> resolveBuiltinEquality(
    const std::string& leftRaw,const std::string& rightRaw) {
    using namespace common_type_detail;
    const std::string left=canonical(leftRaw),right=canonical(rightRaw);
    using Signature=std::pair<std::string,std::string>;
    static const std::vector<Signature> signatures=[] {
        std::vector<Signature> result;
        for(const auto& type:std::vector<std::string>{"boolean","\"char\"","name","text",
            "bpchar","numeric","money","oid","date","time","timetz","timestamp",
            "timestamptz","interval","uuid","bytea","inet","macaddr","macaddr8",
            "bit","bit varying","jsonb","pg_lsn","tsvector","tsquery","path",
            "circle","lseg","line"}) result.emplace_back(type,type);
        for(const auto& a:std::vector<std::string>{"smallint","integer","bigint"})
            for(const auto& b:std::vector<std::string>{"smallint","integer","bigint"})
                result.emplace_back(a,b);
        for(const auto& a:std::vector<std::string>{"real","double precision"})
            for(const auto& b:std::vector<std::string>{"real","double precision"})
                result.emplace_back(a,b);
        for(const auto& a:std::vector<std::string>{"date","timestamp","timestamptz"})
            for(const auto& b:std::vector<std::string>{"date","timestamp","timestamptz"})
                if(a!=b) result.emplace_back(a,b);
        result.emplace_back("name","text"); result.emplace_back("text","name");
        return result;
    }();
    const auto missing=[&]() -> Signature {
        throw DbError("42883","operator does not exist: "+left+" = "+right);
    };
    // anyarray requires one actual array type, not ARRAY's compatible result
    // element promotion. Different named array types cannot select array_eq.
    if(array(left)||array(right)) {
        if((left==right && array(left)) || (array(left) && right=="unknown")) return {left,left};
        if(left=="unknown" && array(right)) return {right,right};
        return missing();
    }
    const std::string exactLeft=left=="unknown"?right:left;
    const std::string exactRight=right=="unknown"?left:right;
    for(const auto& signature:signatures)
        if(signature.first==exactLeft && signature.second==exactRight) return signature;

    std::vector<Signature> candidates;
    for(const auto& signature:signatures)
        if(implicit(left,signature.first)&&implicit(right,signature.second)) candidates.push_back(signature);
    if(candidates.empty()) return missing();
    const auto best=[&](const auto& score) {
        int maximum=-1;
        for(const auto& candidate:candidates) maximum=std::max(maximum,score(candidate));
        candidates.erase(std::remove_if(candidates.begin(),candidates.end(),
            [&](const auto& candidate){return score(candidate)!=maximum;}),candidates.end());
    };
    best([&](const auto& candidate){return int(left==candidate.first)+int(right==candidate.second);});
    if(candidates.size()==1) return candidates.front();
    best([&](const auto& candidate){
        return int(left==candidate.first || (left!="unknown" && category(left)==category(candidate.first) && preferred(candidate.first)))+
            int(right==candidate.second || (right!="unknown" && category(right)==category(candidate.second) && preferred(candidate.second)));
    });
    if(candidates.size()==1) return candidates.front();
    // Unknown arguments retain operator-resolution string bias. Simple CASE
    // finalizes an unknown switch to text, so normally only WHEN is unknown.
    for(size_t position=0;position<2;++position) {
        if((position==0?left:right)!="unknown") continue;
        char chosen=0; bool conflict=false,hasPreferred=false;
        for(const auto& candidate:candidates) {
            const auto& type=position==0?candidate.first:candidate.second;
            const char next=category(type);
            if(next=='S') {chosen='S';conflict=false;break;}
            if(!chosen) chosen=next; else if(chosen!=next) conflict=true;
        }
        if(conflict) throw DbError("42725","operator is not unique: "+left+" = "+right);
        for(const auto& candidate:candidates) {
            const auto& type=position==0?candidate.first:candidate.second;
            if(category(type)==chosen && preferred(type)) hasPreferred=true;
        }
        candidates.erase(std::remove_if(candidates.begin(),candidates.end(),[&](const auto& candidate){
            const auto& type=position==0?candidate.first:candidate.second;
            return category(type)!=chosen || (hasPreferred&&!preferred(type));
        }),candidates.end());
    }
    if(candidates.size()==1) return candidates.front();
    if((left=="unknown")!=(right=="unknown")) {
        const auto& known=left=="unknown"?right:left;
        candidates.erase(std::remove_if(candidates.begin(),candidates.end(),[&](const auto& candidate){
            return !implicit(known,left=="unknown"?candidate.first:candidate.second);
        }),candidates.end());
        if(candidates.size()==1) return candidates.front();
    }
    throw DbError("42725","operator is not unique: "+left+" = "+right);
}
} // namespace dbms
