#pragma once

#include "parser/ast.h"
#include "expression/ExprEvaluator.h"
#include "types/numeric.h"
#include "common/DbError.h"
#include <cmath>
#include <functional>
#include <utility>

namespace dbms {

// Offsets are initialized once by the execution owner, after pure binding.
// Bounds use half-open source ordinals, including genuinely empty frames.
struct WindowFrame {
    using Bound = WindowDef::BoundKind;
    std::string mode = "rows";
    Bound start = Bound::Unspecified, end = Bound::Unspecified;
    ExprValue startOffset, endOffset;

    bool prepared() const { return start != Bound::Unspecified; }
    static bool offsetBound(Bound bound) {return bound==Bound::Preceding || bound==Bound::Following;}
    void validateOffsets() const {
        const auto validate=[&](Bound kind,const ExprValue& value) {
            if(!offsetBound(kind))return;
            if(value.isNull)throw DbError("22004","frame offset must not be null");
            // RANGE checks negative distance when in_range is demanded, not
            // for an empty source or a ranking function that ignores frames.
            if(mode!="range" && value.asInt()<0)
                throw DbError("22013","frame offset must not be negative");
        };
        validate(start,startOffset);validate(end,endOffset);
    }

    using Peers = std::function<std::pair<size_t,size_t>(size_t)>;
    using Key = std::function<ExprValue(size_t)>;
    std::pair<size_t,size_t> bounds(size_t current,size_t first,size_t last,
        const Peers& peers,const Key& key,bool ascending,bool nullsFirst) const {
        const auto boundary=[&](Bound kind,const ExprValue& distance,bool isEnd) -> size_t {
            if(kind==Bound::UnboundedPreceding)return first;
            if(kind==Bound::UnboundedFollowing)return last;
            if(kind==Bound::CurrentRow) {
                if(mode=="rows")return current+(isEnd?1:0);
                const auto group=peers(current);return isEnd?group.second:group.first;
            }
            if(!offsetBound(kind))throw DbError("XX000","uninitialized window frame boundary");
            if(distance.isNull)throw DbError("22004","frame offset must not be null");
            if(mode=="rows") {
                const int64_t signedCount=distance.asInt();
                if(signedCount<0)throw DbError("22013","frame offset must not be negative");
                const uint64_t count=static_cast<uint64_t>(signedCount);
                if(kind==Bound::Preceding) {
                    if(count>current-first)return first;
                    return current-static_cast<size_t>(count)+(isEnd?1:0);
                }
                if(count>=last-current)return last;
                return current+static_cast<size_t>(count)+(isEnd?1:0);
            }
            if(mode=="groups") {
                const int64_t signedCount=distance.asInt();
                if(signedCount<0)throw DbError("22013","frame offset must not be negative");
                uint64_t count=static_cast<uint64_t>(signedCount);
                auto group=peers(current);
                while(count) {
                    if(kind==Bound::Preceding) {
                        if(group.first==first)return isEnd?first:group.first;
                        group=peers(group.first-1);
                    } else {
                        if(group.second==last)return last;
                        group=peers(group.second);
                    }
                    --count;
                }
                return isEnd?group.second:group.first;
            }
            if(mode!="range")throw DbError("XX000","unknown window frame mode");
            const auto value=key(current);
            if(value.isNull) {
                const auto group=peers(current);return isEnd?group.second:group.first;
            }
            const bool subtract=(kind==Bound::Preceding)==ascending;
            const bool floating=value.typeName=="real" || value.typeName=="float4" ||
                value.typeName=="double precision" || value.typeName=="float8" || value.typeName=="double" ||
                value.typeName=="float" || distance.typeName=="double precision";
            const auto compare=[&](const ExprValue& candidate) {
                if(floating) {
                    const double offset=distance.asDouble();
                    if(offset<0)throw DbError("22013","frame offset must not be negative");
                    const double limit=value.asDouble()+(subtract?-offset:offset), other=candidate.asDouble();
                    if(std::isnan(other))return std::isnan(limit)?0:1;
                    if(std::isnan(limit))return -1;
                    return other<limit?-1:other>limit?1:0;
                }
                const Numeric offset(distance.value);
                if(offset<Numeric(0))throw DbError("22013","frame offset must not be negative");
                const Numeric limit=subtract?Numeric(value.value)-offset:Numeric(value.value)+offset;
                const Numeric other(candidate.value);
                return other<limit?-1:other>limit?1:0;
            };
            // Validate a negative distance even if no candidate passes the
            // threshold. Exact decimal arithmetic avoids integer overflow
            // and BIGINT/NUMERIC precision loss in RANGE boundaries.
            (void)compare(value);
            size_t position=first;
            while(position<last) {
                const auto candidate=key(position);
                bool before;
                if(candidate.isNull)before=nullsFirst;
                else {
                    const int cmp=compare(candidate)*(ascending?1:-1);
                    before=isEnd?cmp<=0:cmp<0;
                }
                if(!before)break;
                ++position;
            }
            return position;
        };
        const size_t begin=boundary(start,startOffset,false);
        const size_t endPosition=boundary(end,endOffset,true);
        return {begin,endPosition<begin?begin:endPosition};
    }
};

} // namespace dbms
