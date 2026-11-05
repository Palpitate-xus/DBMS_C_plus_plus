#pragma once
#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>

namespace dbms::planner_statistics {
// The existing native ANALYZE image stores actual row counts. Match MCVs
// with the same declared-column equality as execution; estimate non-MCVs
// from the remaining non-NULL probability and distinct population.
// Unknown input/operator semantics are not a new constant-analysis owner.
template<class Statistics,class Equal>
std::optional<double> equalityFraction(const Statistics& stats,double analyzedRows,Equal equal) {
    if(!std::isfinite(analyzedRows) || analyzedRows<=0 ||
       stats.nullCount>analyzedRows || stats.cardinality<stats.mcv.size())return {};
    if(!stats.cardinality) {
        if(stats.nullCount==analyzedRows)return 0;
        return {};
    }
    long double total=0,matched=0;
    double smallest=1;
    bool unknown=false,hit=false;
    for(const auto& entry:stats.mcv) {
        total+=entry.second;
        smallest=std::min(smallest,double(entry.second)/analyzedRows);
        const auto matches=equal(entry.first);
        if(!matches)unknown=true;
        else if(*matches){hit=true;matched+=entry.second;}
    }
    if(total+stats.nullCount>analyzedRows)return {};
    if(unknown)return {};
    if(hit)return std::clamp(double(matched/analyzedRows),0.0,1.0);
    double other=double((analyzedRows-stats.nullCount-total)/analyzedRows);
    const auto remaining=stats.cardinality-stats.mcv.size();
    if(remaining>1)other/=remaining;
    if(!stats.mcv.empty())other=std::min(other,smallest);
    return std::clamp(other,0.0,1.0);
}
} // namespace dbms::planner_statistics
