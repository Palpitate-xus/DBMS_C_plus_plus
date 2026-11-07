#pragma once
#include "common/DbError.h"
#include "interfaces/table_schema.h"
#include <cerrno>
#include <cstdlib>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace dbms::sequence_parameters {
inline int64_t integer(const std::string& raw) {
    size_t at=!raw.empty() && (raw[0]=='+' || raw[0]=='-') ? 1 : 0;
    if(at==raw.size())throw DbError("22P02","invalid input syntax for type bigint: \""+raw+"\"");
    for(size_t i=at;i<raw.size();++i)
        if(raw[i]<'0' || raw[i]>'9')throw DbError("22P02","invalid input syntax for type bigint: \""+raw+"\"");
    errno=0;char* end=nullptr;const auto value=std::strtoll(raw.c_str(),&end,10);
    if(errno==ERANGE)throw DbError("22003","value \""+raw+"\" is out of range for type bigint");
    if(end!=raw.c_str()+raw.size())throw DbError("22P02","invalid input syntax for type bigint");
    return static_cast<int64_t>(value);
}
struct Parameters {
    SequenceInfo request;
    SequenceInfo effective;
    std::optional<std::string> owner;
};
// Pure declaration processing in PostgreSQL init_params order. Resolving AS
// only consults copied type metadata. No sequence value is read or allocated.
template<class ResolveType>
Parameters resolve(const std::vector<std::pair<std::string,std::string>>& options,
                   const SequenceInfo* previous,ResolveType&& resolveType) {
    Parameters result;auto& request=result.request;auto& value=result.effective;
    const bool creating=!previous;
    if(previous)value=*previous;
    std::map<std::string,std::pair<std::string,std::string>> supplied;
    static const std::set<std::string> allowed={"as","start","restart","increment","minvalue",
        "maxvalue","nominvalue","nomaxvalue","cache","cycle","nocycle","ownedby"};
    for(const auto& option:options) {
        if(!allowed.count(option.first))throw DbError("42601","invalid sequence option: "+option.first);
        auto key=option.first;
        if(key=="nominvalue")key="minvalue";
        else if(key=="nomaxvalue")key="maxvalue";
        else if(key=="nocycle")key="cycle";
        if(!supplied.emplace(key,option).second)throw DbError("42601","conflicting or redundant options");
    }
    const auto option=[&](const std::string& name)->const std::pair<std::string,std::string>* {
        const auto found=supplied.find(name);return found==supplied.end()?nullptr:&found->second;
    };
    bool resetMin=false,resetMax=false;
    if(const auto* type=option("as")) {
        const auto oid=resolveType(type->second);
        if(!SequenceInfo::validDataType(oid))throw DbError("22023","sequence type must be smallint, integer, or bigint");
        request.dataTypeOid=oid;request.dataTypeSpecified=true;
        if(previous) {
            resetMin=previous->minValue==SequenceInfo::typeMinimum(previous->dataTypeOid);
            resetMax=previous->maxValue==SequenceInfo::typeMaximum(previous->dataTypeOid);
        }
        value.dataTypeOid=oid;
    } else if(creating)value.dataTypeOid=20;
    if(!SequenceInfo::validDataType(value.dataTypeOid))throw DbError("XX001","invalid stored sequence type");
    const auto low=SequenceInfo::typeMinimum(value.dataTypeOid),high=SequenceInfo::typeMaximum(value.dataTypeOid);
    if(const auto* increment=option("increment")) {
        request.incrementSpecified=true;request.increment=value.increment=integer(increment->second);
    }
    if(!value.increment)throw DbError("22023","INCREMENT must not be zero");
    if(const auto* cycle=option("cycle")) {
        request.cycleSpecified=true;request.cycle=value.cycle=cycle->first=="cycle";
    }
    const auto* maximum=option("maxvalue");
    if(maximum && maximum->first=="maxvalue") {
        request.hasMaxValue=true;request.maxValue=value.maxValue=integer(maximum->second);
    } else if(creating || maximum || resetMax) {
        value.maxValue=value.increment>0 || resetMax ? high : -1;
        if(maximum)request.noMaxValue=true;
    }
    if(value.maxValue<low || value.maxValue>high)throw DbError("22023","MAXVALUE is out of range for sequence data type");
    const auto* minimum=option("minvalue");
    if(minimum && minimum->first=="minvalue") {
        request.hasMinValue=true;request.minValue=value.minValue=integer(minimum->second);
    } else if(creating || minimum || resetMin) {
        value.minValue=value.increment<0 || resetMin ? low : 1;
        if(minimum)request.noMinValue=true;
    }
    if(value.minValue<low || value.minValue>high)throw DbError("22023","MINVALUE is out of range for sequence data type");
    if(value.minValue>=value.maxValue)throw DbError("22023","MINVALUE must be less than MAXVALUE");
    if(const auto* start=option("start")) {
        request.startSpecified=true;request.start=value.start=integer(start->second);
    } else if(creating)value.start=value.increment>0?value.minValue:value.maxValue;
    if(value.start<value.minValue || value.start>value.maxValue)throw DbError("22023","START value is outside sequence bounds");
    if(const auto* restart=option("restart")) {
        request.restartSpecified=true;request.restartValueSpecified=!restart->second.empty();
        request.restart=request.restartValueSpecified?integer(restart->second):value.start;
        if(request.restart<value.minValue || request.restart>value.maxValue)throw DbError("22023","RESTART value is outside sequence bounds");
    }
    if(const auto* cache=option("cache")) {
        request.cacheSpecified=true;request.cache=value.cache=integer(cache->second);
    }
    if(value.cache<=0)throw DbError("22023","CACHE must be greater than zero");
    if(const auto* owner=option("ownedby"))result.owner=owner->second;
    if(creating) {
        // Pass fully resolved bounds to the native creator. Its historical
        // untyped API defaults remain an independent compatibility contract.
        value.hasMinValue=value.hasMaxValue=value.startSpecified=true;
        value.dataTypeSpecified=true;
        value.restartSpecified=request.restartSpecified;
        value.restartValueSpecified=request.restartValueSpecified;
        value.restart=request.restart;
    }
    return result;
}
}
