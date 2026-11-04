#pragma once
#include "common/DatumType.h"
#include "expression/GeneratedColumn.h"
#include <functional>
#include <map>
#include <set>
#include <string>
namespace dbms {
// An analysis-only view of one copied catalog generation. No execution row,
// evaluator, value, parameter frame, Session or source/provider is retained.
struct DatumMetadataContext {
    std::function<DatumTypePtr(const std::string&,uint32_t)> declaration;
    std::function<std::string(const std::string&)> inputDeclaration;
    std::function<DatumTypePtr(DatumTypePtr,int32_t)> modifier;
    bool sourceColumns = false; // true even when a physical source lacks OIDs
    std::function<std::shared_ptr<const CompoundDatum>(const std::vector<DatumTypePtr>&)> record;
    std::map<std::string,DatumTypePtr> columns;
    // Real durable source input only. Virtual/generated expression displays
    // must arrive through their actual owning-datum receiver instead.
    std::set<std::string> storedColumns;
    GeneratedRelationPtr generatedSource; // real physical schema; no runtime row
};
using DatumMetadataContextPtr = std::shared_ptr<const DatumMetadataContext>;
} // namespace dbms
