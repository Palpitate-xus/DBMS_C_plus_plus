#pragma once

#include "commands/TableManage.h"
#include "executor/SortSpill.h"
#include "expression/ExprEvaluator.h"

#include <limits>
#include <algorithm>
#include <memory>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace dbms {

// This observer belongs only to the private temporary-sort codec. Restoring a
// snapshot constructs a new value container; it never rebinds an execution,
// provider, source, parameter owner or cursor.
struct RowContextSpillAccess {
    using SqlValues = std::map<FunctionCallExpr::SqlValue, ExprValue>;
    using Values = std::map<std::string, ExprValue>;
    using Parameters = std::vector<ExprValue>;
    using BoundColumns = std::map<std::pair<size_t, size_t>, ExprValue>;
    static const SqlValues& sqlValues(const RowContext& row) { return row.sqlValues_; }
    static const Values& values(const RowContext& row) { return row.values_; }
    static const Parameters& parameters(const RowContext& row) { return row.parameters_; }
    static const BoundColumns& boundColumns(const RowContext& row) { return row.boundColumns_; }
    static const std::shared_ptr<const GeneratedInputState>& generated(const RowContext& row){return row.generatedInputs_;}
    static void generated(RowContext& row,std::shared_ptr<const GeneratedInputState> state){row.generatedInputs_=std::move(state);}
    static RowContext restore(SqlValues sql, Values values, Parameters parameters, BoundColumns bound) {
        RowContext result;
        result.sqlValues_ = std::move(sql); result.values_ = std::move(values);
        result.parameters_ = std::move(parameters); result.boundColumns_ = std::move(bound);
        return result;
    }
};

namespace typed_sort_detail {
// Codec-local immutable program identities; no runtime row/parameter/provider
// is retained and no process pointer is serialized into a temporary file.
struct GeneratedRegistry {
    std::vector<GeneratedReadPtr> reads;
    uint64_t intern(const GeneratedReadPtr& read) {
        if(!read)throw DbError("XX000","generated spill input has no program");
        for(size_t i=0;i<reads.size();++i)if(reads[i]==read)return i;
        reads.push_back(read);return reads.size()-1;
    }
    GeneratedReadPtr at(uint64_t id) const {
        if(id>=reads.size())throw DbError("58030","invalid generated spill descriptor identity");
        return reads[id];
    }
};

// Process-private framing: value/type pointers never appear on disk.
struct ValueHeader {
    uint64_t type=0,value=0,collation=0,elements=0,fields=0,arrayValues=0,dimensions=0;
    uint32_t oid=0;
    uint8_t isNull=0,hasOid=0,hasFields=0,hasCompound=0,hasArray=0,hasType=0;
    uint8_t bitmapPresent=0,bitmap=0;
};
struct ElementHeader {uint32_t oid=0;uint8_t present=0;};
struct TypeHeader {
    uint64_t fields=0;
    uint32_t actual=0,codec=0;
    int32_t modifier=-1;
    int16_t size=-1;
    uint8_t kind=0,enumType=0,output=0,element=0;
};
struct ContextHeader {uint64_t sql=0,values=0,parameters=0,bound=0,generatedBound=0,generatedNamed=0;};
inline size_t addBytes(size_t a,size_t b) {
    return b>std::numeric_limits<size_t>::max()-a?std::numeric_limits<size_t>::max():a+b;
}
inline size_t multiplyBytes(size_t a,size_t b) {
    return a && b>std::numeric_limits<size_t>::max()/a?std::numeric_limits<size_t>::max():a*b;
}
[[noreturn]] inline void invalid(const char* detail) {
    throw DbError("58030",std::string("invalid typed sort temporary ")+detail);
}
inline void requireCount(uint64_t count,uint64_t minimum,uint64_t maximum,uint64_t& remaining) {
    if(count>maximum || (minimum && count>remaining/minimum))invalid("count");
    remaining-=count*minimum;
}
inline std::string readString(sort_detail::Reader& reader,uint64_t length) {
    if(length>reader.remaining() || length>std::string{}.max_size())invalid("string extent");
    return reader.string(static_cast<size_t>(length));
}
template<class Visitor>
inline void visitTypes(const DatumType& root,Visitor&& visit) {
    struct Frame {const DatumType* node;size_t next;};
    std::vector<Frame> stack{{&root,0}};
    visit(root);
    while(!stack.empty()) {
        auto& frame=stack.back();const DatumType* child=nullptr;
        if(frame.next<frame.node->recordFields.size())child=&frame.node->recordFields[frame.next++];
        else if(frame.next==frame.node->recordFields.size() && frame.node->arrayElement) {
            ++frame.next;child=frame.node->arrayElement.get();
        } else {stack.pop_back();continue;}
        if(std::any_of(stack.begin(),stack.end(),[&](const Frame& parent){return parent.node==child;}))
            invalid("cyclic datum type");
        visit(*child);stack.push_back({child,0});
    }
}
inline size_t typeBytes(const DatumType& root) {
    size_t bytes=sizeof(DatumType)+2*sizeof(void*);
    visitTypes(root,[&](const DatumType& type) {
        bytes=addBytes(bytes,multiplyBytes(type.recordFields.capacity(),sizeof(DatumType)));
        if(type.arrayElement)bytes=addBytes(bytes,sizeof(DatumType)+2*sizeof(void*));
    });
    return bytes;
}
template<class Visitor>
inline void visitValues(const ExprValue& root,Visitor&& visit) {
    struct Frame {const std::vector<ExprValue>* records;const std::vector<ExprValue>* items;size_t next;};
    std::vector<Frame> stack;
    const ExprValue* current=&root;
    for(;;) {
        visit(*current);
        const auto* records=current->recordFields?&*current->recordFields:nullptr;
        const auto* items=current->compound && current->compound->arrayElements?&*current->compound->arrayElements:nullptr;
        const size_t recordCount=records?records->size():0,arrayCount=items?items->size():0;
        if(arrayCount && std::any_of(stack.begin(),stack.end(),[&](const Frame& parent){return parent.items==items;}))
            invalid("cyclic array value");
        if(recordCount || arrayCount)stack.push_back({records,items,0});
        const ExprValue* next=nullptr;
        while(!stack.empty()) {
            auto& frame=stack.back();const size_t count=frame.records?frame.records->size():0;
            if(frame.next<count){next=&frame.records->at(frame.next++);break;}
            const size_t arrayIndex=frame.next-count;
            if(frame.items && arrayIndex<frame.items->size()){next=&frame.items->at(arrayIndex);++frame.next;break;}
            stack.pop_back();
        }
        if(!next)return;
        current=next;
    }
}
inline size_t valueBytes(const ExprValue& value) {
    size_t bytes=sizeof(ExprValue);
    visitValues(value,[&](const ExprValue& cell) {
        for(const auto* text:{&cell.typeName,&cell.value,&cell.collation})bytes=addBytes(bytes,addBytes(text->capacity(),1));
        bytes=addBytes(bytes,multiplyBytes(cell.elementOids.capacity(),sizeof(std::optional<uint32_t>)));
        if(cell.recordFields)bytes=addBytes(bytes,multiplyBytes(cell.recordFields->capacity(),sizeof(ExprValue)));
        if(cell.compound) {
            bytes=addBytes(bytes,sizeof(CompoundDatum)+2*sizeof(void*));
            bytes=addBytes(bytes,multiplyBytes(cell.compound->dimensions.capacity(),sizeof(DatumArrayDimension)));
            if(cell.compound->arrayElements)bytes=addBytes(bytes,multiplyBytes(cell.compound->arrayElements->capacity(),sizeof(ExprValue)));
            if(cell.compound->type)bytes=addBytes(bytes,typeBytes(*cell.compound->type));
        }
    });
    return bytes;
}
inline size_t dynamicValueBytes(const ExprValue& value) {
    const auto bytes=valueBytes(value);
    return bytes==std::numeric_limits<size_t>::max()?bytes:bytes-sizeof(ExprValue);
}
template<class Map>inline size_t mapBytes(const Map& values) {
    return multiplyBytes(values.size(),addBytes(sizeof(typename Map::value_type),4*sizeof(void*)));
}
inline size_t contextBytes(const RowContext& row) {
    const auto& sql=RowContextSpillAccess::sqlValues(row);const auto& values=RowContextSpillAccess::values(row);
    const auto& parameters=RowContextSpillAccess::parameters(row);const auto& bound=RowContextSpillAccess::boundColumns(row);
    size_t bytes=addBytes(sizeof(RowContext),addBytes(mapBytes(sql),addBytes(mapBytes(values),mapBytes(bound))));
    bytes=addBytes(bytes,multiplyBytes(parameters.capacity(),sizeof(ExprValue)));
    for(const auto& item:sql)bytes=addBytes(bytes,dynamicValueBytes(item.second));
    for(const auto& item:values){bytes=addBytes(bytes,addBytes(item.first.capacity(),1));bytes=addBytes(bytes,dynamicValueBytes(item.second));}
    for(const auto& cell:parameters)bytes=addBytes(bytes,dynamicValueBytes(cell));
    for(const auto& item:bound)bytes=addBytes(bytes,dynamicValueBytes(item.second));
    if(const auto& generated=RowContextSpillAccess::generated(row)) {
        bytes=addBytes(bytes,sizeof(GeneratedInputState)+2*sizeof(void*));
        bytes=addBytes(bytes,addBytes(mapBytes(generated->bound),mapBytes(generated->named)));
        for(const auto& entry:generated->named)bytes=addBytes(bytes,entry.first.capacity()+1);
    }
    return bytes;
}
inline void writeType(sort_detail::Writer& writer,const DatumType& type) {
    visitTypes(type,[&](const DatumType& node) {
        TypeHeader h{};std::memset(static_cast<void*>(&h),0,sizeof(h));
        h.actual=node.actualOid;h.codec=node.codecOid;h.size=node.codecSize;h.modifier=node.effectiveTypeMod;
        h.kind=static_cast<uint8_t>(node.kind);h.enumType=node.enumType;h.output=node.hasBinaryOutput;
        h.element=bool(node.arrayElement);h.fields=node.recordFields.size();writer.scalar(h);
    });
}
struct TypeNode {DatumType value;uint64_t fields;bool element;};
inline TypeNode readTypeNode(sort_detail::Reader& reader) {
    const auto h=reader.scalar<TypeHeader>();
    if(h.kind>static_cast<uint8_t>(DatumKind::Array) || h.enumType>1 || h.output>1 || h.element>1)invalid("datum type flags");
    uint64_t remaining=reader.remaining();
    requireCount(h.fields,sizeof(TypeHeader),std::vector<DatumType>{}.max_size(),remaining);
    requireCount(h.element,sizeof(TypeHeader),1,remaining);
    DatumType type;type.actualOid=h.actual;type.codecOid=h.codec;type.codecSize=h.size;
    type.effectiveTypeMod=h.modifier;type.kind=static_cast<DatumKind>(h.kind);
    type.enumType=h.enumType;type.hasBinaryOutput=h.output;type.recordFields.reserve(static_cast<size_t>(h.fields));
    return {std::move(type),h.fields,h.element!=0};
}
inline DatumTypePtr readType(sort_detail::Reader& reader) {
    auto node=readTypeNode(reader);auto root=std::make_shared<DatumType>(std::move(node.value));
    struct Frame {DatumType* type;uint64_t fields;bool element;};
    std::vector<Frame> stack{{root.get(),node.fields,node.element}};
    while(!stack.empty()) {
        auto& frame=stack.back();DatumType* child=nullptr;
        if(!frame.fields && !frame.element){stack.pop_back();continue;}
        auto next=readTypeNode(reader);
        if(frame.fields) {
            frame.type->recordFields.push_back(std::move(next.value));--frame.fields;
            child=&frame.type->recordFields.back();
        } else {
            auto element=std::make_shared<DatumType>(std::move(next.value));child=element.get();
            frame.type->arrayElement=std::move(element);frame.element=false;
        }
        if(next.fields || next.element)stack.push_back({child,next.fields,next.element});
    }
    return root;
}
inline void writeValue(sort_detail::Writer& writer,const ExprValue& value) {
    visitValues(value,[&](const ExprValue& cell) {
        ValueHeader h{};std::memset(static_cast<void*>(&h),0,sizeof(h));
        h.type=cell.typeName.size();h.value=cell.value.size();h.collation=cell.collation.size();
        h.elements=cell.elementOids.size();h.fields=cell.recordFields?cell.recordFields->size():0;
        h.isNull=cell.isNull;h.hasOid=bool(cell.objectOid);h.oid=cell.objectOid.value_or(0);h.hasFields=bool(cell.recordFields);
        h.hasCompound=bool(cell.compound);
        if(cell.compound) {
            const auto& data=*cell.compound;h.hasType=bool(data.type);h.hasArray=bool(data.arrayElements);
            h.arrayValues=data.arrayElements?data.arrayElements->size():0;h.dimensions=data.dimensions.size();
            h.bitmapPresent=data.arrayNullBitmap.has_value();h.bitmap=data.arrayNullBitmap.value_or(false);
        }
        writer.scalar(h);writer.append(cell.typeName.data(),cell.typeName.size());
        writer.append(cell.value.data(),cell.value.size());writer.append(cell.collation.data(),cell.collation.size());
        for(const auto& oid:cell.elementOids) {
            ElementHeader e{};std::memset(static_cast<void*>(&e),0,sizeof(e));e.present=oid.has_value();e.oid=oid.value_or(0);writer.scalar(e);
        }
        if(cell.compound) {
            if(cell.compound->type)writeType(writer,*cell.compound->type);
            for(const auto& d:cell.compound->dimensions){writer.scalar(d.length);writer.scalar(d.lower);}
        }
    });
}
struct ValueNode {
    ExprValue cell;uint64_t records=0,arrays=0;std::shared_ptr<CompoundDatum> compound;
};
inline ValueNode readNode(sort_detail::Reader& reader) {
    const auto h=reader.scalar<ValueHeader>();
    if(h.isNull>1 || h.hasOid>1 || h.hasFields>1 || h.hasCompound>1 || h.hasArray>1 || h.hasType>1 || h.bitmapPresent>1 || h.bitmap>1 ||
       (!h.hasOid && h.oid) || (!h.hasFields && h.fields) || (!h.hasArray && h.arrayValues) ||
       (!h.bitmapPresent && h.bitmap) || (!h.hasCompound && (h.hasArray || h.hasType || h.dimensions || h.bitmapPresent)) || h.dimensions>6)
        invalid("value flags");
    uint64_t remaining=reader.remaining();
    for(const auto n:{h.type,h.value,h.collation})requireCount(n,1,std::string{}.max_size(),remaining);
    requireCount(h.elements,sizeof(ElementHeader),std::vector<std::optional<uint32_t>>{}.max_size(),remaining);
    requireCount(h.fields,sizeof(ValueHeader),std::vector<ExprValue>{}.max_size(),remaining);
    requireCount(h.arrayValues,sizeof(ValueHeader),std::vector<ExprValue>{}.max_size(),remaining);
    requireCount(h.dimensions,2*sizeof(int32_t),6,remaining);
    requireCount(h.hasType,sizeof(TypeHeader),1,remaining);
    ExprValue value;value.typeName=readString(reader,h.type);value.value=readString(reader,h.value);
    value.collation=readString(reader,h.collation);value.isNull=h.isNull!=0;if(h.hasOid)value.objectOid=h.oid;
    value.elementOids.reserve(static_cast<size_t>(h.elements));
    for(uint64_t i=0;i<h.elements;++i) {
        const auto e=reader.scalar<ElementHeader>();if(e.present>1 || (!e.present && e.oid))invalid("element identity flag");
        value.elementOids.push_back(e.present?std::optional<uint32_t>(e.oid):std::nullopt);
    }
    if(h.hasFields){value.recordFields.emplace();value.recordFields->reserve(static_cast<size_t>(h.fields));}
    std::shared_ptr<CompoundDatum> data;
    if(h.hasCompound) {
        data=std::make_shared<CompoundDatum>();if(h.hasType)data->type=readType(reader);
        data->dimensions.reserve(static_cast<size_t>(h.dimensions));
        uint64_t count=h.dimensions?1:0;
        for(uint64_t i=0;i<h.dimensions;++i) {
            const auto length=reader.scalar<int32_t>(),lower=reader.scalar<int32_t>();
            if(length<=0 || count>std::numeric_limits<uint64_t>::max()/static_cast<uint32_t>(length) ||
               int64_t(lower)+length-1>=INT32_MAX)invalid("array extent");
            count*=static_cast<uint32_t>(length);data->dimensions.push_back({length,lower});
        }
        if(h.hasArray) {
            if(count!=h.arrayValues)invalid("array cardinality");
            data->arrayElements.emplace();data->arrayElements->reserve(static_cast<size_t>(h.arrayValues));
        } else if(h.dimensions || h.arrayValues)invalid("array payload presence");
        if(h.bitmapPresent)data->arrayNullBitmap=h.bitmap!=0;
        value.compound=data;
    }
    return {std::move(value),h.fields,h.arrayValues,std::move(data)};
}
inline ExprValue readValue(sort_detail::Reader& reader) {
    auto root=readNode(reader);ExprValue value=std::move(root.cell);
    struct Frame {std::vector<ExprValue>* records;std::vector<ExprValue>* arrays;uint64_t recordsLeft,arraysLeft;};
    std::vector<Frame> stack;
    const auto push=[&](ExprValue& cell,const ValueNode& node) {
        if(node.records || node.arrays)stack.push_back({cell.recordFields?&*cell.recordFields:nullptr,
            node.compound && node.compound->arrayElements?&*node.compound->arrayElements:nullptr,node.records,node.arrays});
    };
    push(value,root);
    while(!stack.empty()) {
        auto& frame=stack.back();auto next=readNode(reader);
        auto* target=frame.recordsLeft?frame.records:frame.arrays;
        if(!target)invalid("value child sequence");
        target->push_back(std::move(next.cell));
        if(frame.recordsLeft)--frame.recordsLeft;else --frame.arraysLeft;
        auto* child=&target->back();push(*child,next);
        while(!stack.empty() && !stack.back().recordsLeft && !stack.back().arraysLeft)stack.pop_back();
    }
    return value;
}

inline void writeContext(sort_detail::Writer& writer, const RowContext& row,GeneratedRegistry* registry=nullptr) {
    const auto& sql = RowContextSpillAccess::sqlValues(row);
    const auto& values = RowContextSpillAccess::values(row);
    const auto& parameters = RowContextSpillAccess::parameters(row);
    const auto& bound = RowContextSpillAccess::boundColumns(row);
    const auto& generated=RowContextSpillAccess::generated(row);
    if(generated && !registry)throw DbError("XX000","generated context spill lost its actual codec owner");
    writer.scalar(ContextHeader{sql.size(),values.size(),parameters.size(),bound.size(),
        generated?generated->bound.size():0,generated?generated->named.size():0});
    for (const auto& item : sql) { writer.scalar(static_cast<uint32_t>(item.first)); writeValue(writer, item.second); }
    for (const auto& item : values) {
        writer.scalar(uint64_t(item.first.size())); writer.append(item.first.data(), item.first.size());
        writeValue(writer, item.second);
    }
    for (const auto& cell : parameters) writeValue(writer, cell);
    for (const auto& item : bound) {
        writer.scalar(uint64_t(item.first.first)); writer.scalar(uint64_t(item.first.second));
        writeValue(writer, item.second);
    }
    if(generated) {
        for(const auto& entry:generated->bound) {
            writer.scalar(uint64_t(entry.first.first));writer.scalar(uint64_t(entry.first.second));
            writer.scalar(registry->intern(entry.second.read));writer.scalar(uint8_t(entry.second.present));
        }
        for(const auto& entry:generated->named) {
            writer.scalar(uint64_t(entry.first.size()));writer.append(entry.first.data(),entry.first.size());
            writer.scalar(registry->intern(entry.second.read));writer.scalar(uint8_t(entry.second.present));
        }
    }
}
inline RowContext readContext(sort_detail::Reader& reader,const GeneratedRegistry* registry=nullptr) {
    const auto header = reader.scalar<ContextHeader>();
    RowContextSpillAccess::SqlValues sql;
    RowContextSpillAccess::Values values;
    RowContextSpillAccess::Parameters parameters;
    RowContextSpillAccess::BoundColumns bound;
    uint64_t remaining = reader.remaining();
    requireCount(header.sql, sizeof(uint32_t) + sizeof(ValueHeader), sql.max_size(), remaining);
    requireCount(header.values, sizeof(uint64_t) + sizeof(ValueHeader), values.max_size(), remaining);
    requireCount(header.parameters, sizeof(ValueHeader), parameters.max_size(), remaining);
    requireCount(header.bound, 2 * sizeof(uint64_t) + sizeof(ValueHeader), bound.max_size(), remaining);
    for (uint64_t i = 0; i < header.sql; ++i) {
        const auto key = reader.scalar<uint32_t>();
        if (key > static_cast<uint32_t>(FunctionCallExpr::SqlValue::LocalTimestamp)) invalid("SQL value kind");
        auto cell = readValue(reader);
        if (!sql.emplace(static_cast<FunctionCallExpr::SqlValue>(key), std::move(cell)).second) invalid("duplicate SQL value");
    }
    for (uint64_t i = 0; i < header.values; ++i) {
        const auto size = reader.scalar<uint64_t>();
        if (reader.remaining() < sizeof(ValueHeader) || size > reader.remaining() - sizeof(ValueHeader)) invalid("value name extent");
        auto key = readString(reader, size); auto cell = readValue(reader);
        if (!values.emplace(std::move(key), std::move(cell)).second) invalid("duplicate value name");
    }
    parameters.reserve(static_cast<size_t>(header.parameters));
    for (uint64_t i = 0; i < header.parameters; ++i) parameters.push_back(readValue(reader));
    for (uint64_t i = 0; i < header.bound; ++i) {
        const auto source = reader.scalar<uint64_t>(), column = reader.scalar<uint64_t>();
        if (source > std::numeric_limits<size_t>::max() || column > std::numeric_limits<size_t>::max()) invalid("bound column ordinal");
        auto cell = readValue(reader);
        if (!bound.emplace(std::make_pair(static_cast<size_t>(source), static_cast<size_t>(column)), std::move(cell)).second)
            invalid("duplicate bound column");
    }
    auto result=RowContextSpillAccess::restore(std::move(sql),std::move(values),std::move(parameters),std::move(bound));
    if(header.generatedBound || header.generatedNamed) {
        if(!registry)invalid("generated program registry");
        requireCount(header.generatedBound,3*sizeof(uint64_t)+sizeof(uint8_t),std::numeric_limits<size_t>::max(),remaining);
        requireCount(header.generatedNamed,2*sizeof(uint64_t)+sizeof(uint8_t),std::numeric_limits<size_t>::max(),remaining);
        auto generated=std::make_shared<GeneratedInputState>();
        for(uint64_t i=0;i<header.generatedBound;++i) {
            const auto source=reader.scalar<uint64_t>(),column=reader.scalar<uint64_t>(),id=reader.scalar<uint64_t>();
            const auto present=reader.scalar<uint8_t>();if(present>1 || source>SIZE_MAX || column>SIZE_MAX)invalid("generated occurrence");
            auto read=registry->at(id);
            // Restore the same occurrence/attribute invariant as the live
            // RowContext attachment, before exposing any generated recipe.
            if(!read || !read->program || read->named || read->sourceOrdinal!=source ||
               column>=read->program->inputs.size() ||
               read->program->inputs[size_t(column)].dataName!=read->program->target.dataName)
                invalid("generated occurrence descriptor");
            if(!generated->bound.emplace(std::make_pair(size_t(source),size_t(column)),GeneratedInputState::Input{std::move(read),bool(present)}).second)invalid("duplicate generated occurrence");
        }
        for(uint64_t i=0;i<header.generatedNamed;++i) {
            const auto length=reader.scalar<uint64_t>();auto key=readString(reader,length);
            const auto id=reader.scalar<uint64_t>();const auto present=reader.scalar<uint8_t>();if(present>1)invalid("generated presence");
            if(!generated->named.emplace(std::move(key),GeneratedInputState::Input{registry->at(id),bool(present)}).second)invalid("duplicate generated name");
        }
        RowContextSpillAccess::generated(result,std::move(generated));
    }
    return result;
}

} // namespace typed_sort_detail
} // namespace dbms
