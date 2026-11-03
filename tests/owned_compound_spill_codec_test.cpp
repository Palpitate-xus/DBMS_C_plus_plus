// Owning compound datum spill encoding: round-trip and corruption checks.
// These explicitly constructed datums test only the private temporary codec.
// They do not stand in for physical/expression producer pipeline evidence.
#include "executor/TypedSortSpill.h"
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace dbms;
namespace codec=dbms::typed_sort_detail;
namespace {
bool sameType(const DatumType* a,const DatumType* b) {
    if(!a || !b)return a==b;
    if(a->actualOid!=b->actualOid || a->codecOid!=b->codecOid || a->codecSize!=b->codecSize ||
       a->effectiveTypeMod!=b->effectiveTypeMod || a->kind!=b->kind || a->enumType!=b->enumType ||
       a->hasBinaryOutput!=b->hasBinaryOutput || a->recordFields.size()!=b->recordFields.size() ||
       !sameType(a->arrayElement.get(),b->arrayElement.get()))return false;
    for(size_t i=0;i<a->recordFields.size();++i)
        if(!sameType(&a->recordFields[i],&b->recordFields[i]))return false;
    return true;
}
bool same(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields) ||
       bool(a.compound)!=bool(b.compound))return false;
    if(a.recordFields){if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!same(a.recordFields->at(i),b.recordFields->at(i)))return false;}
    if(!a.compound)return true;
    const auto& x=*a.compound;const auto& y=*b.compound;
    if(!sameType(x.type.get(),y.type.get()) || x.arrayNullBitmap!=y.arrayNullBitmap ||
       x.dimensions.size()!=y.dimensions.size() || bool(x.arrayElements)!=bool(y.arrayElements))return false;
    for(size_t i=0;i<x.dimensions.size();++i)
        if(x.dimensions[i].length!=y.dimensions[i].length || x.dimensions[i].lower!=y.dimensions[i].lower)return false;
    if(x.arrayElements){if(x.arrayElements->size()!=y.arrayElements->size())return false;
        for(size_t i=0;i<x.arrayElements->size();++i)if(!same(x.arrayElements->at(i),y.arrayElements->at(i)))return false;}
    return true;
}
DatumType scalar(uint32_t actual,uint32_t codec,int16_t size,int32_t modifier=-1) {
    DatumType result;result.actualOid=actual;result.codecOid=codec;result.codecSize=size;
    result.effectiveTypeMod=modifier;result.kind=DatumKind::Scalar;result.hasBinaryOutput=true;return result;
}
ExprValue specimen(std::optional<bool> bitmap) {
    auto integer=std::make_shared<DatumType>(scalar(23,23,4));
    auto record=std::make_shared<DatumType>();record->kind=DatumKind::Record;
    record->actualOid=2249;record->codecOid=2249;record->hasBinaryOutput=true;
    // Component-only synthetic domain identity; no claim of catalog/heap origin.
    record->recordFields={scalar(71001,1700,-1,8),scalar(2206,2206,4),scalar(25,25,-1)};
    ExprValue first("record","(opaque display is deliberately not parsed)");
    first.recordFields=std::vector<ExprValue>{ExprValue("numeric","",true),ExprValue("regtype","opaque-name"),ExprValue("text","NULL")};
    first.recordFields->at(1).objectOid=0;
    first.recordFields->at(2).collation="C";
    auto payload=std::make_shared<CompoundDatum>();payload->type=record;first.compound=payload;
    auto arrayType=std::make_shared<DatumType>();arrayType->kind=DatumKind::Array;
    arrayType->actualOid=2287;arrayType->codecOid=2287;arrayType->arrayElement=record;arrayType->hasBinaryOutput=true;
    auto data=std::make_shared<CompoundDatum>();data->type=arrayType;data->dimensions={{2,-7}};
    data->arrayElements=std::vector<ExprValue>{first,first};
    data->arrayElements->at(1).recordFields->at(1).objectOid=std::numeric_limits<uint32_t>::max();
    data->arrayNullBitmap=bitmap;
    ExprValue result("record[]","opaque unrelated display");result.compound=data;return result;
}
ExprValue roundtrip(const ExprValue& value) {
    sort_detail::File file;sort_detail::Writer writer(file.fd(),7);codec::writeValue(writer,value);writer.flush();
    sort_detail::Reader reader(file.fd(),0,writer.position(),11);auto result=codec::readValue(reader);
    if(reader.remaining())throw DbError("XX000","codec did not consume exact value framing");return result;
}
template<class Write>bool reject(Write write) {
    sort_detail::File file;sort_detail::Writer writer(file.fd(),7);write(writer);writer.flush();
    sort_detail::Reader reader(file.fd(),0,writer.position(),11);
    try{(void)codec::readValue(reader);}catch(const DbError& error){return error.sqlState()=="58030";}
    return false;
}
}
int main() {
    size_t checked=0,failed=0;
    const auto check=[&](const char* label,const auto& action) {
        ++checked;bool good=false;
        try{good=action();}catch(const std::exception& error){std::cerr<<"exception="<<error.what()<<'\n';}
        failed+=!good;std::cout<<"OWNED_COMPOUND_SPILL_CODEC "<<checked<<' '<<label<<" good="<<good<<'\n';
    };
    for(const auto bitmap:{std::optional<bool>{},std::optional<bool>{false},std::optional<bool>{true}})
        check("nested record-array bounds identity and three-state bitmap roundtrip",[&] {
            const auto value=specimen(bitmap);const auto decoded=roundtrip(value);
            return same(value,decoded) && value.compound.get()!=decoded.compound.get() &&
                value.compound->type.get()!=decoded.compound->type.get();
        }); // 1..3
    check("metadata-only SQL NULL does not acquire owning elements",[&] {
        auto value=specimen(true);value.isNull=true;
        auto metadata=std::make_shared<CompoundDatum>();metadata->type=value.compound->type;value.compound=metadata;
        const auto decoded=roundtrip(value);return same(value,decoded) && !decoded.compound->arrayElements;
    }); //4
    check("engaged empty elements remain distinct from missing elements",[&] {
        auto value=specimen(false);auto data=std::make_shared<CompoundDatum>(*value.compound);
        data->arrayElements->clear();data->dimensions.clear();data->arrayNullBitmap=false;value.compound=data;
        const auto decoded=roundtrip(value);return same(value,decoded) && decoded.compound->arrayElements && decoded.compound->arrayElements->empty();
    }); //5
    check("sequential values retain independent type/value frame boundaries",[&] {
        const auto a=specimen({}),b=specimen(true);
        sort_detail::File file;sort_detail::Writer writer(file.fd(),13);
        codec::writeValue(writer,a);codec::writeValue(writer,b);writer.flush();
        sort_detail::Reader reader(file.fd(),0,writer.position(),5);
        const auto x=codec::readValue(reader),y=codec::readValue(reader);
        return same(a,x) && same(b,y) && !reader.remaining();
    }); //6
    check("all four context snapshots retain full owning metadata",[&] {
        const auto value=specimen(true);
        RowContext input(std::map<std::string,ExprValue>{{"UnFolded",value}});
        input.setSqlValue(FunctionCallExpr::SqlValue::CurrentUser,value);
        input.setParameters({value});input.setBoundColumn(11,7,value);
        sort_detail::File file;sort_detail::Writer writer(file.fd(),19);codec::writeContext(writer,input);writer.flush();
        sort_detail::Reader reader(file.fd(),0,writer.position(),5);const auto out=codec::readContext(reader);
        const auto name=RowContextSpillAccess::values(out).find("UnFolded");
        const auto sql=out.sqlValue(FunctionCallExpr::SqlValue::CurrentUser);
        return !reader.remaining() && name!=RowContextSpillAccess::values(out).end() && same(name->second,value) &&
            sql && same(*sql,value) && out.parameterCount()==1 && same(out.parameter(0),value) && same(out.boundColumn(11,7),value);
    }); //7
    check("type layout 1025 fields is preserved with validated exact reserve",[&] {
        ExprValue value("record","opaque");value.recordFields.emplace(1025,ExprValue("integer","1"));
        auto type=std::make_shared<DatumType>();type->actualOid=2249;type->codecOid=2249;type->kind=DatumKind::Record;
        type->recordFields.assign(1025,scalar(23,23,4));auto data=std::make_shared<CompoundDatum>();data->type=type;value.compound=data;
        const auto result=roundtrip(value);
        return same(value,result) && result.compound->type->recordFields.capacity()==1025 && result.recordFields->capacity()==1025 &&
            codec::valueBytes(result)>=sizeof(ExprValue)+1025*sizeof(ExprValue)+1025*sizeof(DatumType);
    }); //8
    check("truncated compound type rejected before declared child allocation",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=h.hasType=1;writer.scalar(h);});
    }); //9
    check("invalid compound presence flag rejected",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=2;writer.scalar(h);});
    }); //10
    check("bitmap without compound rejected",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.bitmapPresent=h.bitmap=1;writer.scalar(h);});
    }); //11
    check("invalid bitmap boolean rejected",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=h.bitmapPresent=1;h.bitmap=2;writer.scalar(h);});
    }); //12
    check("unknown declaration kind rejected",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=h.hasType=1;writer.scalar(h);codec::TypeHeader t{};t.kind=255;writer.scalar(t);});
    }); //13
    check("impossible nested field count rejected before reserve",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=h.hasType=1;writer.scalar(h);codec::TypeHeader t{};t.fields=UINT64_MAX;writer.scalar(t);});
    }); //14
    check("array cardinality does not silently drop a typed value",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=h.hasArray=1;h.dimensions=1;h.arrayValues=1;writer.scalar(h);
            writer.scalar(int32_t(2));writer.scalar(int32_t(-1));writer.scalar(codec::ValueHeader{});});
    }); //15
    check("invalid type output-role flag rejected",[&] {
        return reject([](auto& writer){codec::ValueHeader h{};h.hasCompound=h.hasType=1;writer.scalar(h);codec::TypeHeader t{};t.output=2;writer.scalar(t);});
    }); //16
    if(checked!=16)++failed;
    std::cout<<"OWNED_COMPOUND_SPILL_CODEC_COMPLETE "<<checked<<" FAILURES "<<failed<<'\n';return failed?1:0;
}
