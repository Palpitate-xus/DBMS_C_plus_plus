#pragma once

#include "catalog/AnalyzedStatistics.h"
#include "common/DbError.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <string_view>

namespace dbms::analyzed_statistics {
namespace codec {
inline constexpr size_t maxBytes = 128 * 1024 * 1024;
inline constexpr uint64_t maxNodes = 1000000;
inline constexpr size_t maxDepth = 128;
inline constexpr char magic[] = "\0DBMS_ANALYZED_STATS";
inline constexpr uint32_t version = 1;
static_assert(sizeof(float)==4 && sizeof(double)==8 &&
    std::numeric_limits<float>::is_iec559 && std::numeric_limits<double>::is_iec559,
    "statistics snapshots require IEEE binary32/binary64");
[[noreturn]] inline void invalid(const char* message) {
    throw DbError("XX001",std::string("invalid analyzed statistics snapshot: ")+message);
}
[[noreturn]] inline void limit(const char* message) {
    throw DbError("54000",std::string("analyzed statistics snapshot exceeds ")+message);
}
inline uint64_t checksum(std::string_view bytes) {
    uint64_t h=14695981039346656037ULL;
    for(unsigned char c:bytes){h^=c;h*=1099511628211ULL;}
    return h;
}
inline void valid(const Key& key,const Column& c) {
    if(!key.relation || key.attribute<=0 || !c.attributeType ||
       !c.attributeType->actualOid || c.nullRows>c.sampleRows ||
       !std::isfinite(c.totalRows) || c.totalRows<0 || c.target<0 ||
       !std::isfinite(c.nullFraction) || c.nullFraction<0 || c.nullFraction>1 ||
       c.averageWidth<0 || !std::isfinite(c.distinctEstimate) ||
       c.distinctEstimate < -1 || c.slots.size()>5) invalid("column fields");
    unsigned kinds=0;
    for(const auto& slot:c.slots) {
        const auto kind=static_cast<uint16_t>(slot.kind);
        if(kind<1 || kind>7 || (kinds & (1u<<kind)))invalid("slot kind");
        kinds|=1u<<kind;
        if(slot.numbers)for(float x:*slot.numbers)
            if(!std::isfinite(x))invalid("slot number");
        if(slot.values && (!slot.values->elementType ||
                          !slot.values->elementType->actualOid))invalid("slot element declaration");
    }
}
inline void dimensions(const CompoundDatum& d) {
    if(d.dimensions.size()>6)invalid("array dimensions");
    uint64_t count=d.dimensions.empty()?0:1;
    for(const auto& x:d.dimensions) {
        if(x.length<=0 || int64_t(x.lower)+x.length-1>=INT32_MAX ||
           count>maxNodes/static_cast<uint32_t>(x.length))invalid("array extent");
        count*=static_cast<uint32_t>(x.length);
    }
    if(d.arrayElements) {
        if(count!=d.arrayElements->size())invalid("array cardinality");
    } else if(count || d.arrayNullBitmap)invalid("array payload absent");
}
class Writer {
    std::string bytes_;
    uint64_t nodes_=0;
    std::vector<const DatumType*> types_;
    std::vector<const CompoundDatum*> compounds_;
    void node(size_t depth) {
        if(depth>maxDepth || ++nodes_>maxNodes)limit("structure limit");
    }
public:
    void raw(std::string_view data) {
        if(data.size()>maxBytes-bytes_.size())limit("byte limit");
        bytes_.append(data.data(),data.size());
    }
    void u(uint64_t value,size_t width) {
        char b[8];for(size_t i=0;i<width;++i){b[i]=static_cast<char>(value&255);value>>=8;}
        raw({b,width});
    }
    void flag(bool b){u(b?1:0,1);}
    void text(const std::string& s) {
        if(s.size()>maxBytes)limit("string length");
        u(s.size(),4);raw(s);
    }
    void f4(float x){uint32_t u32=0;std::memcpy(&u32,&x,4);u(u32,4);}
    void f8(double x){uint64_t u64=0;std::memcpy(&u64,&x,8);u(u64,8);}
    void count(size_t n){if(n>maxNodes)limit("count limit");u(n,4);}
    void type(const DatumType& t,size_t depth=0) {
        node(depth);
        if(static_cast<uint8_t>(t.kind)>static_cast<uint8_t>(DatumKind::Array) ||
           std::find(types_.begin(),types_.end(),&t)!=types_.end())invalid("type structure");
        types_.push_back(&t);
        u(t.actualOid,4);u(t.codecOid,4);u(static_cast<uint16_t>(t.codecSize),2);
        u(static_cast<uint32_t>(t.effectiveTypeMod),4);u(static_cast<uint8_t>(t.kind),1);
        flag(t.enumType);flag(t.hasBinaryOutput);flag(bool(t.arrayElement));
        if(t.arrayElement)type(*t.arrayElement,depth+1);
        count(t.recordFields.size());for(const auto& f:t.recordFields)type(f,depth+1);
        types_.pop_back();
    }
    void value(const ExprValue& v,size_t depth=0) {
        node(depth);text(v.typeName);text(v.value);text(v.collation);flag(v.isNull);
        flag(v.objectOid.has_value());if(v.objectOid)u(*v.objectOid,4);
        count(v.elementOids.size());for(const auto& x:v.elementOids){flag(x.has_value());if(x)u(*x,4);}
        flag(v.recordFields.has_value());
        if(v.recordFields){count(v.recordFields->size());for(const auto& f:*v.recordFields)value(f,depth+1);}
        flag(bool(v.compound));
        if(v.compound) {
            const auto& d=*v.compound;dimensions(d);
            if(std::find(compounds_.begin(),compounds_.end(),&d)!=compounds_.end())invalid("value cycle");
            compounds_.push_back(&d);
            flag(bool(d.type));if(d.type)type(*d.type,depth+1);
            count(d.dimensions.size());for(const auto& x:d.dimensions){u(static_cast<uint32_t>(x.length),4);u(static_cast<uint32_t>(x.lower),4);}
            u(d.arrayNullBitmap?(*d.arrayNullBitmap?2:1):0,1);
            flag(d.arrayElements.has_value());
            if(d.arrayElements){count(d.arrayElements->size());for(const auto& f:*d.arrayElements)value(f,depth+1);}
            compounds_.pop_back();
        }
    }
    std::string finish(){return std::move(bytes_);}
};
class Reader {
    std::string_view bytes_;
    size_t offset_=0;
    uint64_t nodes_=0;
    void node(size_t depth){if(depth>maxDepth || ++nodes_>maxNodes)invalid("structure limit");}
public:
    explicit Reader(std::string_view b):bytes_(b){}
    size_t remaining()const{return bytes_.size()-offset_;}
    std::string_view raw(size_t n) {
        if(n>remaining())invalid("truncated data");
        const auto s=bytes_.substr(offset_,n);offset_+=n;return s;
    }
    uint64_t u(size_t width) {
        const auto b=raw(width);uint64_t result=0;
        for(size_t i=0;i<width;++i)result|=uint64_t(static_cast<unsigned char>(b[i]))<<(8*i);
        return result;
    }
    bool flag(){const auto b=u(1);if(b>1)invalid("boolean flag");return b!=0;}
    int32_t i4(){const auto bits=static_cast<uint32_t>(u(4));int32_t x;std::memcpy(&x,&bits,4);return x;}
    int16_t i2(){const auto bits=static_cast<uint16_t>(u(2));int16_t x;std::memcpy(&x,&bits,2);return x;}
    float f4(){const auto bits=static_cast<uint32_t>(u(4));float x;std::memcpy(&x,&bits,4);return x;}
    double f8(){const auto bits=u(8);double x;std::memcpy(&x,&bits,8);return x;}
    std::string text(){return std::string(raw(static_cast<size_t>(u(4))));}
    size_t count(size_t minimum,size_t maximum=maxNodes) {
        const auto n=u(4);if(n>maximum || (minimum && n>remaining()/minimum))invalid("count exceeds frame");
        return static_cast<size_t>(n);
    }
    DatumTypePtr type(size_t depth=0) {
        node(depth);auto t=std::make_shared<DatumType>();t->actualOid=static_cast<uint32_t>(u(4));
        t->codecOid=static_cast<uint32_t>(u(4));t->codecSize=i2();t->effectiveTypeMod=i4();
        const auto kind=u(1);if(kind>static_cast<uint8_t>(DatumKind::Array))invalid("type kind");
        t->kind=static_cast<DatumKind>(kind);t->enumType=flag();t->hasBinaryOutput=flag();
        if(flag())t->arrayElement=type(depth+1);
        const auto n=count(22);t->recordFields.reserve(n);
        for(size_t i=0;i<n;++i)t->recordFields.push_back(*type(depth+1));
        return t;
    }
    ExprValue value(size_t depth=0) {
        node(depth);ExprValue v;v.typeName=text();v.value=text();v.collation=text();v.isNull=flag();
        if(flag())v.objectOid=static_cast<uint32_t>(u(4));
        const auto n=count(1);v.elementOids.reserve(n);
        for(size_t i=0;i<n;++i){if(flag())v.elementOids.emplace_back(static_cast<uint32_t>(u(4)));else v.elementOids.emplace_back();}
        if(flag()){const auto countFields=count(20);v.recordFields.emplace();v.recordFields->reserve(countFields);for(size_t i=0;i<countFields;++i)v.recordFields->push_back(value(depth+1));}
        if(flag()) {
            auto d=std::make_shared<CompoundDatum>();if(flag())d->type=type(depth+1);
            const auto nd=count(8,6);for(size_t i=0;i<nd;++i){const int32_t len=i4(),lower=i4();d->dimensions.push_back({len,lower});}
            const auto bitmap=u(1);if(bitmap>2)invalid("array bitmap flag");if(bitmap)d->arrayNullBitmap=bitmap==2;
            if(flag()){const auto na=count(20);d->arrayElements.emplace();d->arrayElements->reserve(na);for(size_t i=0;i<na;++i)d->arrayElements->push_back(value(depth+1));}
            dimensions(*d);v.compound=std::move(d);
        }
        return v;
    }
};
} // namespace codec

inline std::string encode(const Snapshot& snapshot) {
    codec::Writer body;body.u(snapshot.generation,8);body.count(snapshot.columns.size());
    for(const auto& entry:snapshot.columns) {
        const auto& key=entry.first;const auto& c=entry.second;codec::valid(key,c);
        body.u(key.relation,4);body.u(static_cast<uint16_t>(key.attribute),2);body.flag(key.inherited);
        body.u(c.physicalRelation,8);body.u(c.schemaGeneration,8);body.type(*c.attributeType);
        body.u(c.collation,4);body.u(static_cast<uint32_t>(c.target),4);
        body.u(c.sampleRows,8);body.u(c.nullRows,8);body.f8(c.totalRows);
        body.f4(c.nullFraction);body.u(static_cast<uint32_t>(c.averageWidth),4);body.f4(c.distinctEstimate);
        body.count(c.slots.size());
        for(const auto& slot:c.slots) {
            body.u(static_cast<uint16_t>(slot.kind),2);body.u(slot.comparisonOperator,4);body.u(slot.collation,4);
            body.flag(slot.numbers.has_value());
            if(slot.numbers){body.count(slot.numbers->size());for(float x:*slot.numbers)body.f4(x);}
            body.flag(slot.values.has_value());
            if(slot.values){body.type(*slot.values->elementType);body.count(slot.values->cells.size());for(const auto& v:slot.values->cells)body.value(v);}
        }
    }
    const auto payload=body.finish();codec::Writer frame;
    frame.raw({codec::magic,sizeof(codec::magic)});frame.u(codec::version,4);frame.u(payload.size(),8);frame.raw(payload);
    auto bytes=frame.finish();const auto hash=codec::checksum(bytes);codec::Writer trailer;trailer.u(hash,8);bytes+=trailer.finish();
    if(bytes.size()>codec::maxBytes)codec::limit("byte limit");
    return bytes;
}
inline Snapshot decode(std::string_view bytes) {
    const size_t header=sizeof(codec::magic)+4+8;
    if(bytes.size()>codec::maxBytes || bytes.size()<header+8)codec::invalid("frame size");
    codec::Reader frame(bytes);if(frame.raw(sizeof(codec::magic))!=std::string_view(codec::magic,sizeof(codec::magic)))codec::invalid("magic");
    if(frame.u(4)!=codec::version)codec::invalid("version");
    const auto size=frame.u(8);if(size!=frame.remaining()-8)codec::invalid("payload length");
    codec::Reader r(frame.raw(static_cast<size_t>(size)));const auto stored=frame.u(8);
    if(stored!=codec::checksum(bytes.substr(0,bytes.size()-8)))codec::invalid("checksum");
    Snapshot result;result.generation=r.u(8);const auto n=r.count(80);
    for(size_t i=0;i<n;++i) {
        Key key;key.relation=static_cast<uint32_t>(r.u(4));key.attribute=r.i2();key.inherited=r.flag();Column c;
        c.physicalRelation=r.u(8);c.schemaGeneration=r.u(8);c.attributeType=r.type();
        c.collation=static_cast<uint32_t>(r.u(4));c.target=r.i4();c.sampleRows=r.u(8);c.nullRows=r.u(8);
        c.totalRows=r.f8();c.nullFraction=r.f4();c.averageWidth=r.i4();c.distinctEstimate=r.f4();
        const auto ns=r.count(12,5);c.slots.reserve(ns);
        for(size_t j=0;j<ns;++j) {
            Slot slot;slot.kind=static_cast<Kind>(r.u(2));slot.comparisonOperator=static_cast<uint32_t>(r.u(4));slot.collation=static_cast<uint32_t>(r.u(4));
            if(r.flag()){const auto nn=r.count(4);slot.numbers.emplace();slot.numbers->reserve(nn);for(size_t k=0;k<nn;++k)slot.numbers->push_back(r.f4());}
            if(r.flag()){Values values;values.elementType=r.type();const auto nv=r.count(20);values.cells.reserve(nv);for(size_t k=0;k<nv;++k)values.cells.push_back(r.value());slot.values=std::move(values);}
            c.slots.push_back(std::move(slot));
        }
        codec::valid(key,c);if(!result.columns.emplace(key,std::move(c)).second)codec::invalid("duplicate key");
    }
    if(r.remaining())codec::invalid("trailing payload");
    return result;
}
} // namespace dbms::analyzed_statistics
