#pragma once
#include "catalog/AnalyzedStatistics.h"
#include "expression/CompoundValueOps.h"
#include "expression/float8_output.inc"
#include <charconv>
#include <cstring>

namespace dbms::analyzed_statistics {
// PostgreSQL FLOAT4 output: shortest round-trip, excluding midpoint endpoints,
// and the float4 fixed/scientific thresholds (-4,6). Locale independent.
inline std::string float4Text(float input) {
    static_assert(sizeof(float)==sizeof(uint32_t)&&std::numeric_limits<float>::is_iec559);
    if(std::isnan(input))return "NaN";
    if(std::isinf(input))return std::signbit(input)?"-Infinity":"Infinity";
    const bool negative=std::signbit(input);
    if(input==0)return negative?"-0":"0";
    const float value=std::fabs(input);
    struct Candidate {float8_output_detail::Decimal decimal;bool roundTrips;};
    const auto candidate=[&](int precision) {
        char buffer[64];const auto encoded=precision?
            std::to_chars(buffer,buffer+sizeof(buffer),value,std::chars_format::general,precision):
            std::to_chars(buffer,buffer+sizeof(buffer),value,std::chars_format::general);
        if(encoded.ec!=std::errc())throw DbError("XX000","could not format statistics float4");
        uint64_t coefficient=0;int fraction=0,exponent=0;bool dot=false;
        const char* cursor=buffer;
        for(;cursor!=encoded.ptr&&*cursor!='e';++cursor) {
            if(*cursor=='.'){dot=true;continue;}
            coefficient=coefficient*10+unsigned(*cursor-'0');fraction+=dot;
        }
        if(cursor!=encoded.ptr) {
            ++cursor;if(*cursor=='+')++cursor;
            const auto parsed=std::from_chars(cursor,encoded.ptr,exponent);
            if(parsed.ec!=std::errc()||parsed.ptr!=encoded.ptr)throw DbError("XX000","invalid statistics float4 exponent");
        }
        float parsed=0;const auto decoded=std::from_chars(buffer,encoded.ptr,parsed);
        return Candidate{{coefficient,exponent-fraction,true},decoded.ec==std::errc()&&decoded.ptr==encoded.ptr&&parsed==value};
    };
    uint32_t bits=0;std::memcpy(&bits,&value,4);const auto fraction=bits&((1U<<23)-1),biased=(bits>>23)&255;
    const uint64_t mantissa=fraction|(biased?1U<<23:0);
    const int exponent=int(biased?biased:1)-127-23-2;
    const uint64_t lower=4*mantissa-(fraction==0&&biased>1?1:2),upper=4*mantissa+2;
    const auto boundary=[&](const auto& decimal){return float8_output_detail::equalsMidpoint(decimal,lower,exponent)||float8_output_detail::equalsMidpoint(decimal,upper,exponent);};
    auto decimal=candidate(0).decimal;
    if(boundary(decimal)) {
        bool found=false;
        for(int precision=1;precision<=std::numeric_limits<float>::max_digits10;++precision) {
            const auto next=candidate(precision);
            if(next.roundTrips&&!boundary(next.decimal)){decimal=next.decimal;found=true;break;}
        }
        if(!found)throw DbError("XX000","no interior statistics float4 display");
    }
    while(decimal.coefficient%10==0){decimal.coefficient/=10;++decimal.scale;}
    const auto digits=std::to_string(decimal.coefficient);const int length=digits.size(),scientific=length+decimal.scale-1;
    std::string text=negative?"-":"";
    if(scientific>=-4&&scientific<6) {
        const int point=length+decimal.scale;
        if(point<=0)text+="0."+std::string(-point,'0')+digits;
        else if(point>=length)text+=digits+std::string(point-length,'0');
        else text+=digits.substr(0,point)+"."+digits.substr(point);
    } else {
        text+=digits.front();if(length>1)text+='.'+digits.substr(1);
        text+=scientific<0?"e-":"e+";const auto exp=std::to_string(std::abs(scientific));
        if(exp.size()<2)text+='0';
        text+=exp;
    }
    return text;
}
inline double scalarFloat(const std::string& text) {
    double value=0;const auto parsed=std::from_chars(text.data(),text.data()+text.size(),value);
    if(parsed.ec!=std::errc()||parsed.ptr!=text.data()+text.size())throw DbError("XX001","invalid actual statistics floating datum");
    return value;
}
// Only existing owning structure is traversed. No display string is reparsed
// to manufacture record fields, array dimensions, elements or NULL flags.
inline ExprValue outputValue(const ExprValue& input,const DatumTypePtr& type,size_t depth=0) {
    if(depth>128)throw DbError("54001","statistics output nesting limit exceeded");
    ExprValue result=input;if(result.isNull)return result;
    if(!type)throw DbError("XX001","missing statistics value declaration");
    if(type->kind==DatumKind::Array) {
        const auto& actual=input.compound;
        if(!actual || !actual->type || !actual->arrayElements || !type->arrayElement ||
           actual->type->actualOid!=type->actualOid)
            throw DbError("XX001","missing actual statistics array structure");
        auto data=std::make_shared<CompoundDatum>(*actual);data->arrayElements->clear();
        for(const auto& cell:*actual->arrayElements)data->arrayElements->push_back(outputValue(cell,type->arrayElement,depth+1));
        result.value=compound_value_detail::render(*data->arrayElements,data->dimensions);result.compound=std::move(data);
    } else if(type->kind==DatumKind::Scalar && type->codecOid==700)result.value=float4Text(float(scalarFloat(result.value)));
    else if(type->kind==DatumKind::Scalar && type->codecOid==701)result.value=float8_output_detail::format(scalarFloat(result.value));
    return result;
}
inline ExprValue arrayOutput(const char* spelling,const DatumTypePtr& declaration,
    const DatumTypePtr& element,const std::vector<ExprValue>& values) {
    if(!declaration||declaration->kind!=DatumKind::Array||!element||values.size()>INT32_MAX)
        throw DbError("XX001","invalid statistics array output declaration");
    if(declaration->codecOid!=2277 && (!declaration->arrayElement || declaration->arrayElement->actualOid!=element->actualOid))
        throw DbError("XX001","statistics fixed array element identity mismatch");
    auto actual=std::make_shared<DatumType>(*declaration);actual->arrayElement=element;
    auto data=std::make_shared<CompoundDatum>();data->type=std::move(actual);data->arrayElements.emplace();
    if(!values.empty())data->dimensions.push_back({int32_t(values.size()),1});
    bool null=false;
    for(const auto& value:values){data->arrayElements->push_back(outputValue(value,element));null|=value.isNull;}
    data->arrayNullBitmap=null;
    ExprValue result(spelling,compound_value_detail::render(*data->arrayElements,data->dimensions),false);result.compound=std::move(data);return result;
}
struct OutputTypes {DatumTypePtr anyarray,numbers,real;};
inline std::vector<ExprValue> project(const Column& column,const std::string& schema,
    const std::string& table,const std::string& attribute,const OutputTypes& types) {
    std::map<Kind,const Slot*> slots;for(const auto& slot:column.slots)slots.emplace(slot.kind,&slot);
    const auto find=[&](Kind kind)->const Slot*{const auto p=slots.find(kind);return p==slots.end()?nullptr:p->second;};
    const auto values=[&](Kind kind){const auto* s=find(kind);return s&&s->values?
        arrayOutput("anyarray",types.anyarray,s->values->elementType,s->values->cells):ExprValue("anyarray","",true);};
    const auto numbers=[&](Kind kind){const auto* s=find(kind);if(!s||!s->numbers)return ExprValue("real[]","",true);
        std::vector<ExprValue> cells;cells.reserve(s->numbers->size());for(float n:*s->numbers)cells.emplace_back("real",float4Text(n),false);
        return arrayOutput("real[]",types.numbers,types.real,cells);};
    const auto first=[&](Kind kind){const auto* s=find(kind);return s&&s->numbers&&!s->numbers->empty()?
        ExprValue("real",float4Text(s->numbers->front()),false):ExprValue("real","",true);};
    return {ExprValue("name",schema,false),ExprValue("name",table,false),ExprValue("name",attribute,false),
        ExprValue("boolean","f",false),ExprValue("real",float4Text(column.nullFraction),false),
        ExprValue("integer",std::to_string(column.averageWidth),false),ExprValue("real",float4Text(column.distinctEstimate),false),
        values(Kind::MostCommon),numbers(Kind::MostCommon),values(Kind::Histogram),first(Kind::Correlation),
        values(Kind::CommonElements),numbers(Kind::CommonElements),numbers(Kind::ElementCounts),
        values(Kind::RangeLength),first(Kind::RangeLength),values(Kind::RangeBounds)};
}
} // namespace dbms::analyzed_statistics
