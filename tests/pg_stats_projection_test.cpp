#include "catalog/PgStatsProjection.h"
#include <iostream>
#include <stdexcept>
#include <cstring>
namespace {
using namespace dbms;namespace stats=analyzed_statistics;
size_t checks=0;
void need(bool b,const char* m){++checks;if(!b)throw std::runtime_error(m);}
DatumTypePtr type(uint32_t oid,DatumKind kind=DatumKind::Scalar,DatumTypePtr element={}) {
    auto t=std::make_shared<DatumType>();t->actualOid=t->codecOid=oid;t->kind=kind;t->hasBinaryOutput=true;t->arrayElement=std::move(element);return t;
}
}
int main(){try{
    using namespace dbms;namespace stats=analyzed_statistics;
    const auto i=type(23),real=type(700),text=type(25),r=type(3904),length=type(701);
    const auto any=type(2277,DatumKind::Array),numbers=type(1021,DatumKind::Array,real);
    const stats::OutputTypes output{any,numbers,real};
    stats::Column c;c.attributeType=i;c.sampleRows=4;c.nullRows=1;c.totalRows=4;c.nullFraction=.25f;c.averageWidth=4;c.distinctEstimate=-.5f;
    stats::Slot m;m.kind=stats::Kind::MostCommon;m.numbers=std::vector<float>{.5f};m.values=stats::Values{i,{ExprValue("integer","1",false)}};
    stats::Slot h;h.kind=stats::Kind::Histogram;h.values=stats::Values{i,{ExprValue("integer","2",false),ExprValue("integer","10",false)}};
    stats::Slot cor;cor.kind=stats::Kind::Correlation;cor.numbers=std::vector<float>{-.25f};c.slots={m,h,cor};
    auto row=stats::project(c,"real schema","real table","real attribute",output);
    need(row.size()==17,"all seventeen fields");need(row[0].value=="real schema"&&row[1].value=="real table"&&row[2].value=="real attribute","actual labels unchanged");
    need(row[4].value=="0.25"&&row[5].value=="4"&&row[6].value=="-0.5","real scalar fields");
    need(row[7].value=="{1}"&&row[8].value=="{0.5}"&&row[9].value=="{2,10}"&&row[10].value=="-0.25","scalar slot projections");
    for(size_t n=11;n<17;++n)need(row[n].isNull,"absent slots remain SQLNULL");
    need(row[7].compound->type->actualOid==2277&&row[7].compound->type->arrayElement->actualOid==23,"output-only anyarray has real integer element");
    need(!any->arrayElement,"static anyarray declaration remains element-independent");
    c.slots.clear();row=stats::project(c,"s","t","a",output);for(size_t n=7;n<17;++n)need(row[n].isNull,"no slot is not a fake empty array");
    m.values->cells.clear();m.numbers=std::vector<float>{};c.slots={m};row=stats::project(c,"s","t","a",output);
    need(!row[7].isNull&&row[7].value=="{}"&&row[7].compound->arrayElements->empty()&&row[7].compound->dimensions.empty(),"present-empty owning values distinct from absent");
    need(!row[8].isNull&&row[8].value=="{}","present-empty number array distinct from absent");
    const auto array=type(1007,DatumKind::Array,i);auto data=std::make_shared<CompoundDatum>();data->type=array;data->dimensions={{2,-3}};data->arrayNullBitmap=true;data->arrayElements=std::vector<ExprValue>{ExprValue("integer","2",false),ExprValue("integer","",true)};
    ExprValue a("integer[]","[-3:-2]={2,NULL}",false);a.compound=data;
    m.values=stats::Values{array,{a}};c.slots={m};row=stats::project(c,"s","t","a",output);
    const auto& inner=row[7].compound->arrayElements->front();need(row[7].compound->type->arrayElement->actualOid==1007&&inner.compound->type->actualOid==1007,"array-valued MCV retains array element identity, no invented outer concrete OID");
    need(inner.compound->dimensions.front().lower==-3&&inner.compound->arrayElements->at(1).isNull&&inner.compound->arrayNullBitmap==true,"owning inner dimensions, bounds, NULL and bitmap retained");
    m.values=stats::Values{text,{ExprValue("text","",false),ExprValue("text","NULL",false),ExprValue("text","a,b",false),ExprValue("text","界",false),ExprValue("text","",true)}};c.slots={m};row=stats::project(c,"s","t","a",output);
    need(row[7].value=="{\"\",\"NULL\",\"a,b\",界,NULL}","empty, literalNULL, delimiter, UTF8 and SQLNULL remain distinct");
    stats::Slot l;l.kind=stats::Kind::RangeLength;l.numbers=std::vector<float>{.5f};l.values=stats::Values{length,{ExprValue("double precision","0.80000000000000004",false),ExprValue("double precision","Infinity",false)}};
    stats::Slot b;b.kind=stats::Kind::RangeBounds;b.values=stats::Values{r,{ExprValue("int4range","[1,4)",false)}};c.slots={l,b};row=stats::project(c,"s","t","a",output);
    need(row[14].value=="{0.8,Infinity}"&&row[15].value=="0.5"&&row[16].value=="{\"[1,4)\"}","range slot values, float8 output and empty fraction");
    need(row[14].compound->type->arrayElement->actualOid==701&&row[16].compound->type->arrayElement->actualOid==3904,"range slots preserve different actual element types");
    l.values.reset();l.numbers=std::vector<float>{1};c.slots={l};row=stats::project(c,"s","t","a",output);need(row[14].isNull&&row[15].value=="1"&&row[16].isNull,"all-empty range actualNULL length values and present frequency");
    for(const auto& p:std::vector<std::pair<float,std::string>>{{0,"0"},{-0.0f,"-0"},{.25f,"0.25"},{1.0f/3,"0.33333334"},{1e6f,"1e+06"},{1e-5f,"1e-05"},{1e-4f,"0.0001"},{100000.f,"100000"}})need(stats::float4Text(p.first)==p.second,"PG float4 thresholds and shortest output");
    uint32_t bits=0x67c29b31;
    for(size_t n=0;n<512;++n) {
        bits=bits*1664525+1013904223;float value=0;std::memcpy(&value,&bits,4);if(!std::isfinite(value))continue;
        const auto text=stats::float4Text(value);float decoded=0;const auto result=std::from_chars(text.data(),text.data()+text.size(),decoded);
        need(result.ec==std::errc()&&result.ptr==text.data()+text.size()&&decoded==value,"every deterministic finite output round trips as float4");
        std::cout<<"PG_STATS_FLOAT4 "<<bits<<' '<<text<<'\n';
    }
    std::cout<<"PG_STATS_PROJECTION checks="<<checks<<" failures=0\n";return 0;
}catch(const std::exception& e){std::cerr<<"PG_STATS_PROJECTION_FAIL checks="<<checks<<" "<<e.what()<<'\n';return 1;}}
