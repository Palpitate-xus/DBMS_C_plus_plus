#include "commands/AnalyzedRangeInput.h"
#include "catalog/AnalyzedStatisticsCodec.h"
#include <iostream>
#include <stdexcept>

namespace {
using namespace dbms;
namespace stats = analyzed_statistics;
namespace input = stats::range_input;
size_t checks = 0;
void require(bool good,const char* message) { ++checks;if(!good)throw std::runtime_error(message); }
DatumTypePtr type(uint32_t oid,int16_t size=-1,uint32_t codec=0) {
    auto t=std::make_shared<DatumType>();t->actualOid=oid;t->codecOid=codec?codec:oid;
    t->codecSize=size;t->hasBinaryOutput=true;return t;
}
stats::Column column(size_t rows,uint32_t oid=3904,int target=2) {
    stats::Column c;c.attributeType=type(oid);c.totalRows=rows;c.target=target;return c;
}
using Samples=std::vector<stats::RangeSample>;
Samples rows(uint32_t oid,const std::vector<std::optional<std::string>>& values,bool prepared=false) {
    Samples result;const auto spec=*input::builtin(oid);
    for(const auto& value:values)result.push_back(input::decode(spec,value.value_or(""),!value,
        result.size(),value?value->size():0,prepared));
    return result;
}
std::optional<stats::Column> collect(const Samples& sample,uint32_t oid=3904,int target=2,
                                   const std::function<void()>& interrupt={}) {
    const auto spec=*input::builtin(oid);
    return stats::collectRangeStatistics(column(sample.size(),oid,target),sample,type(oid),type(701,8),
        [&](const auto& a,const auto& b){return input::compare(spec,a,b);},
        [&](const auto& upper,const auto& lower){return input::distance(spec,upper,lower);},
        [&](const auto& lower,const auto& upper){return input::construct(spec,lower,upper);},interrupt);
}
const stats::Slot* slot(const stats::Column& c,stats::Kind kind) {
    for(const auto& s:c.slots)if(s.kind==kind)return &s;
    return nullptr;
}
std::vector<std::string> values(const stats::Column& c,stats::Kind kind) {
    std::vector<std::string> result;
    if(const auto* s=slot(c,kind))if(s->values)for(const auto& v:s->values->cells)result.push_back(v.value);
    return result;
}
std::string frame(const stats::Column& c) {
    stats::Snapshot snapshot;snapshot.generation=1;snapshot.columns.emplace(stats::Key{1,1,false},c);
    return stats::encode(snapshot);
}
template<class F>void rejected(F action,const std::string& state="XX001") {
    bool good=false;try{action();}catch(const DbError& e){good=e.sqlState()==state;}
    require(good,"invalid input or subdiff range failure retains its actual SQLSTATE");
}
} // namespace

int main() {
    try {
        auto sample=rows(3904,{"[1,3)","[10,30)","empty",{},"(,20)","[3,)"});
        const uint32_t widths[]={20,24,9,UINT32_MAX,12,16};
        for(size_t i=0;i<sample.size();++i)sample[i].storedWidth=widths[i];
        auto c=*collect(sample);
        require(c.sampleRows==6&&c.nullRows==1&&c.nullFraction==float(1.0/6)&&c.averageWidth==16,
            "range width uses actual non-NULL stored spans, not display text");
        require(c.distinctEstimate==float(-(1.0-c.nullFraction))&&c.slots.size()==2,
            "range distinct is PG's negative non-NULL fraction, without scalar MCV/correlation");
        require(values(c,stats::Kind::RangeBounds)==std::vector<std::string>({"(,3)","[1,20)","[10,)"}),
            "separately sorted lower and upper quantiles form real ranges");
        require(values(c,stats::Kind::RangeLength)==std::vector<std::string>({"2","20","Infinity"}),
            "nonempty range length quantiles include unbounded infinity");
        require(slot(c,stats::Kind::RangeBounds)->comparisonOperator==0&&
                slot(c,stats::Kind::RangeBounds)->values->elementType->actualOid==3904&&
                slot(c,stats::Kind::RangeLength)->comparisonOperator==672&&
                slot(c,stats::Kind::RangeLength)->values->elementType->actualOid==701&&
                *slot(c,stats::Kind::RangeLength)->numbers==std::vector<float>{.2f},
            "range slot roles, actual element declarations and empty denominator");
        std::reverse(sample.begin(),sample.end());
        require(frame(*collect(sample))==frame(c),"reservoir position cannot change range statistics");
        const auto decoded=stats::decode(frame(c));
        require(stats::encode(decoded)==frame(c),"range slots preserve complete snapshot framing and special values");

        sample=rows(3904,{"empty",{},{} });c=*collect(sample);
        require(c.nullFraction==float(2.0/3)&&c.averageWidth==5&&
                slot(c,stats::Kind::RangeLength)->numbers->front()==1&&
                !slot(c,stats::Kind::RangeLength)->values&&!slot(c,stats::Kind::RangeBounds),
            "all-empty non-NULL population has no fabricated length values or bounds");
        sample=rows(3904,{"[1,2)"});c=*collect(sample);
        require(c.distinctEstimate==-1&&!slot(c,stats::Kind::RangeBounds)&&
                !slot(c,stats::Kind::RangeLength)->values&&slot(c,stats::Kind::RangeLength)->numbers->front()==0,
            "single nonempty range has empty fraction but no histogram");
        sample=rows(3904,{"[1,2)","[1,2)"});c=*collect(sample,3904,1);
        require(c.distinctEstimate==-1&&values(c,stats::Kind::RangeBounds)==std::vector<std::string>({"[1,2)","[1,2)"})&&
                values(c,stats::Kind::RangeLength)==std::vector<std::string>({"1","1"}),
            "duplicate ranges still use range statistics; target one keeps two endpoints");
        sample=rows(3904,{{},{},{}});c=*collect(sample);
        require(c.nullFraction==1&&c.averageWidth==0&&c.distinctEstimate==0&&c.slots.empty(),
            "all-NULL range column has no invented slots");
        require(!collect({})&&!collect(rows(3904,{"[1,2)"}),3904,0),
            "empty table and target zero do not produce a statistics row");

        sample=rows(3906,{"[1,2]","(1,2]","[1,2)","(1,2)"});c=*collect(sample,3906,100);
        require(values(c,stats::Kind::RangeBounds)==std::vector<std::string>({"[1,2)","[1,2)","(1,2]","(1,2]"}),
            "equal bound values retain distinct inclusive and exclusive ordering");
        const auto spec=*input::builtin(3906);
        const auto cmp=[&](const auto& a,const auto& b){return input::compare(spec,a,b);};
        require(stats::compareRangeBounds(sample[0].lower,sample[1].lower,cmp)<0&&
                stats::compareRangeBounds(sample[2].upper,sample[0].upper,cmp)<0,
            "lower and upper inclusivity ties order in opposite directions");
        auto same=rows(3906,{"[1,1]","[2,2]"});
        require(stats::compareRangeBounds(same[0].lower,same[0].upper,cmp)==0,
            "a point range has equal inclusive lower and upper bounds");

        sample=rows(3926,{"[9007199254740992,9007199254740993)","[-9223372036854775808,9223372036854775807)"});
        c=*collect(sample,3926);const auto lengths=values(c,stats::Kind::RangeLength);
        require(lengths.size()==2&&std::stod(lengths[0])==0&&
                std::stod(lengths[1])==double(INT64_MAX)-double(INT64_MIN),
            "int8 subdiff converts both endpoints before subtracting without signed overflow");
        sample=rows(3906,{"[10000000000000000000000000000,10000000000000000000000000001)","[0,2)"});
        c=*collect(sample,3906);
        require(values(c,stats::Kind::RangeLength)==std::vector<std::string>({"1","2"}),
            "numeric subdiff subtracts exact large bounds before float8 conversion");
        sample=rows(3908,{"[\"2000-01-01 00:00:00.1\",\"2000-01-01 00:00:00.9\")",
                         "[\"9999-01-01 00:00:00\",\"9999-01-01 00:00:00.000001\")"});
        c=*collect(sample,3908);
        require(std::stod(values(c,stats::Kind::RangeLength)[0])==0&&
                std::stod(values(c,stats::Kind::RangeLength)[1])==.8,
            "timestamp length uses PG epoch microseconds and float8 operand rounding");
        const auto tz=rows(3910,{"[\"2000-01-01 01:00:00+01\",\"2000-01-01 01:00:01+01\")"},true);
        require(tz[0].lower.integer==0&&tz[0].upper.integer==1000000,
            "timestamptz bounds retain absolute instants");
        const auto without=rows(3908,{"[\"2000-01-01 00:00:00+05\",\"2000-01-01 00:00:01+05\")"},true);
        require(without[0].lower.integer==0&&without[0].upper.integer==1000000,
            "timestamp without time zone retains its physical wall-clock semantics");
        const auto rounded=rows(3908,{"[\"2000-01-01 00:00:00.0000005\",\"2000-01-01 00:00:00.0000015\")"},true);
        require(rounded[0].lower.integer==0&&rounded[0].upper.integer==2,
            "stored timestamp input rounds microsecond ties to even");
        sample=rows(3912,{"[2000-01-01,2000-01-02)","[2000-01-01,2000-01-11)"});c=*collect(sample,3912);
        require(values(c,stats::Kind::RangeLength)==std::vector<std::string>({"1","10"}),"daterange subdiff is in days");
        sample=rows(3908,{"[-infinity,infinity]","(,)"});c=*collect(sample,3908);
        require(std::isfinite(std::stod(values(c,stats::Kind::RangeLength)[0]))&&
                values(c,stats::Kind::RangeLength)[1]=="Infinity",
            "literal timestamp infinity values differ from unbounded range endpoints");
        sample=rows(3906,{"[NaN,NaN]","[0,1)"});c=*collect(sample,3906);
        require(values(c,stats::Kind::RangeLength)==std::vector<std::string>({"1","NaN"})&&
                stats::encode(stats::decode(frame(c)))==frame(c),
            "NaN length remains a real float8 value with a safe strict sort order");

        for(uint32_t oid:{3904U,3926U,3906U,3912U,3908U,3910U}) {
            const std::string value=oid==3912?"[2000-01-01,2000-01-02)":
                oid==3908||oid==3910?"[\"2000-01-01 00:00:00\",\"2000-01-02 00:00:00\")":"[1,3)";
            const auto raw=rows(oid,{value,value,"empty",{}}),cached=rows(oid,{value,value,"empty",{}},true);
            require(frame(*collect(raw,oid))==frame(*collect(cached,oid)),
                "prepared keys preserve every statistic and value for each actual builtin subtype");
        }
        sample=rows(3904,{"[1,2)","[2,3)"});
        auto domain=column(2);domain.attributeType=type(10010,-1,3904);
        const auto intSpec=*input::builtin(3904);
        auto domainStats=stats::collectRangeStatistics(domain,sample,type(3904),type(701,8),
            [&](const auto& a,const auto& b){return input::compare(intSpec,a,b);},
            [&](const auto& a,const auto& b){return input::distance(intSpec,a,b);},
            [&](const auto& a,const auto& b){return input::construct(intSpec,a,b);});
        require(domainStats->attributeType->actualOid==10010&&
                slot(*domainStats,stats::Kind::RangeBounds)->values->elementType->actualOid==3904,
            "domain attribute identity is distinct from base range histogram values");

        rejected([&]{collect(rows(3906,{"[0,1e400)"}),3906);},"22003");
        rejected([&]{collect(rows(3906,{"[0,1e-400)"}),3906);},"22003");
        rejected([&]{rows(3904,{"[2,1)"});});
        rejected([&]{rows(3904,{"[1,1)"});});
        rejected([&]{rows(3904,{"[,2)"});});
        rejected([&]{rows(3904,{"[1,2,3)"});});
        rejected([&]{rows(3904,{"[\"1\"junk,2)"});});
        auto bad=sample;bad[1].ordinal=bad[0].ordinal;rejected([&]{collect(bad);});
        bad=sample;bad[0].storedWidth=UINT32_MAX;rejected([&]{collect(bad);});
        bad=sample;bad[0].lower.lower=false;rejected([&]{collect(bad);});
        rejected([&]{collect(sample,3904,10001);});
        Samples many;for(int i=0;i<1500;++i)many.push_back(input::decode(intSpec,
            "["+std::to_string(i)+","+std::to_string(i+1)+")",false,i,12,true));
        size_t interrupts=0;bool cancelled=false;
        try{collect(many,3904,100,[&]{if(++interrupts==3)throw DbError("57014","test cancellation");});}
        catch(const DbError& e){cancelled=e.sqlState()=="57014";}
        require(cancelled&&interrupts==3,"query cancellation propagates during range bound sorting");
        std::cout<<"ANALYZED_RANGE_COLLECTOR checks="<<checks<<" failures=0\n";return 0;
    } catch(const std::exception& e) {
        std::cerr<<"ANALYZED_RANGE_COLLECTOR_FAIL checks="<<checks<<' '<<e.what()<<'\n';return 1;
    }
}
