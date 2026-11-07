// Oracle: actual PostgreSQL 18.6 typed-sequence reference replies.
#include "commands/SequenceParameters.h"
#include <iostream>
#include <stdexcept>

using namespace dbms;
using Options=std::vector<std::pair<std::string,std::string>>;
namespace {
unsigned checks=0;
void need(bool value,const std::string& reason){++checks;if(!value)throw std::runtime_error(reason);}
std::vector<std::string> values(const SequenceInfo& info) {
    return {std::to_string(info.dataTypeOid),std::to_string(info.start),std::to_string(info.increment),
        std::to_string(info.maxValue),std::to_string(info.minValue),std::to_string(info.cache),info.cycle?"t":"f"};
}
}
int main() {
    try {
        unsigned typeReads=0;
        const auto type=[&](const std::string& name)->uint32_t {
            ++typeReads;
            if(name=="smallint" || name=="pg_catalog.int2")return 21;
            if(name=="integer" || name=="int4")return 23;
            if(name=="bigint")return 20;
            if(name=="text")return 25;
            throw DbError("42704","type does not exist");
        };
        struct Case {Options options;std::vector<std::string> expected;};
        for(const auto& item:std::vector<Case>{
            {{{"as","smallint"}},{"21","1","1","32767","1","1","f"}},
            {{{"as","integer"}},{"23","1","1","2147483647","1","1","f"}},
            {{{"as","bigint"}},{"20","1","1","9223372036854775807","1","1","f"}},
            {{{"as","pg_catalog.int2"},{"increment","-2"}},{"21","-1","-2","-1","-32768","1","f"}},
            {{{"as","int4"},{"start","17"},{"increment","3"},{"cache","4"},{"cycle",""}},
                {"23","17","3","2147483647","1","4","t"}},
            {{{"as","bigint"},{"nominvalue",""},{"nomaxvalue",""},{"increment","-1"}},
                {"20","-1","-1","-1","-9223372036854775808","1","f"}}}) {
            const auto result=sequence_parameters::resolve(item.options,nullptr,type);
            need(values(result.effective)==item.expected,"all seven actual declared parameters");
            need(result.effective.dataTypeSpecified && result.effective.hasMinValue && result.effective.hasMaxValue,
                "SQL creator receives resolved type and both bounds");
        }
        for(const auto& item:std::vector<std::pair<Options,std::string>>{
            {{{"as","text"}},"22023"},{{{"as","smallint"},{"maxvalue","32768"}},"22023"},
            {{{"as","integer"},{"minvalue","-2147483649"}},"22023"},
            {{{"as","smallint"},{"minvalue","2"},{"maxvalue","2"}},"22023"},
            {{{"as","smallint"},{"increment","0"}},"22023"},
            {{{"as","smallint"},{"cache","0"}},"22023"},
            {{{"start","1.0"},{"start","2"}},"42601"},
            {{{"as","missing"},{"as","bigint"}},"42601"},
            {{{"as","missing"}},"42704"},{{{"increment","1.0"}},"22P02"},
            {{{"start","9223372036854775808"}},"22003"}}) {
            std::string state;
            try{(void)sequence_parameters::resolve(item.first,nullptr,type);}
            catch(const DbError& error){state=error.sqlState();}
            need(state==item.second,"reference parameter SQLSTATE "+item.second+" actual "+state);
        }
        auto small=sequence_parameters::resolve(Options{{"as","smallint"}},nullptr,type).effective;
        auto widened=sequence_parameters::resolve(Options{{"as","integer"}},&small,type);
        need(values(widened.effective)==std::vector<std::string>{"23","1","1","2147483647","1","1","f"},
            "AS resets the old type extreme");
        need(widened.request.dataTypeSpecified && !widened.request.startSpecified && !widened.request.incrementSpecified,
            "ALTER retains genuine request roles");
        small.maxValue=100;
        auto custom=sequence_parameters::resolve(Options{{"as","integer"}},&small,type);
        need(custom.effective.maxValue==100 && small.dataTypeOid==21,"custom bound and original declaration retained");
        custom.effective.maxValue=40000;const auto before=values(custom.effective);
        std::string state;
        try{(void)sequence_parameters::resolve(Options{{"as","smallint"}},&custom.effective,type);}
        catch(const DbError& error){state=error.sqlState();}
        need(state=="22023" && values(custom.effective)==before,"failed narrowing is pure and atomic");
        const auto restarted=sequence_parameters::resolve(Options{{"start","3"},{"restart","17"}},nullptr,type);
        need(restarted.effective.start==3 && restarted.effective.restartSpecified &&
            restarted.effective.restartValueSpecified && restarted.effective.restart==17,
            "CREATE RESTART retains recorded START independently");
        const auto reads=typeReads;
        try{(void)sequence_parameters::resolve(Options{{"as","missing"},{"as","smallint"}},nullptr,type);}
        catch(const DbError& error){need(error.sqlState()=="42601","duplicate before type conversion");}
        need(typeReads==reads,"duplicate rejection performs no type lookup");
        std::cout<<"[SEQUENCE TYPE PARAMETERS] checks="<<checks<<" pure_reference_contract=true\n";return 0;
    } catch(const std::exception& error){std::cerr<<"SEQUENCE_TYPE_PARAMETERS_FAIL "<<error.what()<<'\n';return 1;}
}
