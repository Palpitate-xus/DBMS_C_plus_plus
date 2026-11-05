#include "executor/EqualityStatistics.h"
#include <cmath>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
namespace {
struct Statistics {size_t cardinality=0,nullCount=0;std::vector<std::pair<std::string,size_t>> mcv;};
size_t checks=0;
void need(bool good){++checks;if(!good)throw std::runtime_error("equality statistics expectation");}
std::optional<double> estimate(const Statistics& stats,double rows,const std::string& probe) {
 return dbms::planner_statistics::equalityFraction(stats,rows,[&](const std::string& value)->std::optional<bool>{return value==probe;});
}
void equal(const std::optional<double>& actual,double expected){need(actual&&std::abs(*actual-expected)<1e-14);}
}
int main(){try{
 const Statistics skew{101,100,{{"hot",800},{"warm",10}}};
 equal(estimate(skew,1000,"hot"),.8);equal(estimate(skew,1000,"warm"),.01);equal(estimate(skew,1000,"cold"),.09/99);
 equal(estimate({0,1000,{}},1000,"anything"),0);need(!estimate({},1000,"anything"));need(!estimate(skew,0,"hot"));
 equal(estimate({2,0,{{"hot",100}}},1000,"other"),.1);
 equal(estimate({1,100,{{"hot",900}}},1000,"other"),0);
 equal(estimate({10,100,{}},1000,"other"),.09);
 need(!estimate({0,0,{{"hot",1}}},1000,"hot"));need(!estimate({1,1001,{}},1000,"hot"));need(!estimate({1,100,{{"hot",901}}},1000,"hot"));
 const auto unknown=[](const std::string&)->std::optional<bool>{return {};};need(!dbms::planner_statistics::equalityFraction(skew,1000,unknown));
 const auto aliases=[](const std::string& value)->std::optional<bool>{return value=="hot"||value=="warm";};equal(dbms::planner_statistics::equalityFraction(skew,1000,aliases),.81);
 const auto incomplete=[](const std::string& value)->std::optional<bool>{if(value=="hot")return true;return {};};need(!dbms::planner_statistics::equalityFraction(skew,1000,incomplete));
 need(!estimate(skew,std::numeric_limits<double>::quiet_NaN(),"hot"));need(!estimate(skew,std::numeric_limits<double>::infinity(),"hot"));
 std::cout<<"EQUALITY_STATISTICS checks="<<checks<<" failures=0\n";return 0;
}catch(const std::exception& e){std::cerr<<"EQUALITY_STATISTICS_FAIL checks="<<checks<<' '<<e.what()<<'\n';return 1;}}
