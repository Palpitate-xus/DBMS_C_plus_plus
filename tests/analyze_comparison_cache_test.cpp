// Preserve complete typed ANALYZE statistics and persistent bytes across repeated runs.
#include "catalog/type_registry.h"
#include "catalog/collation.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using Engine=dbms::StorageEngine;using Cell=std::optional<std::string>;using Rows=std::vector<std::vector<Cell>>;
void require(bool value,const char* message){if(!value)throw std::runtime_error(message);}
Rows snapshot(const std::string& db,const dbms::TableSchema& table){
 dbms::TableScanOp scan(&g_engine,db,table.tablename);require(scan.open(),"real scan open");Rows rows;std::string raw;
 while(scan.next(raw)){std::vector<Cell> row;for(size_t i=0;i<table.len;++i)row.push_back(scan.lastColumnIsNull(i)?Cell{}:Cell(Engine::extractColumnValueStatic(raw,table,i)));rows.push_back(std::move(row));}
 require(!scan.hasError(),"real scan error");scan.close();return rows;
}
// Independent existing comparator oracle: sort the actual stored values, not
// cached keys/order indexes or the possibly normalized insertion arguments.
Engine::ColumnStats reference(const dbms::Column& column,const Rows& rows,size_t ordinal){
 Engine::ColumnStats out;std::vector<std::string> values;
 for(const auto& row:rows){if(!row[ordinal])++out.nullCount;else values.push_back(*row[ordinal]);}
 std::sort(values.begin(),values.end(),[&](const auto& a,const auto& b){
  const bool av=Engine::compareValues(column,a,false,a,false,"=")!=Engine::PredicateTruth::Unknown;
  const bool bv=Engine::compareValues(column,b,false,b,false,"=")!=Engine::PredicateTruth::Unknown;
  if(av!=bv)return av;if(!av)return a<b;
  return Engine::compareValues(column,a,false,b,false,"<")==Engine::PredicateTruth::True;
 });
 if(!values.empty()){out.minVal=values.front();out.maxVal=values.back();}
 if(values.size()>=20){const auto width=values.size()/10;for(size_t b=0;b<10;++b)out.histogram.push_back({values[b*width],values[b==9?values.size()-1:(b+1)*width]});}
 for(const auto& value:values){if(!out.mcv.empty()&&(out.mcv.back().first==value||Engine::compareValues(column,out.mcv.back().first,false,value,false,"=")==Engine::PredicateTruth::True))++out.mcv.back().second;else out.mcv.push_back({value,1});}
 out.cardinality=out.mcv.size();std::stable_sort(out.mcv.begin(),out.mcv.end(),[](const auto& a,const auto& b){return a.second>b.second;});if(out.mcv.size()>10)out.mcv.resize(10);return out;
}
std::string readFile(const std::string& path){std::ifstream in(path,std::ios::binary);require(bool(in),"actual .stats exists");return {std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};}
std::string hex(const std::string& raw){const char* h="0123456789abcdef";std::string out;out.reserve(raw.size()*2);for(unsigned char c:raw){out+=h[c>>4];out+=h[c&15];}return out;}
}
int main(){
 using namespace dbms;TypeRegistry::instance().bootstrap();const auto db=testDbPath("analyze_cache_stats");cleanupTestDb("analyze_cache_stats");
 // Only fixture setup disables automatic ANALYZE; every measured/validated
 // operation is explicit ANALYZE. Product defaults/frequency are unchanged.
 const bool saved=g_config.autoAnalyzeEnabled;g_config.autoAnalyzeEnabled=false;struct Restore{bool v;~Restore(){g_config.autoAnalyzeEnabled=v;}}restore{saved};
 require(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"create private database");
 require(collation::isLocaleAvailable("C.UTF-8"),"fixed explicit system-locale fixture requires available C.UTF-8; do not silently skip");
 require(g_engine.createCollation(db,"caller_utf","libc","C.UTF-8")==DBStatus::OK,"real catalog system locale");
 size_t controls=0,fields=0;
 const std::vector<std::string> text={"","NULL","Apple","apple","Zoo","10","2","A","a","é","e"};
 const std::vector<std::string> floating={"NaN","-Infinity","Infinity","-2.25","0","-0","1.25"};
 for(const size_t n:{size_t(0),size_t(1),size_t(2),size_t(3),size_t(17),size_t(20),size_t(33),size_t(65)}){
  TableSchema t;t.tablename="rows_"+std::to_string(n);
  t.append(makeIntColumn("precise",true,8));t.append(makeIntColumn("integer",true,2));t.append(makeDoubleColumn("floating",true));t.append(makeBooleanColumn("boolean",true));t.append(makeDecimalColumn("exact",true,40,3));
  t.append(makeVarCharColumn("implicit_default",true,128));
  for(const auto& named:std::vector<std::pair<std::string,std::string>>{{"explicit_default","default"},{"binary","C"},{"reversed","reverse"},{"folded","nocase"}}){auto c=makeVarCharColumn(named.first,true,128);c.collation=named.second;t.append(c);}
  auto padded=makeStringColumn("padded",true,8);padded.collation="C";t.append(padded);
  auto system=makeVarCharColumn("system_locale",true,128);system.collation="caller_utf";t.append(system);
  t.append(makeUuidColumn("uuid",true));t.append(makeTextColumn("all_null",true));t.append(makeTextColumn("long_text",true));
  require(t.len==15&&g_engine.createTable(db,t)==DBStatus::OK,"real fifteen-column table");require(g_engine.beginTransaction(db)==DBStatus::OK,"actual fixture transaction");
  for(size_t row=0;row<n;++row){const auto k=(row*23+n*7)%29;const auto value=text[k%text.size()];std::ostringstream uuid;uuid<<"00000000-0000-0000-0000-"<<std::hex<<std::setfill('0')<<std::setw(12)<<k;
   Engine::SqlRow input{{"precise",std::to_string(9007199254740992LL+static_cast<long long>(k))},{"integer",std::to_string(static_cast<int>(k)-14)},{"floating",floating[k%floating.size()]},{"boolean",k%2?"true":"false"},{"exact",row%5==4?(row%2?"0.50":"0.500"):"123456789012345678901234567890."+std::to_string(k+10)},{"implicit_default",value},{"explicit_default",value},{"binary",value},{"reversed",value},{"folded",value},{"padded",k%3==0?"a":k%3==1?"a   ":"b"},{"system_locale",value},{"uuid",uuid.str()},{"all_null",std::nullopt},{"long_text",std::string(4096,'x')+std::to_string(k%4)}};
   if(row%7==6){input["implicit_default"]=std::nullopt;input["system_locale"]=std::nullopt;input["boolean"]=std::nullopt;}if(row%11==10)input["floating"]=std::nullopt;
   require(g_engine.insertRow(db,t.tablename,input)==DBStatus::OK,"actual stored input");
  }
  require(g_engine.commitTransaction()==DBStatus::OK,"actual durable input commit");const auto stored=g_engine.getTableSchema(db,t.tablename);require(stored.cols[11].resolvedCollationUsesLocale,"actual explicit facet path, not pseudo C normalization");
  const auto before=snapshot(db,stored);require(before.size()==n,"all actual rows");std::vector<Engine::ColumnStats> expected;for(size_t i=0;i<stored.len;++i)expected.push_back(reference(stored.cols[i],before,i));
  std::string first;
  for(int invocation=0;invocation<2;++invocation){require(g_engine.analyzeTable(db,t.tablename),"actual analyze success");require(g_engine.getTableRowCount(db,t.tablename)==n,"actual row stats");require(snapshot(db,stored)==before,"all original owning heap values unchanged");
   for(size_t i=0;i<stored.len;++i){const auto a=g_engine.getColumnStats(db,t.tablename,stored.cols[i].dataName);const auto& b=expected[i];require(a.nullCount==b.nullCount&&a.cardinality==b.cardinality&&a.minVal==b.minVal&&a.maxVal==b.maxVal&&a.histogram==b.histogram&&a.mcv==b.mcv,"full original column stats including raw representative and stable MCV frequency ties");++fields;}
   const auto bytes=readFile(db+"/.stats");if(invocation==0)first=bytes;else require(bytes==first,"same graph-free repeated ANALYZE exact complete persistent .stats bytes");
   std::cout<<"ANALYZE_CACHE_STATS table="<<t.tablename<<" invocation="<<invocation<<" bytes="<<bytes.size()<<" hex="<<hex(bytes)<<'\n';
  }
  ++controls;std::cout<<"ANALYZE_CACHE_CONTROL "<<controls<<" N="<<n<<" full_fields=30 good=1\n";
 }
 auto integer=makeIntColumn("v",true,8);auto real=makeDoubleColumn("v",true);require(Engine::compareValues(integer,"oops",false,"oops",false,"=")==Engine::PredicateTruth::Unknown,"original invalid integral partition probe");require(Engine::compareValues(real,"oops",false,"oops",false,"=")==Engine::PredicateTruth::Unknown,"original invalid floating partition probe");require(Engine::compareValues(integer,"oops",true,"1",false,"=")==Engine::PredicateTruth::Unknown,"true NULL does not demand invalid datum conversion");
 require(g_engine.dropDatabase(db)==DBStatus::OK,"drop owned database");cleanupTestDb("analyze_cache_stats");finalCleanupTestData();
 std::cout<<"ANALYZE_CACHE_DIFFERENTIAL_TERMINAL controls="<<controls<<" expected=8 field_checks="<<fields<<" expected_fields=240 fullvalues=1 comparison_probes=3\n";return controls==8&&fields==240?0:1;
}
