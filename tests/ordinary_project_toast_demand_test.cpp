#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
using Cell=std::optional<std::string>;
using Row=std::vector<Cell>;
using Model=std::vector<Row>;
size_t groups=0,checks=0,failures=0;
void need(bool good,const std::string& message) {
 ++checks;if(!good){++failures;std::cerr<<"FAIL "<<message<<'\n';}
}
void group(const char* name,const std::function<void()>& f) {
 ++groups;std::cout<<"GROUP "<<name<<'\n';
 try {f();}catch(const DbError& e){need(false,std::string(name)+" "+e.sqlState()+" "+e.what());}
 catch(const std::exception& e){need(false,std::string(name)+" "+e.what());}
}
std::string hex(const std::string& raw) {
 static constexpr char digits[]="0123456789abcdef";std::string result="\\x";
 for(unsigned char c:raw){result+=digits[c>>4];result+=digits[c&15];}
 return result;
}
struct Result {Model rows;std::vector<int64_t> rids;bool failed=false;std::string error;};
Result run(const std::string& db,const std::string& table,const TableSchema& projection,
           const std::set<std::string>& selected,bool filter=false) {
 auto scan=std::make_unique<TableScanOp>(&g_engine,db,table);auto* physical=scan.get();
 OpPtr child=std::move(scan);
 if(filter)child=std::make_unique<FilterOp>(std::move(child),projection,std::vector<StorageEngine::Condition>{});
 ProjectOp op(std::move(child),projection,selected);Result result;
 try {
  if(!op.open())throw std::runtime_error("actual projection open failed");
  std::string raw;
  while(op.next(raw)) {
   std::vector<std::string> cells;std::vector<bool> nulls;
   if(!op.lastStructuredRow(cells,nulls)||cells.size()!=nulls.size())
    throw std::runtime_error("genuine structured result unavailable");
   Row row;for(size_t i=0;i<cells.size();++i)row.push_back(nulls[i]?Cell{}:Cell{cells[i]});
   result.rows.push_back(std::move(row));result.rids.push_back(physical->lastRid());
  }
  result.failed=op.hasError();result.error=op.errorMessage();op.close();
 }catch(...){op.close();throw;}
 return result;
}
Model project(const Model& rows,const TableSchema& schema,const std::set<std::string>& selected) {
 Model result;
 for(const auto& row:rows) {
  Row out;
  for(size_t i=0;i<schema.len;++i)if(selected.empty()||selected.count(schema.cols[i].dataName))out.push_back(row.at(i));
  result.push_back(std::move(out));
 }
 return result;
}
Model ordered(Model rows){std::sort(rows.begin(),rows.end());return rows;}
void matches(const Result& result,const Model& expected,const std::string& name) {
 need(!result.failed,name+" has no execution error "+result.error);
 need(ordered(result.rows)==ordered(expected),name+" every cell/NULL and row multiplicity");
 need(result.rids.size()==result.rows.size()&&std::set<int64_t>(result.rids.begin(),result.rids.end()).size()==result.rows.size(),name+" actual unique physical RIDs");
 need(std::all_of(result.rids.begin(),result.rids.end(),[](auto rid){return rid>0;}),name+" positive actual source RIDs");
}
struct HiddenToast {
 std::filesystem::path live,saved;
 HiddenToast(const std::string& db,const std::string& table):live(g_engine.toastDataPath(db,table)),saved(live.string()+".projection-test-saved") {
  if(!std::filesystem::is_regular_file(live)||std::filesystem::exists(saved))throw std::runtime_error("actual TOAST file prerequisite missing");
  std::filesystem::rename(live,saved);
 }
 ~HiddenToast(){if(std::filesystem::exists(saved)&&!std::filesystem::exists(live))std::filesystem::rename(saved,live);}
 void noReplacement(){need(!std::filesystem::exists(live),"missing TOAST read never invents a replacement file");}
};
}
int main() {
 using namespace dbms;
 try {
  TypeRegistry::instance().bootstrap();const auto db=testDbPath("ordinary_project_toast_demand");
  Session session;session.username="admin";session.permission=1;session.currentDB=db;
  struct Restore {Session* previous;bool analyze;~Restore(){setCurrentSession(previous);g_config.autoAnalyzeEnabled=analyze;}}
   restore{currentSession(),g_config.autoAnalyzeEnabled};
  setCurrentSession(&session);g_config.autoAnalyzeEnabled=false;
  if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)throw std::runtime_error("actual database creation failed");
  DdlExecutor ddl;
  for(const std::string sql:{"CREATE TABLE ptd_values(id integer PRIMARY KEY,n integer,small varchar(64),pad varchar(16384),raw bytea)",
    "CREATE TABLE ptd_empty(id integer PRIMARY KEY,n integer,pad text)",
    "CREATE TABLE ptd_virtual(id integer PRIMARY KEY,n integer,pad text,good integer GENERATED ALWAYS AS(n+1) VIRTUAL,bad integer GENERATED ALWAYS AS(1/(id-id)) VIRTUAL)"})
   if(ddl.executeSql(sql,session))throw std::runtime_error("actual DDL prerequisite failed: "+sql);
  std::string allBytes;for(int k=0;k<40;++k)for(int c=0;c<256;++c)allBytes+=char(c);
  const std::string a=std::string(9216,'A')+"\nend",b(11264,'b'),binary=hex(allBytes);
  Model rows{{"1",Cell{},"",a,binary},{"2","0",Cell{},Cell{},Cell{}},
    {"3","-2147483648","NULL","__TOAST__1","\\x"},
    {"4","2147483647","é中","","\\x004e554c4c"},
    {"5","7","two words",b,"\\xff001020"},{"6",Cell{},"line\tend","NULL",Cell{}}};
  const std::vector<std::string> names{"id","n","small","pad","raw"};
  for(const auto& row:rows) {
   std::map<std::string,Cell> input;for(size_t i=0;i<names.size();++i)input[names[i]]=row[i];
   if(g_engine.insertRow(db,"ptd_values",input)!=DBStatus::OK)throw std::runtime_error("actual mixed data insert failed");
  }
  for(const std::string id:{"1","2"})
   if(g_engine.insertRow(db,"ptd_virtual",{{"id",id},{"n",id=="1"?Cell{}:Cell{"2"}},{"pad",a}})!=DBStatus::OK)
    throw std::runtime_error("actual virtual-dependency fixture insert failed");
  const auto schema=g_engine.getTableSchema(db,"ptd_values"),virt=g_engine.getTableSchema(db,"ptd_virtual");
  const auto check=[&](const std::set<std::string>& selected,const std::string& name) {
   matches(run(db,"ptd_values",schema,selected),project(rows,schema,selected),name);
  };
  group("healthy wildcard and explicit all columns",[&]{check({},"wildcard");check({"id","n","small","pad","raw"},"all explicit");});
  group("healthy nullable fixed and inline variable subset",[&]{check({"id","n"},"nullable fixed");check({"small"},"inline text including NULL/empty/literalNULL");check({"id","small"},"fixed plus variable subset");});
  group("healthy selected large and marker-looking values",[&]{check({"pad"},"large and literal TOAST marker");check({"id","raw"},"binary all256 bytes");check({"n","pad","raw"},"multiple selected LOBs and NULL");});
  group("healthy virtual dependency and selected error",[&] {
   matches(run(db,"ptd_virtual",virt,{"id","good"}),{{"1",Cell{}},{"2","3"}},"actual virtual dependency");
   bool error=false;try {(void)run(db,"ptd_virtual",virt,{"id","bad"});}catch(const DbError& e){error=e.sqlState()=="22012";}
   need(error,"selected virtual division error remains22012");
  });
  group("healthy filtered full-row owner",[&]{matches(run(db,"ptd_values",schema,{"id","n"},true),project(rows,schema,{"id","n"}),"actual Filter wrapper");});
  group("empty physical table",[&]{const auto empty=g_engine.getTableSchema(db,"ptd_empty");matches(run(db,"ptd_empty",empty,{"id"}),{},"empty");});
  {
   HiddenToast hidden(db,"ptd_values");
   group("unselected LOB absent fixed projection",[&]{check({"id","n"},"only fixed fields with unavailable unselected TOAST");hidden.noReplacement();});
   group("unselected LOB absent variable subset",[&]{check({"id","small"},"inline variable subset with unavailable unselected TOAST");hidden.noReplacement();});
   group("selected LOB errors before row escapes",[&] {
    for(const auto& selected:std::vector<std::set<std::string>>{{"pad"},{"id","raw"},{},{"id","n","small","pad","raw"}}) {
     auto result=run(db,"ptd_values",schema,selected);need(result.failed,"selected or wildcard TOAST read fails");need(result.rows.empty(),"no partial selected-failure row emitted");
    }
    hidden.noReplacement();
   });
   group("wrapped consumer keeps full source errors",[&]{const auto result=run(db,"ptd_values",schema,{"id"},true);need(result.failed&&result.rows.empty(),"Filter wrapper retains original full-row source contract");hidden.noReplacement();});
   group("mismatched schema cannot grant projection capability",[&] {
    for(int k=0;k<4;++k) {
     auto changed=schema;
     if(k==0)changed.tablename="not_the_actual_table";
     if(k==1)changed.cols[1].dsize+=1;
     if(k==2)changed.cols[2].isNull=!changed.cols[2].isNull;
     if(k==3)changed.formatVersion=1;
     const auto result=run(db,"ptd_values",changed,{"id"});need(result.failed&&result.rows.empty(),"schema witness mismatch retains full source path "+std::to_string(k));
    }
    const auto unknown=run(db,"ptd_values",schema,{"not_a_column"});need(unknown.failed&&unknown.rows.empty(),"unknown selected name cannot suppress full source error");hidden.noReplacement();
   });
  }
  group("restored original LOB bytes remain recoverable",[&]{check({},"full original contents after file restore");});
  {
   HiddenToast hidden(db,"ptd_virtual");
   group("unselected virtual targets do not demand LOB",[&]{matches(run(db,"ptd_virtual",virt,{"id","n"}),{{"1",Cell{}},{"2","2"}},"stored subset, both virtual targets unselected");hidden.noReplacement();});
   group("selected virtual retains complete input owner",[&]{const auto result=run(db,"ptd_virtual",virt,{"id","good"});need(result.failed&&result.rows.empty(),"selected virtual keeps original complete physical input read/error");hidden.noReplacement();});
  }
  std::cout<<"ORDINARY_PROJECT_TOAST_DEMAND groups="<<groups<<" checks="<<checks<<" failures="<<failures<<'\n';
  return failures?1:0;
 }catch(const DbError& e){std::cerr<<"FATAL "<<e.sqlState()<<' '<<e.what()<<'\n';return 1;}
 catch(const std::exception& e){std::cerr<<"FATAL "<<e.what()<<'\n';return 1;}
}
