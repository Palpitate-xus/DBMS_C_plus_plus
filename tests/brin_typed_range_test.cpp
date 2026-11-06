// test_link_flags: -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj -Wl,--wrap=_ZNK4dbms14TxnIdGenerator16maxCommittedTxIdEv
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "executor/ExecutionPlan.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "access/BrinIndexFormat.h"
#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "common/Config.h"
#include "transaction/TxnIdGenerator.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <algorithm>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <set>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
size_t checks=0;
thread_local PageAllocator* watched=nullptr;
thread_local size_t fetched=0,fenceCalls=0;
thread_local bool injectAllocation=false;
void need(bool yes,const std::string& label){++checks;if(!yes)throw std::runtime_error(label);}
using Cell=std::optional<std::string>;
struct Probe{std::string op,value;std::vector<int> expected;};
struct Case{std::string name,type;std::vector<Cell> values;std::vector<Probe> probes;};
std::string bytes(const std::filesystem::path& p){std::ifstream in(p,std::ios::binary);need(bool(in),"real index image");return {std::istreambuf_iterator<char>(in),{}};}
void write(const std::filesystem::path& p,const std::string& b){std::ofstream out(p,std::ios::binary|std::ios::trunc);out.write(b.data(),b.size());out.close();need(!out.fail(),"complete private image replacement");}
bool covers(const std::vector<std::pair<uint32_t,uint32_t>>& ranges,uint32_t page){return std::any_of(ranges.begin(),ranges.end(),[&](auto r){return r.first<=page&&page<=r.second;});}
uint32_t crc(const std::string& b){uint32_t x=0xffffffffu;for(unsigned char c:b){x^=c;for(unsigned j=0;j<8;++j)x=(x>>1)^((x&1)?0x82f63b78u:0);}return x^0xffffffffu;}
void number(std::string& b,uint64_t n,unsigned width){for(unsigned i=0;i<width;++i)b.push_back(char(n>>(i*8)));}
std::string legacy(unsigned version){std::string b;number(b,0x5342524e,4);number(b,version,4);number(b,1,8);number(b,1,4);number(b,1,4);number(b,2,8);b+="10";number(b,1,8);b+="2";if(version==2)number(b,crc(b),4);return b;}
const auto db=testDbPath("brin_typed_range");
std::map<int,int64_t> allRows(const std::string& table){
 const auto schema=g_engine.getTableSchema(db,table);std::map<int,int64_t> ids;
 need(g_engine.forEachRow(db,table,[&](uint32_t p,uint16_t slot,const char* data,size_t len){
  const auto rid=StorageEngine::encodeRid(p,slot);const std::string row(data,len);const int id=std::stoi(g_engine.extractColumnValue(row,schema,0,db));need(ids.emplace(id,rid).second,"complete unique visible row");
  const auto nulls=detail::storedRowNullBitmap(g_engine,db,table,schema,rid);need(nulls&&nulls->size()==schema.len,"complete physical NULL bitmap");
  for(size_t i=0;i<schema.len;++i){bool isNull=false;const auto cell=g_engine.extractColumnValue(row,schema,i,db,true,&isNull);need(isNull==(*nulls)[i],"full decoded SQL NULL map");std::cout<<"BRIN_NATIVE_CELL "<<table<<' '<<id<<' '<<i<<' '<<isNull<<' '<<cell.size()<<'\n';}
 }),"whole actual heap scan");return ids;
}
void probe(const Case& c,const std::map<int,int64_t>& ids,bool requireTrusted=true){
 for(const auto& p:c.probes){
  if(requireTrusted)need(bool(g_engine.tryBrinPageRanges(db,c.name,"v",p.op,p.value)),"real trusted typed pruning: "+c.name+" "+p.op+" "+p.value);
  const auto ranges=g_engine.brinSearchRange(db,c.name,"v",p.op,p.value);
  for(int id:p.expected){uint32_t page=0;uint16_t slot=0;StorageEngine::decodeRid(ids.at(id),page,slot);need(covers(ranges,page),"no lost typed candidate: "+c.name+" "+p.op+" "+p.value);}
 }
}
}
extern "C" char* realFetch(dbms::PageAllocator*,uint32_t)asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*,uint32_t)asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* owner,uint32_t page){if(owner==watched)++fetched;return realFetch(owner,page);}
extern "C" uint64_t realMax(const dbms::TxnIdGenerator*)asm("__real__ZNK4dbms14TxnIdGenerator16maxCommittedTxIdEv");
extern "C" uint64_t wrapMax(const dbms::TxnIdGenerator*)asm("__wrap__ZNK4dbms14TxnIdGenerator16maxCommittedTxIdEv");
extern "C" uint64_t wrapMax(const dbms::TxnIdGenerator* owner){if(injectAllocation&&++fenceCalls==2)(void)dbms::TxnIdGenerator::instance().nextTxId();return realMax(owner);}
int main()try{
 using namespace dbms;TypeRegistry::instance().bootstrap();g_config.autoAnalyzeEnabled=false;g_config.autoVacuumEnabled=false;g_engine.setBackgroundIntervals(60000,60000);
 Session session;session.username="testuser";session.permission=1;session.currentDB=db;auto* previous=currentSession();setCurrentSession(&session);
 struct Restore{Session* p;~Restore(){injectAllocation=false;watched=nullptr;if(g_engine.inTransaction())g_engine.rollbackTransaction();setCurrentSession(p);}}restore{previous};
 SQLParser parser;
 for(const auto& input:std::vector<std::pair<std::string,std::map<std::string,std::string>>>{{"pages_per_range=1",{{"pages_per_range","1"}}},{"pages_per_range=-1",{{"pages_per_range","-1"}}},{"pages_per_range=2,autosummarize=false",{{"pages_per_range","2"},{"autosummarize","false"}}},{"autosummarize",{{"autosummarize","true"}}}}){auto result=parser.parse("CREATE INDEX option_probe ON integers USING brin(v) WITH("+input.first+")");const auto* stmt=result.success?dynamic_cast<const CreateIndexStmt*>(result.stmt.get()):nullptr;need(stmt&&stmt->options==input.second,"complete reloption AST map and balanced delimiters");}
 for(const auto& options:{"pages_per_range=1,","pages_per_range=","pages_per_range=1 pages_per_range=2","pages_per_range=1,pages_per_range=2"})need(!parser.parse("CREATE INDEX option_probe ON integers USING brin(v) WITH("+std::string(options)+")").success,"malformed/duplicate reloptions cannot be silently dropped");
 need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"private database");DdlExecutor ddl;
 const std::vector<Case> cases={
  {"integers","integer",{"2","10","-10","-2","0","2147483647",std::nullopt},{{">=","8",{2,6}},{"<","-5",{3}},{"=","2",{1}},{"IS NULL","",{7}},{"IS NOT NULL","",{1,2,3,4,5,6}}}},
  {"bigints","bigint",{"-9223372036854775808","-9007199254740993","9007199254740992","9007199254740993","9223372036854775807",std::nullopt},{{">","9007199254740992",{4,5}},{"<","-9007199254740992",{1,2}}}},
  {"decimals","numeric",{"-10.5","-2.25","0.00","2.5","10.25","999999999999999999999999999999.1",std::nullopt},{{">=","8",{5,6}},{"<","-2.3",{1}},{"=","2.500",{4}}}},
  {"floats","double precision",{"-10.5","-2.25","0","2.5","10.25","NaN",std::nullopt},{{">=","8",{5,6}},{"<","-2.3",{1}},{"=","NaN",{6}}}},
  {"booleans","boolean",{"false","true",std::nullopt},{{"=","true",{2}},{"<","true",{1}},{"IS NULL","",{3}}}},
  {"strings","text COLLATE \"C\"",{"","10","2","NULL","界\n|;,",std::nullopt},{{"=","",{1}},{">","2",{4,5}},{"IS NULL","",{6}}}},
  {"dates","date",{"1999-12-31","2000-01-01","2020-06-03",std::nullopt},{{">=","2000-01-01",{2,3}},{"<","2000-01-01",{1}}}},
  {"times","time",{"00:00:00","12:34:56.123456","23:59:59.999999",std::nullopt},{{">","12:34:56.1",{2,3}}}},
  {"timestamps","timestamp",{"1999-12-31 23:59:59.999999","2000-01-01 00:00:00","2020-01-01 00:00:00.000001",std::nullopt},{{">=","2000-01-01 00:00:00",{2,3}}}},
  {"uuids","uuid",{"00000000-0000-0000-0000-000000000001","ffffffff-ffff-ffff-ffff-ffffffffffff",std::nullopt},{{">","00000000-0000-0000-0000-000000000010",{2}}}},
  {"arrays","integer[]",{"{2,NULL}","{}","[0:1]={10,20}",std::nullopt},{{"IS NULL","",{4}},{"IS NOT NULL","",{1,2,3}}}},
  {"ranges","int4range",{"[1,3)","empty","[10,20)",std::nullopt},{{"IS NULL","",{4}},{"IS NOT NULL","",{1,2,3}}}}
 };
 for(const auto& c:cases){
  need(!ddl.executeSql("CREATE TABLE "+c.name+"(id integer PRIMARY KEY,v "+c.type+",pad text)",session),"real scalar DDL");
  for(size_t i=0;i<c.values.size();++i)need(g_engine.insertRow(db,c.name,{{"id",std::to_string(i+1)},{"v",c.values[i]},{"pad",i%2?Cell{}:Cell("payload 界\n"+std::to_string(i))}})==DBStatus::OK,"real scalar row");
  const auto before=allRows(c.name);need(g_engine.createBrinIndex(db,c.name,"v",1)==DBStatus::OK,"actual typed BRIN build");need(allRows(c.name)==before,"build preserves every RID/row");probe(c,before);
 }
 const auto path=g_engine.dbPath(db)/"integers_v.brin";const auto original=bytes(path);
 for(unsigned version:{1u,2u}){write(path,legacy(version));need(!g_engine.tryBrinPageRanges(db,"integers","v",">=","8"),"old bounds lack a typed identity");probe(cases[0],allRows("integers"),false);}
 write(path,original);auto damaged=original;damaged.back()^=1;write(path,damaged);need(!g_engine.tryBrinPageRanges(db,"integers","v",">=","8"),"corrupt summary means heap fallback, not empty candidates");need(g_engine.brinSearchRange(db,"integers","v",">=","8").empty(),"legacy diagnostic API retains corruption rejection");const auto marker=g_engine.dbPath(db)/"integers.specialized_index_dirty";write(path,damaged);write(marker,"DBMS_SPECIALIZED_INDEX_DIRTY_V1\n");need(!g_engine.tryBrinPageRanges(db,"integers","v",">=","8"),"dirty corrupt generation cannot prove pruning");need(!g_engine.brinSearchRange(db,"integers","v",">=","8").empty(),"actual dirty corrupt sidecar bypassed before native heap fallback");std::filesystem::remove(marker);write(path,original);
 brin_index_format::TypedIndex model;need(brin_index_format::decodeTyped(original,model),"real V3 full image");model.orderingIdentity+="changed";write(path,brin_index_format::encodeTyped(model));need(!g_engine.tryBrinPageRanges(db,"integers","v",">=","8"),"identity mismatch declines pruning");write(path,original);
 injectAllocation=true;fenceCalls=0;const auto moved=g_engine.tryBrinPageRanges(db,"integers","v",">=","8");injectAllocation=false;need(fenceCalls>=2&&!moved,"actual intervening durable allocation invalidates proof");
 need(g_engine.setIsolationLevel(IsolationLevel::SERIALIZABLE),"select SSI");need(g_engine.beginTransaction(db)==DBStatus::OK,"real SSI reader");need(!g_engine.tryBrinPageRanges(db,"integers","v",">=","8"),"SSI retains full heap predicate registration");probe(cases[0],allRows("integers"),false);need(g_engine.commitTransaction()==DBStatus::OK,"SSI reader commit");need(bool(g_engine.tryBrinPageRanges(db,"integers","v",">=","8")),"ended native SSI block is not an active serializable reader");need(g_engine.setIsolationLevel(IsolationLevel::READ_COMMITTED),"restore RC");
 need(!ddl.executeSql("CREATE TABLE correlated(id integer PRIMARY KEY,v integer,pad text)",session),"real correlated heap");need(g_engine.beginTransaction(db)==DBStatus::OK,"large seed");
 for(int i=1;i<=768;++i)need(g_engine.insertRow(db,"correlated",{{"id",std::to_string(i)},{"v",std::to_string(i)},{"pad",std::string(192,char('a'+i%26))}})==DBStatus::OK,"correlated real row");
 need(g_engine.commitTransaction()==DBStatus::OK,"large seed durable");need(g_engine.createBrinIndex(db,"correlated","v",1)==DBStatus::OK,"one-page summaries");const auto schema=g_engine.getTableSchema(db,"correlated");const auto ranges=g_engine.tryBrinPageRanges(db,"correlated","v",">=","760");need(bool(ranges),"trusted typed candidate ranges");uint64_t count=0;for(auto r:*ranges)count+=r.second-r.first+1;const auto extent=g_engine.tableNumPages(db,"correlated");need(count>0&&count<extent-1,"correlated summary excludes real physical pages");
 watched=g_engine.getPageAllocator(db,"correlated");fetched=0;std::set<int> actual;{auto cursor=g_engine.beginHeapScan(db,"correlated",schema,&*ranges);HeapScanRow row;while(cursor->next(row)){need(row.nulls.size()==schema.len,"limited cursor owns complete bitmap");actual.insert(std::stoi(g_engine.extractColumnValue(row.data,schema,0,db)));}}watched=nullptr;
 for(int i=760;i<=768;++i)need(actual.count(i),"limited cursor contains every matching row");need(fetched==count,"limited cursor fetches selected pages once");std::cout<<"BRIN_PHYSICAL_PROBE extent="<<extent<<" selected="<<count<<" actual_fetches="<<fetched<<" candidates="<<actual.size()<<'\n';
 const auto correlatedPath=g_engine.dbPath(db)/"correlated_v.brin";const auto correlatedBytes=bytes(correlatedPath);
 const auto plan=[&]{auto query=g_engine.prepareBoundQuery(db,"SELECT id,v,pad FROM correlated WHERE v>=760 ORDER BY id");const auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());const auto* binary=select?dynamic_cast<const BinaryOpExpr*>(select->whereClause.get()):nullptr;need(binary&&(!binary->comparison||(binary->comparison->strict&&binary->comparison->identity.rfind("builtin-comparison:",0)==0)),"no non-builtin AST role is narrowed");const auto role=ExprEvaluator::resolveComparison(">=","integer","integer");need(role.strict&&role.identity=="builtin-comparison:>=(integer,integer)","actual ordinary builtin signature, independent of optional enum/reference AST cell");return QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"correlated",std::move(query));};
 const auto complete=[&](OpPtr op){auto result=QueryPlanner::executePlanChecked(std::move(op));result.throwIfFailed();need(result.structuredRowsAvailable&&result.structuredRows.size()==9,"complete real SQL publication after late invalidation");for(size_t i=0;i<9;++i){const auto id=760+i;const auto& row=result.structuredRows[i];need(row.size()==3&&row[0]==std::to_string(id)&&row[1]==std::to_string(id)&&row[2]==std::string(192,char('a'+id%26))&&result.structuredNulls.size()==9&&result.structuredNulls[i]==std::vector<bool>({false,false,false}),"all complete SQL values preserved");}};
 {PlanContext context;context.dbname=db;context.tablename="correlated";context.conds=StorageEngine::parseConditions({">=v 760"});auto source=QueryPlanner::buildSelectPlan(&g_engine,context);need(QueryPlanner::explainJson(source,&g_engine,db).find("\"nodeType\":\"BrinScan\"")!=std::string::npos,"ordinary compact SQL source selects BRIN");complete(std::move(source));}
 {auto source=plan();need(QueryPlanner::explainJson(source,&g_engine,db).find("\"nodeType\":\"BrinScan\"")!=std::string::npos,"actual SQL source selects BRIN");complete(std::move(source));}
 {auto source=plan();std::filesystem::remove(correlatedPath);complete(std::move(source));write(correlatedPath,correlatedBytes);}
 {auto source=plan();auto bad=correlatedBytes;bad.back()^=1;write(correlatedPath,bad);complete(std::move(source));write(correlatedPath,correlatedBytes);}
 need(g_engine.beginTransaction(db)==DBStatus::OK,"actual own writer");need(g_engine.updateRows(db,"correlated",StorageEngine::SqlRow{{"v",std::string("9999")}},{"=id 1"})==DBStatus::OK,"real indexed update");need(!g_engine.tryBrinPageRanges(db,"correlated","v",">=","9999"),"own-write owner declines current-summary proof");need(!g_engine.brinSearchRange(db,"correlated","v",">=","9999").empty(),"own write remains visible through heap fallback");need(g_engine.rollbackTransaction()==DBStatus::OK,"rollback real write");
 brin_index_format::TypedIndex configured;need(brin_index_format::decodeTyped(bytes(correlatedPath),configured)&&configured.pagesPerRange==1,"commit/rollback maintenance preserves configured range width");
 std::cout<<"BRIN_TYPED_RANGE checks="<<checks<<" failures=0\n";return 0;
}catch(const std::exception& error){injectAllocation=false;watched=nullptr;std::cerr<<"BRIN_TYPED_RANGE_FAIL checks="<<checks<<' '<<error.what()<<'\n';return 1;}
