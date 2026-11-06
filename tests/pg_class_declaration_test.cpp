#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/DdlExecutor.h"
#include "common/Config.h"
#include "expression/ExprEvaluator.h"
#include "test_utils.h"
#if __has_include("catalog/PgClassCatalog.h")
#include "catalog/PgClassCatalog.h"
#define HAS_CLASS_DECLARATION 1
#else
#define HAS_CLASS_DECLARATION 0
#endif
#include <array>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
using Snapshot=CatalogManager::MetadataSnapshot;
// Independently transcribed from REL_18_6 pg_class.h/pg_type.dat, rather than
// importing the implementation's field table to generate its expectations.
struct Field {const char* name;Oid type;int16_t size;};
constexpr std::array<Field,34> expected{{
 {"oid",26,4},{"relname",19,64},{"relnamespace",26,4},{"reltype",26,4},
 {"reloftype",26,4},{"relowner",26,4},{"relam",26,4},{"relfilenode",26,4},
 {"reltablespace",26,4},{"relpages",23,4},{"reltuples",700,4},
 {"relallvisible",23,4},{"relallfrozen",23,4},{"reltoastrelid",26,4},
 {"relhasindex",16,1},{"relisshared",16,1},{"relpersistence",18,1},
 {"relkind",18,1},{"relnatts",21,2},{"relchecks",21,2},{"relhasrules",16,1},
 {"relhastriggers",16,1},{"relhassubclass",16,1},{"relrowsecurity",16,1},
 {"relforcerowsecurity",16,1},{"relispopulated",16,1},{"relreplident",18,1},
 {"relispartition",16,1},{"relrewrite",26,4},{"relfrozenxid",28,4},
 {"relminmxid",28,4},{"relacl",1034,-1},{"reloptions",1009,-1},{"relpartbound",194,-1}}};
size_t groups=0,checks=0,failures=0,notrun=0;
void need(bool good,const std::string& message) {
 ++checks;if(!good){++failures;std::cerr<<"FAIL "<<message<<'\n';}
}
void group(const char* name,const std::function<void()>& f) {
 ++groups;std::cout<<"GROUP "<<name<<'\n';
 try {f();}catch(const DbError& e){need(false,std::string(name)+" "+e.sqlState()+" "+e.what());}
 catch(const std::exception& e){need(false,std::string(name)+" "+e.what());}
}
using Files=std::map<std::string,std::string>;
Files files(const std::filesystem::path& root) {
 Files out;if(!std::filesystem::exists(root))return out;
 for(const auto& p:std::filesystem::recursive_directory_iterator(root))if(p.is_regular_file()) {
  std::ifstream in(p.path(),std::ios::binary);if(!in)throw std::runtime_error("cannot read fixture");
  std::string data{std::istreambuf_iterator<char>(in),{}};
  if(in.bad())throw std::runtime_error("fixture read failure");
  out.emplace(p.path().lexically_relative(root).string(),std::move(data));
 }
 return out;
}
template<class... T> std::string packed(const char* role,const T&... values) {
 std::string out=role;
 const auto add=[&](const auto& value) {
  std::ostringstream stream;stream<<value;const auto text=stream.str();
  out+=std::to_string(text.size())+":"+text;
 };
 (add(values),...);return out;
}
std::vector<std::string> identity(const Snapshot& s) {
 std::vector<std::string> out;
 for(const auto& n:s.namespaces)out.push_back(packed("N",n.oid,n.nspname,n.nspowner));
 for(const auto& c:s.relations)out.push_back(packed("C",c.oid,c.relname,c.relnamespace,c.reltype,c.reloftype,c.relowner,
  c.relam,c.relfilenode,c.reltablespace,c.relpages,c.reltuples,c.relallvisible,c.reltoastrelid,c.relhasindex,c.relisshared,
  c.relpersistence,c.relkind,c.relnatts,c.relchecks,c.relhasrules,c.relhastriggers,c.relhassubclass,c.relrowsecurity,
  c.relforcerowsecurity,c.relispopulated,c.relreplident,c.relispartition,c.relrewrite,c.relfrozenxid,c.relminmxid));
 for(const auto& a:s.attributes)out.push_back(packed("A",a.attrelid,a.attname,a.atttypid,a.attstattarget,a.attlen,a.attnum,
  a.attndims,a.attcacheoff,a.atttypmod,a.attbyval,a.attstorage,a.attalign,a.attnotnull,a.atthasdef,a.attidentity,
  a.attgenerated,a.attisdropped,a.attislocal,a.attinhcount,a.attcollation));
 for(const auto& t:s.types)out.push_back(packed("T",t.oid,t.typname,t.typnamespace,t.typowner,t.typlen,t.typbyval,
  t.typtype,t.typcategory,t.typispreferred,t.typisdefined,t.typdelim,t.typrelid,t.typelem,t.typarray,t.typinput,
  t.typoutput,t.typreceive,t.typsend,t.typmodin,t.typmodout,t.typanalyze,t.typalign,t.typstorage,t.typnotnull,
  t.typbasetype,t.typtypmod,t.typndims,t.typcollation));
 std::sort(out.begin(),out.end());return out;
}
void declaration(const Snapshot& s) {
 const auto self=std::find_if(s.relations.begin(),s.relations.end(),[](const auto& c){return c.oid==1259;});
 need(self!=s.relations.end(),"actual self relation exists");if(self==s.relations.end())return;
 need(std::count_if(s.relations.begin(),s.relations.end(),[](const auto& c){return c.relnamespace==11&&c.relname=="pg_class";})==1,
      "single actual pg_catalog.pg_class identity");
 need(self->relname=="pg_class"&&self->relnamespace==11&&self->reltype==83&&self->relnatts==34&&self->relkind=='r',"full actual self declaration");
 std::vector<PgAttributeRow> attrs;
 for(const auto& a:s.attributes)if(a.attrelid==self->oid)attrs.push_back(a);
 std::sort(attrs.begin(),attrs.end(),[](const auto& a,const auto& b){return a.attnum<b.attnum;});
 need(attrs.size()==expected.size(),"exactly34 actual attributes without compact substitute");
 for(size_t i=0;i<expected.size()&&i<attrs.size();++i) {
  const auto& a=attrs[i];const auto& e=expected[i];
  need(a.attnum==int(i+1)&&a.attname==e.name&&a.atttypid==e.type&&a.attlen==e.size&&!a.attisdropped,
       "continuous genuine field "+std::string(e.name));
  const auto t=std::find_if(s.types.begin(),s.types.end(),[&](const auto& t){return t.oid==a.atttypid;});
  need(t!=s.types.end()&&t->typnamespace==11&&t->typlen==e.size,"actual field type closure "+std::string(e.name));
  need(a.attnotnull==(i<31)&&a.atttypmod==-1,"nullability and unmodified declaration "+std::string(e.name));
 }
 for(const Oid oid:{83U,273U,194U,1033U,1034U}) {
  const auto t=std::find_if(s.types.begin(),s.types.end(),[&](const auto& t){return t.oid==oid;});
  need(t!=s.types.end(),"owned dependency type "+std::to_string(oid));if(t==s.types.end())continue;
  need(!t->typinput&&!t->typoutput&&!t->typreceive&&!t->typsend,"declaration does not invent codecs");
  if(oid==83)need(t->typtype=='c'&&t->typrelid==self->oid&&t->typarray==273,"actual composite back-link");
  if(oid==273)need(t->typelem==83&&t->typcategory=='A',"actual composite array pair");
  if(oid==1033)need(t->typlen==16&&!t->typbyval&&t->typalign=='d'&&t->typarray==1034,"PG18 actual aclitem size/alignment/array identity");
  if(oid==1034)need(t->typelem==1033&&t->typcategory=='A'&&t->typalign=='d',"actual aclitem array pair");
 }
}
ExprValue evaluate(ExprEvaluator& evaluator,const std::string& expression) {
 SQLParser parser;const auto parsed=parser.parse("SELECT "+expression);
 const auto* query=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
 if(!query||query->selectList.size()!=1)throw std::runtime_error("expression parsing prerequisite failed");
 return evaluator.eval(query->selectList[0].expr.get(),RowContext{});
}
#if HAS_CLASS_DECLARATION
std::vector<std::string> completeIdentity(CatalogManager& c) {
 auto out=identity(c.metadataSnapshot());
 for(const auto& pair:{std::pair<Oid,Oid>{PgClassOid_Class,1259},{PgClassOid_Type,83},
                     {PgClassOid_Type,273},{PgClassOid_Type,194},{PgClassOid_Type,1033},{PgClassOid_Type,1034}})
  for(const auto& d:c.findDepends(pair.first,pair.second,-1))
   out.push_back(packed("D",d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype));
 std::sort(out.begin(),out.end());return out;
}
#endif
}
int main() {
 using namespace dbms;
 try {
  TypeRegistry::instance().bootstrap();const auto db=testDbPath("pg_class_declaration");
  Session session;session.username="admin";session.permission=1;session.currentDB=db;
  struct Restore {Session* previous;bool analyze;~Restore(){setCurrentSession(previous);g_config.autoAnalyzeEnabled=analyze;}}
   restore{currentSession(),g_config.autoAnalyzeEnabled};
  setCurrentSession(&session);g_config.autoAnalyzeEnabled=false;
  if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)throw std::runtime_error("actual database creation failed");
  auto initial=g_engine.catalogService().get(db).metadataSnapshot();
  group("actual public bootstrap and type closure",[&]{declaration(initial);});
  group("ordinary DDL preserves self and pg_stats",[&] {
   DdlExecutor ddl;
   for(const std::string sql:{"CREATE SCHEMA class_scope",
     "CREATE TABLE class_scope.odd_rows(id integer PRIMARY KEY,n integer)",
     "CREATE INDEX odd_idx ON class_scope.odd_rows(n)",
     "ALTER TABLE class_scope.odd_rows ADD COLUMN extra text",
     "ALTER TABLE class_scope.odd_rows RENAME TO renamed_rows"})
    need(!ddl.executeSql(sql,session),"actual successful DDL "+sql);
   const auto changed=g_engine.catalogService().metadataSnapshot(db);
   declaration(changed);
   need(std::count_if(changed.relations.begin(),changed.relations.end(),[](const auto& c){return c.relname=="pg_stats"&&c.relkind=='v'&&c.relnatts==17;})==1,
        "owned statistics view remains intact");
   need(std::count_if(changed.relations.begin(),changed.relations.end(),[](const auto& c){return c.relname=="renamed_rows"&&c.relnatts==3;})==1,
        "ordinary DDL keeps actual relation metadata");
  });
  group("cold persisted declaration and startup idempotence",[&] {
   need(g_engine.catalogService().persistAll(db),"checked actual publication");
   const auto live=g_engine.catalogService().metadataSnapshot(db);
   g_engine.catalogService().evict(db);const auto path=g_engine.dbPath(db)/"pg_catalog";
   const auto before=files(path);
   const auto direct=CatalogManager::readMetadataSnapshot(path.string());
   const auto cold=g_engine.catalogService().metadataSnapshot(db);
   need(identity(live)==identity(direct)&&identity(live)==identity(cold),"full declared values survive actual CSV reload");
   declaration(cold);
   need(files(path)==before&&!g_engine.catalogService().has(db),"cold metadata stays read-only and uncached");
   const auto started=g_engine.catalogService().get(db).metadataSnapshot();
   need(identity(started)==identity(live)&&files(path)==before,"second mutable owner validates without reallocation/publication");
  });
  group("absent cold metadata does not create declaration",[&] {
   const auto missing=g_engine.dbPath(testDbPath("absent_pg_class"));
   need(!std::filesystem::exists(missing),"missing fixture prerequisite");
   const auto cold=g_engine.catalogService().metadataSnapshot(testDbPath("absent_pg_class"));
   need(cold.relations.empty()&&cold.types.empty()&&!std::filesystem::exists(missing),"read-only absence is not bootstrap");
  });
  group("selected catalog input availability and ordinary controls",[&] {
   ExprEvaluator evaluator;evaluator.setCurrentDB(db);
   const std::array<std::string,7> targets{{"pg_catalog.pg_class","pg_catalog._pg_class","pg_catalog.pg_class[]",
    "pg_catalog.pg_node_tree","pg_catalog.aclitem","pg_catalog._aclitem","pg_catalog.aclitem[]"}};
   const std::array<Oid,7> identities{{83,273,273,194,1033,1034,1034}};
   const auto declared=g_engine.catalogService().metadataSnapshot(db);
   for(size_t i=0;i<targets.size();++i) {
    const auto& target=targets[i];
    try {need(resolveDeclaredTypeName(target,&declared,&session).typeOid==identities[i],"actual declaration identity "+target);}
    catch(const DbError& e){need(false,"declaration "+target+" actualSQLSTATE="+e.sqlState());}
    bool nullRejected=false;try {(void)evaluate(evaluator,"CAST(NULL AS "+target+")");}
    catch(const DbError& e){nullRejected=e.sqlState()=="0A000";}
    need(nullRejected,"unsupported typed NULL cannot fall back to TEXT "+target);
    for(const auto& expression:{"CAST('unimplemented input' AS "+target+")","'unimplemented input'::"+target}) {
     bool rejected=false;try {(void)evaluate(evaluator,expression);}
     catch(const DbError& e){rejected=e.sqlState()=="0A000";}
     need(rejected,"nonNULL input cannot use nominal passthrough "+expression);
    }
   }
   const auto integer=evaluate(evaluator,"CAST('17' AS pg_catalog.int4)");
   need(!integer.isNull&&integer.value=="17","ordinary implemented integer input");
   const auto text=evaluate(evaluator,"CAST('exact bytes' AS pg_catalog.text)");
   need(!text.isNull&&text.value=="exact bytes","ordinary implemented text input");
   DdlExecutor ddl;need(!ddl.executeSql("CREATE TYPE public.pg_class AS ENUM ('own')",session),"actual user type shadow declaration");
   const auto shadow=evaluate(evaluator,"CAST('own' AS public.pg_class)");
   need(!shadow.isNull&&shadow.value=="own","qualified user type is separate from catalog capability");
  });
  group("legacy initialization and complete preflight rejection",[&] {
#if HAS_CLASS_DECLARATION
   const std::filesystem::path root="__t_class_legacy";CatalogManager legacy(root.string(),false);
   legacy.bootstrapSystemNamespaces();legacy.bootstrapSystemTypes();
   const auto allocated=legacy.allocateOid();
   need(class_catalog::bootstrapRelation(legacy),"real missing legacy declaration initialized");
   need(legacy.allocateOid()==allocated+1,"fixed bootstrap does not consume user object OIDs");
   need(legacy.persistAll(),"persist actual legacy upgrade");declaration(legacy.metadataSnapshot());
   const auto exact=completeIdentity(legacy);const auto disk=files(root);
   need(!class_catalog::bootstrapRelation(legacy)&&completeIdentity(legacy)==exact&&files(root)==disk,"idempotent bootstrap performs no writes");
   const std::array<std::function<void(CatalogManager&)>,9> corrupt{{
    [](auto& c){auto v=*c.getClass(1259);v.relname="collision";c.updateClass(1259,v);},
    [](auto& c){auto v=*c.getClass(1259);v.relnatts=7;c.updateClass(1259,v);},
    [](auto& c){auto v=*c.getType(194);v.typnamespace=2200;c.updateType(194,v);},
    [](auto& c){auto v=*c.getType(1033);v.typlen=8;c.updateType(1033,v);},
    [](auto& c){auto v=*c.getType(83);v.typinput=123456;c.updateType(83,v);},
    [](auto& c){auto v=c.findAttributes(1259);v.pop_back();c.replaceAttributes(1259,v);},
    [](auto& c){auto v=c.findAttributes(1259);v[17].atttypid=25;c.replaceAttributes(1259,v);},
    [](auto& c){c.removeDepend(PgClassOid_Type,83,0,PgClassOid_Class,1259,0);},
    [](auto& c){auto v=*c.getType(273);v.typelem=25;c.updateType(273,v);}}};
   for(size_t i=0;i<corrupt.size();++i) {
    CatalogManager loaded(root.string(),false);corrupt[i](loaded);const auto before=completeIdentity(loaded);const auto bytes=files(root);
    bool rejected=false;try {(void)class_catalog::bootstrapRelation(loaded);}
    catch(const DbError& e){rejected=e.sqlState()=="XX001";}
    need(rejected,"loaded declaration conflict "+std::to_string(i));
    need(completeIdentity(loaded)==before&&files(root)==bytes,"failed preflight changes no declaration or disk "+std::to_string(i));
   }
   for(size_t i=0;i<3;++i) {
    CatalogManager empty("__t_class_conflict_"+std::to_string(i),false);
    empty.bootstrapSystemNamespaces();empty.bootstrapSystemTypes();
    if(i==0){PgClassRow c;c.oid=1259;c.relname="foreign";c.relnamespace=11;empty.createClass(c);}
    if(i==1){PgClassRow c;c.oid=999;c.relname="pg_class";c.relnamespace=11;empty.createClass(c);}
    if(i==2){PgTypeRow t;t.oid=999;t.typname="aclitem";t.typnamespace=11;empty.createType(t);}
    const auto before=completeIdentity(empty);bool rejected=false;
    try {(void)class_catalog::bootstrapRelation(empty);}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
    need(rejected&&completeIdentity(empty)==before,"legacy conflicting OID/name rejected before all edits "+std::to_string(i));
   }
#else
   ++notrun;std::cout<<"NOTRUN missing actual bootstrap capability in old generation\n";
#endif
  });
  std::cout<<"PG_CLASS_DECLARATION groups="<<groups<<" checks="<<checks<<" failures="<<failures<<" NOTRUN="<<notrun<<'\n';
  return failures||notrun?1:0;
 }catch(const DbError& e){std::cerr<<"FATAL "<<e.sqlState()<<' '<<e.what()<<'\n';return 1;}
 catch(const std::exception& e){std::cerr<<"FATAL "<<e.what()<<'\n';return 1;}
}
