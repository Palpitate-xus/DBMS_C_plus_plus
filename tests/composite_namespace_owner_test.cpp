#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
namespace fs=std::filesystem;
struct Checks {
    size_t controls=0,failures=0;
    template<class F> void test(const std::string& name,F action) {
        ++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"COMPOSITE_NAMESPACE_OWNER_FAIL "<<controls<<' '<<name<<" : "<<error.what()<<'\n';}
        catch(...){++failures;std::cerr<<"COMPOSITE_NAMESPACE_OWNER_FAIL "<<controls<<' '<<name<<" : nonstandard exception\n";}
    }
};
void require(bool condition,const std::string& message){if(!condition)throw std::runtime_error(message);}
template<class T,class=void>struct HasOwner:std::false_type{};
template<class T>struct HasOwner<T,std::void_t<decltype(std::declval<T&>().schema),decltype(std::declval<T&>().localName)>>:std::true_type{};
template<class T>void setOwner(T& value,const std::string& schema,const std::string& local){if constexpr(HasOwner<T>::value){value.schema=schema;value.localName=local;}}
template<class T>bool owns(const T& value,const std::string& schema,const std::string& local){if constexpr(HasOwner<T>::value)return value.schema==schema&&value.localName==local;else return false;}
template<class T>bool unknown(const T& value){if constexpr(HasOwner<T>::value)return value.schema.empty()&&value.localName.empty();else return true;}
std::string q(const std::string& value){std::string result="\"";for(char byte:value){result+=byte;if(byte=='"')result+=byte;}return result+'"';}
std::string sqlName(const std::string& schema,const std::string& local){return q(schema)+"."+q(local);}
std::string physical(const std::string& schema,const std::string& local){return schema=="public"?local:schema+"."+local;}
std::string read(const fs::path& path){std::ifstream stream(path,std::ios::binary);require(stream.good(),"read failed");return {std::istreambuf_iterator<char>(stream),std::istreambuf_iterator<char>()};}
void write(const fs::path& path,const std::string& bytes){std::ofstream stream(path,std::ios::binary|std::ios::trunc);stream<<bytes;require(stream.good(),"write failed");}
std::string hex(const std::string& value){const char* digits="0123456789abcdef";std::string result;for(unsigned char byte:value){result+=digits[byte>>4];result+=digits[byte&15];}return result;}
std::string record(const std::string& key,const std::string& schema,const std::string& local,const std::string& field="v"){
    return "DBMS_COMPOSITE_V2:"+hex(key)+"|"+hex(schema)+"|"+hex(local)+"|"+hex(field)+":"+hex("INTEGER")+"\n";
}
template<class F>void state(const std::string& expected,F action){std::string actual;try{action();}catch(const DbError& error){actual=error.sqlState();}require(actual==expected,"expected SQLSTATE "+expected+", got "+actual);}
bool ddl(DdlExecutor& executor,Session& session,const std::string& sql,std::string* code=nullptr){
    std::ostringstream output;ScopedOutputCapture capture(output);bool failed=false;
    try{failed=executor.executeSql(sql,session);}catch(const DbError& error){failed=true;if(code)*code=error.sqlState();}
    if(code&&code->empty()){const auto at=output.str().find("SQLSTATE ");if(at!=std::string::npos)*code=output.str().substr(at+9,5);}
    return failed;
}
void success(DdlExecutor& executor,Session& session,const std::string& sql){std::string code;require(!ddl(executor,session,sql,&code),"DDL rejected ["+code+"]: "+sql);}
void reject(DdlExecutor& executor,Session& session,const std::string& sql,const std::string& expected){std::string code;require(ddl(executor,session,sql,&code)&&code==expected,"expected "+expected+", got "+code+": "+sql);}
void actual(const std::string& db,const std::string& key,const std::string& schema,const std::string& local,size_t fields){
    const auto type=g_engine.getCompositeType(db,key);require(!type.name.empty(),"missing exact physical record");require(owns(type,schema,local),"missing or wrong authoritative owner");require(type.fields.size()==fields,"field count changed");
}
void alterInSnapshot(Session& session,const std::string& old,const StorageEngine::CompositeType& changed){
    DdlTransaction transaction(session);require(transaction.enableSnapshotRollback()&&transaction.begin(),"snapshot begin failed");transaction.markSnapshotDirty();
    require(g_engine.alterCompositeType(session.currentDB,old,changed)==DBStatus::OK,"alter failed");transaction.recordUpdate(DdlObjectKind::Type,old);require(transaction.commit(),"alter snapshot commit failed");
}
}

int main(){
    using namespace dbms;Checks checks;TypeRegistry::instance().bootstrap();Session session;session.username="admin";session.permission=1;session.searchPath="public, pg_catalog";
    auto* previous=currentSession();setCurrentSession(&session);struct Restore{Session* previous;~Restore(){setCurrentSession(previous);}}restore{previous};DdlExecutor executor;
    const std::vector<std::pair<std::string,std::string>> profiles={{"owner_simple","Pair"},{"Owner.Scope","Mixed.Pair"},{"Owner\"Scope","Esc\"Pair"}};
    size_t number=0;
    for(const auto& profile:profiles)for(bool outside:{false,true}){
        const auto db=testDbPath("composite_owner_history_"+std::to_string(number++));session.currentDB=db;
        const auto schema=profile.first,local=profile.second,key=physical(schema,local);
        const auto owner=outside?std::string("public"):schema,ownedLocal=outside?key:local;
        const auto declared=sqlName(owner,ownedLocal),wrong=outside?sqlName(schema,local):sqlName("public",key);
        const auto renamed=outside?"Renamed."+key:"Renamed."+local,newKey=physical(owner,renamed);
        checks.test(db+" database",[&]{require(g_engine.createDatabase(db)==DBStatus::OK,"createDatabase failed");});
        checks.test(db+" actual namespace",[&]{success(executor,session,"CREATE SCHEMA "+q(schema));});
        checks.test(db+" actual SQL composite producer",[&]{success(executor,session,"CREATE TYPE "+declared+" AS(v INTEGER)");});
        checks.test(db+" explicit owner",[&]{actual(db,key,owner,ownedLocal,1);});
        checks.test(db+" physical collision cannot replace owner",[&]{StorageEngine::CompositeType other;other.name=key;other.fields={{"other","TEXT"}};setOwner(other,outside?schema:"public",outside?local:key);require(g_engine.createCompositeType(db,other)==DBStatus::TABLE_ALREADY_EXISTS,"physical collision was not rejected");actual(db,key,owner,ownedLocal,1);});
        checks.test(db+" cold owner",[&]{StorageEngine cold;const auto type=cold.getCompositeType(db,key);require(owns(type,owner,ownedLocal)&&type.fields.size()==1,"cold owner lost");});
        checks.test(db+" wrong DROP TYPE cannot steal physical record",[&]{reject(executor,session,"DROP TYPE "+wrong,"42704");actual(db,key,owner,ownedLocal,1);});
        checks.test(db+" function declaration verifies owner",[&]{reject(executor,session,"CREATE FUNCTION public.owner_wrong(p "+wrong+") RETURNS TEXT LANGUAGE SQL AS $$SELECT 'no'::text$$","42704");require(!g_engine.udfExists(db,"owner_wrong"),"wrong owner installed a routine");});
        checks.test(db+" raw field alter preserves owner",[&]{auto type=g_engine.getCompositeType(db,key);type.fields.push_back({"added","TEXT"});require(g_engine.alterCompositeType(db,key,type)==DBStatus::OK,"field alter failed");actual(db,key,owner,ownedLocal,2);});
        checks.test(db+" legacy-shaped field input cannot erase owner",[&]{StorageEngine::CompositeType type;type.name=key;type.fields=g_engine.getCompositeType(db,key).fields;require(g_engine.alterCompositeType(db,key,type)==DBStatus::OK,"ownerless field update failed");actual(db,key,owner,ownedLocal,2);});
        checks.test(db+" explicit rename preserves namespace",[&]{auto type=g_engine.getCompositeType(db,key);type.name=newKey;setOwner(type,owner,renamed);require(g_engine.alterCompositeType(db,key,type)==DBStatus::OK,"rename failed");actual(db,newKey,owner,renamed,2);require(g_engine.getCompositeType(db,key).name.empty(),"old physical key survived rename");});
        checks.test(db+" RESTRICT uses actual owner",[&]{const auto status=g_engine.dropSchema(db,schema,false);require(status==(outside?DBStatus::OK:DBStatus::INVALID_VALUE),"wrong restrict owner/result");actual(db,newKey,owner,renamed,2);});
        checks.test(db+" CASCADE uses actual owner",[&]{if(outside)require(g_engine.createSchema(db,schema)==DBStatus::OK,"recreate empty namespace failed");require(g_engine.dropSchema(db,schema,true)==DBStatus::OK,"cascade failed");require(!g_engine.schemaExists(db,schema),"namespace marker survived");});
        checks.test(db+" cold post-drop identity",[&]{StorageEngine cold;const auto type=cold.getCompositeType(db,newKey);require(outside?owns(type,owner,renamed):type.name.empty(),"public dotted survivor or actual owned deletion wrong");});
        checks.test(db+" cleanup",[&]{require(g_engine.dropDatabase(db)==DBStatus::OK,"dropDatabase failed");});
    }
    number=0;
    for(const auto& profile:profiles){
        const auto db=testDbPath("composite_owner_rollback_"+std::to_string(number++));session.currentDB=db;const auto schema=profile.first,local=profile.second,key=physical(schema,local);std::string original;
        checks.test(db+" database",[&]{require(g_engine.createDatabase(db)==DBStatus::OK,"createDatabase failed");});
        checks.test(db+" schema",[&]{success(executor,session,"CREATE SCHEMA "+q(schema));});
        checks.test(db+" composite",[&]{success(executor,session,"CREATE TYPE "+sqlName(schema,local)+" AS(v INTEGER)");original=read(g_engine.dbPath(db)/".types");});
        checks.test(db+" begin",[&]{require(g_engine.beginTransaction(db,true)==DBStatus::OK,"begin failed");});
        checks.test(db+" rename and field alter",[&]{auto type=g_engine.getCompositeType(db,key);type.name=physical(schema,"Moved.Pair");setOwner(type,schema,"Moved.Pair");type.fields.push_back({"extra","TEXT"});alterInSnapshot(session,key,type);});
        checks.test(db+" rollback",[&]{require(g_engine.rollbackTransaction()==DBStatus::OK,"rollback failed");});
        checks.test(db+" complete bytes and owner restored",[&]{require(read(g_engine.dbPath(db)/".types")==original,"rollback changed composite record bytes");actual(db,key,schema,local,1);});
        checks.test(db+" savepoint begin",[&]{require(g_engine.beginTransaction(db,true)==DBStatus::OK,"begin failed");});
        checks.test(db+" savepoint",[&]{require(g_engine.savepoint("owner_before")==DBStatus::OK,"savepoint failed");});
        checks.test(db+" field alter after savepoint",[&]{auto type=g_engine.getCompositeType(db,key);type.fields.push_back({"extra","TEXT"});alterInSnapshot(session,key,type);});
        checks.test(db+" rollback to savepoint",[&]{require(g_engine.rollbackToSavepoint("owner_before")==DBStatus::OK,"rollback to savepoint failed");});
        checks.test(db+" savepoint owner restored",[&]{actual(db,key,schema,local,1);require(read(g_engine.dbPath(db)/".types")==original,"savepoint changed record bytes");});
        checks.test(db+" commit",[&]{require(g_engine.commitTransaction()==DBStatus::OK,"commit failed");});
        checks.test(db+" drop transaction begin",[&]{require(g_engine.beginTransaction(db,true)==DBStatus::OK,"begin failed");});
        checks.test(db+" transactional schema drop",[&]{success(executor,session,"DROP SCHEMA "+q(schema)+" CASCADE");});
        checks.test(db+" drop rollback",[&]{require(g_engine.rollbackTransaction()==DBStatus::OK,"drop rollback failed");});
        checks.test(db+" drop rollback restored owner",[&]{StorageEngine cold;require(cold.schemaExists(db,schema)&&owns(cold.getCompositeType(db,key),schema,local),"schema rollback lost authoritative owner");});
        checks.test(db+" cleanup",[&]{require(g_engine.dropDatabase(db)==DBStatus::OK,"dropDatabase failed");});
    }
    const auto db=testDbPath("composite_owner_corruption");session.currentDB=db;const auto path=g_engine.dbPath(db)/".types";std::string original;
    checks.test("corruption database",[&]{require(g_engine.createDatabase(db)==DBStatus::OK,"createDatabase failed");});
    checks.test("corruption schema",[&]{success(executor,session,"CREATE SCHEMA owner_bad");});
    checks.test("corruption real producer",[&]{success(executor,session,"CREATE TYPE owner_bad.Pair AS(v INTEGER)");original=read(path);});
    const auto valid=record("owner_bad.pair","owner_bad","pair");
    const std::vector<std::string> bad={
        valid.substr(0,valid.rfind('|'))+"\n",valid.substr(0,valid.size()-1)+"|\n",
        valid.substr(0,valid.size()-1),
        valid+record("owner_bad.pair","owner_bad","pair","other"),"owner_bad.pair|v:INTEGER\n"+valid,
        "DBMS_COMPOSITE_V2:"+hex("owner_bad.pair")+"||"+hex("pair")+"|76:494e5445474552\n",
        "DBMS_COMPOSITE_V2:"+hex("owner_bad.pair")+"|"+hex("owner_bad")+"||76:494e5445474552\n",
        "DBMS_COMPOSITE_V2:z|"+hex("owner_bad")+"|"+hex("pair")+"|76:494e5445474552\n",
        record("owner_bad.pair","public","pair")};
    for(size_t index=0;index<bad.size();++index){
        const auto label="bad V2 "+std::to_string(index);
        checks.test(label+" checked loader/raw probe",[&]{write(path,bad[index]);require(g_engine.getCompositeType(db,"owner_bad.pair").name.empty()&&!g_engine.isCompositeType(db,"owner_bad.pair"),"raw probe changed absence contract");state("XX001",[&]{(void)g_engine.getCompositeTypeNames(db);});require(read(path)==bad[index],"loader rewrote malformed metadata");});
        checks.test(label+" DROP TYPE rejects without mutation",[&]{write(path,bad[index]);reject(executor,session,"DROP TYPE owner_bad.pair","XX001");require(read(path)==bad[index],"DROP TYPE changed corrupt bytes");});
        checks.test(label+" DROP SCHEMA rejects without mutation",[&]{write(path,bad[index]);reject(executor,session,"DROP SCHEMA owner_bad CASCADE","XX001");require(read(path)==bad[index]&&g_engine.schemaExists(db,"owner_bad"),"DROP SCHEMA changed corrupt input");});
        checks.test(label+" all raw writers fail closed",[&]{write(path,bad[index]);StorageEngine::CompositeType value;value.name="writer";value.fields={{"v","INTEGER"}};const auto created=g_engine.createCompositeType(db,value),altered=g_engine.alterCompositeType(db,"owner_bad.pair",value),dropped=g_engine.dropCompositeType(db,"owner_bad.pair");require(created==DBStatus::CORRUPTED_DATA&&altered==DBStatus::CORRUPTED_DATA&&dropped==DBStatus::CORRUPTED_DATA,"writer lost corruption status");require(read(path)==bad[index],"writer changed malformed bytes");});
        checks.test(label+" exact valid bytes recover",[&]{write(path,original);actual(db,"owner_bad.pair","owner_bad","pair",1);});
    }
    checks.test("I/O remains distinct from absence",[&]{
        const auto saved=g_engine.dbPath(db)/".types.saved";fs::rename(path,saved);require(fs::create_directory(path),"I/O fixture directory failed");
        struct Recover{fs::path path,saved;~Recover(){std::error_code error;fs::remove(path,error);fs::rename(saved,path,error);}}recover{path,saved};
        require(g_engine.getCompositeType(db,"owner_bad.pair").name.empty(),"raw I/O probe must stay nonthrowing");state("58030",[&]{(void)g_engine.getCompositeTypeNames(db);});StorageEngine::CompositeType value;value.name="writer";value.fields={{"v","INTEGER"}};const auto created=g_engine.createCompositeType(db,value),altered=g_engine.alterCompositeType(db,"owner_bad.pair",value),dropped=g_engine.dropCompositeType(db,"owner_bad.pair");require(created==DBStatus::IO_ERROR&&altered==DBStatus::IO_ERROR&&dropped==DBStatus::IO_ERROR,"I/O did not retain IO_ERROR");
    });
    checks.test("corruption cleanup",[&]{require(g_engine.dropDatabase(db)==DBStatus::OK,"dropDatabase failed");});

    const auto legacy=testDbPath("composite_owner_legacy");session.currentDB=legacy;StorageEngine::CompositeType raw;raw.name="legacy_scope.Pair";raw.fields={{"v","INTEGER"}};
    checks.test("legacy database",[&]{require(g_engine.createDatabase(legacy)==DBStatus::OK,"createDatabase failed");});
    checks.test("legacy namespace",[&]{success(executor,session,"CREATE SCHEMA legacy_scope");});
    checks.test("legal raw physical API retains unknown owner",[&]{require(g_engine.createCompositeType(legacy,raw)==DBStatus::OK,"native create failed");StorageEngine cold;require(unknown(cold.getCompositeType(legacy,raw.name)),"legacy owner was guessed");});
    for(const auto& name:{sqlName("legacy_scope","Pair"),sqlName("public",raw.name)})checks.test("legacy DROP TYPE ambiguity "+name,[&]{const auto bytes=read(g_engine.dbPath(legacy)/".types");reject(executor,session,"DROP TYPE "+name,"XX001");require(read(g_engine.dbPath(legacy)/".types")==bytes,"legacy ambiguity changed bytes");});
    checks.test("legacy schema ambiguity has no mutation",[&]{const auto bytes=read(g_engine.dbPath(legacy)/".types");reject(executor,session,"DROP SCHEMA legacy_scope CASCADE","XX001");require(read(g_engine.dbPath(legacy)/".types")==bytes&&g_engine.schemaExists(legacy,"legacy_scope"),"legacy namespace was guessed");});
    checks.test("legacy field rewrite stays unknown",[&]{auto type=g_engine.getCompositeType(legacy,raw.name);type.fields.push_back({"extra","TEXT"});require(g_engine.alterCompositeType(legacy,raw.name,type)==DBStatus::OK,"legacy field alter failed");StorageEngine cold;require(unknown(cold.getCompositeType(legacy,raw.name)),"rewrite guessed legacy owner");});
    checks.test("exact native key get/drop contract",[&]{require(g_engine.getCompositeType(legacy,"legacy_scope").name.empty(),"native getter parsed prefix");require(g_engine.dropCompositeType(legacy,raw.name)==DBStatus::OK&&g_engine.getCompositeType(legacy,raw.name).name.empty(),"native exact-key drop failed");});
    checks.test("legacy cleanup",[&]{require(g_engine.dropDatabase(legacy)==DBStatus::OK,"dropDatabase failed");});
    const auto plain=testDbPath("composite_owner_legacy_plain");session.currentDB=plain;
    checks.test("legacy plain database",[&]{require(g_engine.createDatabase(plain)==DBStatus::OK,"createDatabase failed");});
    checks.test("legacy plain actual native producer",[&]{StorageEngine::CompositeType value;value.name="Plain";value.fields={{"v","INTEGER"}};require(g_engine.createCompositeType(plain,value)==DBStatus::OK,"native create failed");});
    checks.test("dot-free legacy is provably public",[&]{require(g_engine.dropSchema(plain,"public",true)==DBStatus::OK,"provable public cleanup failed");require(g_engine.getCompositeType(plain,"Plain").name.empty(),"plain record survived public drop");});
    checks.test("legacy plain cleanup",[&]{require(g_engine.dropDatabase(plain)==DBStatus::OK,"dropDatabase failed");});
    // History6*15=90 + rollback3*18=54 + corruption setup3/bad9*5/IO1/cleanup1=50
    // + unknown legacy9 + dot-free legacy4 =207. Never stop after a failed control.
    constexpr size_t expected=207;
    if(checks.controls!=expected){++checks.failures;std::cerr<<"COMPOSITE_NAMESPACE_OWNER_INVENTORY expected "<<expected<<" got "<<checks.controls<<'\n';}
    std::cout<<"COMPOSITE_NAMESPACE_OWNER_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expected<<" FAILURES "<<checks.failures<<'\n';return checks.failures?1:0;
}
