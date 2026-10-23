#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
namespace {
size_t checked=0,failures=0;
void require(bool good,const std::string& label) {
    ++checked;failures+=!good;
    std::cout<<"NAMESPACE_CATALOG_NATIVE "<<label<<" PASS="<<good<<std::endl;
}
std::map<std::string,std::string> files(const std::filesystem::path& root) {
    std::map<std::string,std::string> result;
    if(std::filesystem::exists(root))for(const auto& entry:std::filesystem::recursive_directory_iterator(root))
        if(entry.is_regular_file()) {
            std::ifstream input(entry.path(),std::ios::binary);
            result[entry.path().lexically_relative(root).string()]=
                std::string(std::istreambuf_iterator<char>(input),std::istreambuf_iterator<char>());
        }
    return result;
}
void read(StorageEngine& engine,const std::string& db,const std::string& sql,
          const std::vector<std::vector<std::string>>& expected) {
    std::string state;
    std::vector<std::vector<std::string>> rows;
    try {
        auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,sql));
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&engine,db,query,query->ast.get()));
        result.throwIfFailed();rows=std::move(result.structuredRows);
    } catch(const DbError& error){state=error.sqlState();}
    require(state.empty() && rows==expected,sql+" state="+state);
}
void reject(StorageEngine& engine,const std::string& db,const std::string& sql,const std::string& expected) {
    std::string state;
    try{(void)engine.prepareBoundQuery(db,sql);}catch(const DbError& error){state=error.sqlState();}
    require(state==expected,sql+" state="+state);
}
int cold(const std::string& database,Oid oid) {
    StorageEngine engine;
    const auto directory=engine.dbPath(database)/"pg_catalog";
    const auto before=files(directory);
    // Startup recovery may already load a catalog. Establish a genuinely
    // cold source after startup, then keep both no-insertion assertions.
    engine.catalogService().evict(database);
    require(!before.empty() && !engine.catalogService().has(database),"independent exec starts without cached catalog");
    read(engine,database,"SELECT oid,nspname,nspowner FROM pg_catalog.pg_namespace WHERE nspname='Owned Mixed 界'",
         {{std::to_string(oid),"Owned Mixed 界","42"}});
    read(engine,database,"SELECT count(*) AS n FROM pg_namespace WHERE nspname='only_second'",{{"0"}});
    reject(engine,database,"SELECT * FROM pg_catalog.pg_namespace WHERE FALSE","0A000");
    reject(engine,database,"SELECT nspacl FROM pg_catalog.pg_namespace WHERE FALSE","0A000");
    require(!engine.catalogService().has(database),"cold query remains metadata-only, no cache bootstrap");
    require(files(directory)==before,"cold query does not persist/allocate/alter metadata");
    std::cout<<"NAMESPACE_CATALOG_COLD_COMPLETE "<<checked<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
}

int main(int argc,char** argv) {
    TypeRegistry::instance().bootstrap();
    if(argc==4 && std::string(argv[1])=="cold")return cold(argv[2],static_cast<Oid>(std::stoul(argv[3])));
    StorageEngine engine;
    const std::string first="namespace_source_first",second="namespace_source_second",empty="namespace_source_empty";
    require(engine.createDatabase(first)==DBStatus::OK,"create first actual database");
    require(engine.createDatabase(second)==DBStatus::OK,"create second actual database");
    require(engine.createDatabase(empty)==DBStatus::OK,"create cold empty database");
    const auto emptyDirectory=engine.dbPath(empty)/"pg_catalog";
    read(engine,empty,"SELECT count(*) AS n FROM pg_catalog.pg_namespace",{{"0"}});
    require(!engine.catalogService().has(empty) && !std::filesystem::exists(emptyDirectory),"empty read creates no fabricated namespaces or files");
    auto& catalog=engine.catalogService().get(first);
    const auto oid=catalog.createNamespace("Owned Mixed 界",42);
    require(oid!=INVALID_OID,"allocate real namespace identity with non-default owner");
    require(engine.catalogService().get(second).createNamespace("only_second",13)!=INVALID_OID,"seed independent database namespace");
    const auto expected=std::vector<std::vector<std::string>>{{std::to_string(oid),"Owned Mixed 界","42"}};
    read(engine,first,"SELECT oid,nspname,nspowner FROM pg_namespace WHERE nspname='Owned Mixed 界'",expected);
    read(engine,first,"SELECT oid,nspname,nspowner FROM \"pg_catalog\".\"pg_namespace\" WHERE oid="+std::to_string(oid),expected);
    read(engine,first,"SELECT nspname,nspowner FROM pg_catalog.pg_namespace WHERE nspname='only_second'",{});
    read(engine,second,"SELECT nspname,nspowner FROM pg_catalog.pg_namespace WHERE nspname='only_second'",{{"only_second","13"}});
    read(engine,second,"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='Owned Mixed 界'",{});
    {
        std::vector<Oid> oids;
        std::string state;
        try {
            const auto query=engine.prepareBoundQuery(first,"SELECT oid,nspname,nspowner FROM pg_namespace WHERE FALSE");
            for(const auto& column:query.output)oids.push_back(column.typeOid);
        } catch(const DbError& error){state=error.sqlState();}
        require(state.empty() && oids==std::vector<Oid>{26,19,26},"empty descriptor keeps actual builtin type identities state="+state);
    }
    for(const auto* sql:{"SELECT * FROM pg_namespace WHERE FALSE",
                        "SELECT n.* FROM pg_catalog.pg_namespace AS n LIMIT 0",
                        "SELECT nspacl FROM pg_namespace WHERE FALSE",
                        "SELECT count(nspacl) FROM pg_namespace WHERE FALSE",
                        "SELECT nspname FROM pg_namespace WHERE nspacl IS NULL"})reject(engine,first,sql,"0A000");
    for(const auto* sql:{"SELECT count(*) FROM pg_namespace a NATURAL JOIN pg_namespace b",
                        "SELECT count(*) FROM pg_namespace a NATURAL LEFT JOIN pg_namespace b",
                        "SELECT a.nspname FROM pg_namespace a JOIN pg_namespace b USING(nspacl) WHERE FALSE",
                        "SELECT n.* FROM pg_namespace AS n(a,b,c) WHERE FALSE",
                        "SELECT a FROM pg_namespace AS n(a,b,c,d) WHERE FALSE"})reject(engine,first,sql,"0A000");
    reject(engine,first,"SELECT nspacl FROM pg_namespace AS n(nspacl) WHERE FALSE","42702");
    read(engine,first,"SELECT * FROM (SELECT nspname FROM pg_namespace WHERE nspname='Owned Mixed 界') AS d",{{"Owned Mixed 界"}});
    reject(engine,first,"SELECT missing FROM pg_namespace WHERE FALSE","42703");
    reject(engine,first,"SELECT wrong.nspname FROM pg_namespace WHERE FALSE","42P01");
    std::string legacyState;
    try{(void)engine.queryPgCatalog("pg_namespace",{},{"nspname"},{});}catch(const DbError& error){legacyState=error.sqlState();}
    require(legacyState=="0A000","unbound legacy API cannot invent namespaces from database names");
    require(engine.catalogService().persistAll(),"persist real catalogs");
    const auto path=engine.dbPath(first)/"pg_catalog";
    const auto before=files(path);
    const auto child=::fork();
    require(child>=0,"fork independent cold reader");
    if(child==0) {
        const auto encoded=std::to_string(oid);
        ::execl(argv[0],argv[0],"cold",first.c_str(),encoded.c_str(),nullptr);
        _exit(127);
    }
    if(child>0) {
        int status=0;
        const auto waited=::waitpid(child,&status,0);
        require(waited==child && WIFEXITED(status) && WEXITSTATUS(status)==0,"actual cold exec passed");
    }
    require(files(path)==before,"independent reader preserves persisted metadata");
    engine.catalogService().evict(first);
    read(engine,first,"SELECT oid,nspname,nspowner FROM pg_namespace WHERE nspname='Owned Mixed 界'",expected);
    require(!engine.catalogService().has(first),"evicted reader does not initialize cache");
    require(files(path)==before,"evicted reader preserves persisted metadata");
    std::cout<<"NAMESPACE_CATALOG_NATIVE_COMPLETE "<<checked<<" FAILURES "<<failures<<std::endl;
    return checked==35 && !failures?0:1;
}
