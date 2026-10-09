#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "expression/regtype_cast.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    const auto db=testDbPath("regtype_cast_graph");
    size_t checks=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checks;failures+=!pass;std::cout<<"REGTYPE_CAST_GRAPH "<<role<<" pass="<<pass<<'\n';};
    require(g_engine.createDatabase(db)==DBStatus::OK,"create isolated database");
    const std::vector<std::string> invalid={"smallint","numeric","real","double precision","boolean","date","\"char\"","regproc","regclass","regnamespace","regrole","regtype[]"};
    for(const auto& target:invalid)for(const auto& input:{std::string("NULL::regtype"),std::string("'integer'::regtype")}) {
        bool pass=false;try{(void)g_engine.prepareBoundQuery(db,"SELECT CAST("+input+" AS "+target+") WHERE false");}
        catch(const DbError& error){pass=error.sqlState()=="42846";}
        require(pass,"pure invalid outgoing cast "+input+" -> "+target);
    }
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        QueryBindingDatum datum;datum.identity="regtype parameter";datum.name="value";datum.type="regtype";datum.position=1;datum.origin=origin;
        bool pass=false;try{(void)g_engine.prepareBoundQuery(db,"SELECT CAST($1 AS boolean) WHERE false",{datum});}
        catch(const DbError& error){pass=error.sqlState()=="42846";}
        require(pass,"actual parameter origin "+std::to_string(static_cast<int>(origin))+" cannot bypass type graph");
    }
    for(const auto& source:{"regproc","regclass","regnamespace","regrole"}) {
        bool pass=false;try{(void)g_engine.prepareBoundQuery(db,std::string("SELECT CAST(NULL::")+source+" AS regtype) WHERE false");}
        catch(const DbError& error){pass=error.sqlState()=="42846";}
        require(pass,std::string("other OID aliases have no direct regtype cast: ")+source);
    }
    require(!g_engine.catalogService().has(db) && !std::filesystem::exists(g_engine.dbPath(db)/"pg_catalog"),"pure errors create no cold catalog/cache");
    std::cout<<"REGTYPE_CAST_GRAPH_CHECKED="<<checks<<" FAILED="<<failures<<'\n';
    return checks==33 && !failures?0:1;
}
