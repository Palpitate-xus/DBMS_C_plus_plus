#include "catalog/CatalogService.h"
#include "catalog/type_catalog.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    size_t checked=0,failures=0;
    const auto require=[&](bool good,const std::string& role) {
        ++checked;failures+=!good;std::cout<<"OWNED_TYPE_CATALOG "<<role<<" pass="<<good<<'\n';
    };
    const auto database=testDbPath("owned_type_catalog_query");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    const auto execute=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get());
        auto result=QueryPlanner::executePlanChecked(std::move(plan));
        result.throwIfFailed();
        return std::make_pair(std::move(query),std::move(result));
    };
    const auto catalogDirectory=g_engine.dbPath(database)/"pg_catalog";
    require(!std::filesystem::exists(catalogDirectory) && !g_engine.catalogService().has(database),"actual cold state");
    {
        const auto result=execute("SELECT oid, typname, typlen, typtype, typcategory, typelem, typarray, typalign, typstorage, typbyval FROM pg_catalog.pg_type WHERE oid IN (3904,3905,3906,3907,3908,3909,3910,3911,3912,3913,3926,3927) ORDER BY oid");
        const std::vector<std::vector<std::string>> expected={
            {"3904","int4range","-1","r","R","0","3905","i","x","f"},
            {"3905","_int4range","-1","b","A","3904","0","i","x","f"},
            {"3906","numrange","-1","r","R","0","3907","i","x","f"},
            {"3907","_numrange","-1","b","A","3906","0","i","x","f"},
            {"3908","tsrange","-1","r","R","0","3909","d","x","f"},
            {"3909","_tsrange","-1","b","A","3908","0","d","x","f"},
            {"3910","tstzrange","-1","r","R","0","3911","d","x","f"},
            {"3911","_tstzrange","-1","b","A","3910","0","d","x","f"},
            {"3912","daterange","-1","r","R","0","3913","i","x","f"},
            {"3913","_daterange","-1","b","A","3912","0","i","x","f"},
            {"3926","int8range","-1","r","R","0","3927","d","x","f"},
            {"3927","_int8range","-1","b","A","3926","0","d","x","f"}
        };
        require(result.second.structuredRows==expected,"actual PG18 range rows/fields/order");
        const std::vector<Oid> oids={26,19,21,18,18,26,26,18,18,16};
        require(result.first->output.size()==oids.size(),"actual column count");
        for(size_t i=0;i<oids.size();++i)require(result.first->output.at(i).typeOid==oids[i],"actual descriptor "+std::to_string(i));
    }
    require(!std::filesystem::exists(catalogDirectory) && !g_engine.catalogService().has(database),"cold read did not initialize or persist catalog");
    auto& catalog=g_engine.catalogService().get(database);
    PgTypeRow type;
    type.oid=17000;type.typname="Owned Mixed Name";type.typnamespace=2200;type.typowner=10;
    type.typlen=4;type.typbyval=true;type.typispreferred=true;type.typisdefined=false;
    type.typdelim=';';type.typrelid=17001;type.typelem=23;type.typarray=17002;
    type.typinput=17003;type.typoutput=17004;type.typreceive=17005;type.typsend=17006;
    type.typmodin=17007;type.typmodout=17008;type.typanalyze=17009;
    type.typalign='s';type.typstorage='m';type.typnotnull=true;type.typbasetype=23;
    type.typtypmod=7;type.typndims=2;type.typcollation=100;
    require(catalog.createType(type)==type.oid,"actual owned catalog record");
    std::string columns;
    for(const auto& column:ownedTypeCatalogDescriptor())columns+=(columns.empty()?"":",")+column.name;
    const std::vector<std::string> expected={"17000","Owned Mixed Name","2200","10","4","t","b","U","t","f",";","17001","23","17002","17003","17004","17005","17006","17007","17008","17009","s","m","t","23","7","2","100"};
    for(bool disk:{false,true}) {
        if(disk){require(g_engine.catalogService().persistAll(),"persist actual record");g_engine.catalogService().evict(database);}
        const auto result=execute("SELECT "+columns+" FROM pg_catalog.pg_type WHERE oid=17000");
        require(result.second.structuredRows==std::vector<std::vector<std::string>>{expected},disk?"all28 persisted fields":"all28 cached fields");
    }
    std::cout<<"OWNED_TYPE_CATALOG_CHECKED="<<checked<<" FAILED="<<failures<<'\n';
    return checked==19 && !failures?0:1;
}
