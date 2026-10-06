#include "utils/plpgsql.h"
#include <cassert>
#include <iostream>
int main() {
    using namespace dbms;
    for (const auto& item : std::vector<std::pair<std::string,bool>>{
        {"BEGIN SELECT 1; RETURN 2; END",false},
        {"BEGIN SELECT 1 WHERE false; RETURN 2; END",false},
        {"BEGIN INSERT INTO rows VALUES(1) RETURNING id; RETURN 2; END",false},
        {"BEGIN PERFORM 1; RETURN 2; END",true},
        {"BEGIN INSERT INTO rows VALUES(1); RETURN 2; END",true},
        {"DECLARE x INT; BEGIN SELECT 1 INTO x; RETURN x; END",true}}) {
        PlPgsqlHost host; size_t calls=0;
        host.queryPrepared=[&](const std::string& sql,const std::vector<QueryBindingDatum>&,const PlPgsqlQueryOptions& options){
            ++calls; PlPgsqlQueryResult result; result.ok=true;
            result.columnCount=sql.find("INSERT")==0 && sql.find("RETURNING")==std::string::npos?0:1;
            result.rowCount=sql.find("false")==std::string::npos?1:0;
            if(result.columnCount) result.columnTypes={"integer"};
            if(result.rowCount && result.columnCount) result.firstRow={"1"};
            if(item.first.find("INTO")!=std::string::npos && item.first.find("SELECT 1 INTO")!=std::string::npos)
                assert(options.maxRows==1);
            return result;
        };
        std::string value,error,state;
        const bool ok=PlPgsql::run(item.first,{},host,value,error,nullptr,nullptr,nullptr,&state);
        if(ok!=item.second) std::cerr<<"PL_DEST "<<item.first<<" actual "<<ok<<" state "<<state<<'\n';
        assert(ok==item.second && calls==1);
        if(!ok) assert(state=="42601");
        else assert(value==(item.first.find("SELECT 1 INTO")!=std::string::npos?"1":"2"));
    }
    PlPgsqlHost failing;
    failing.queryPrepared=[](const std::string&,const std::vector<QueryBindingDatum>&,const PlPgsqlQueryOptions&){
        PlPgsqlQueryResult result; result.ok=false; result.columnCount=1;
        result.sqlState="42883"; result.message="missing function"; return result;
    };
    std::string value,error,state;
    assert(!PlPgsql::run("BEGIN SELECT missing(1); END",{},failing,value,error,nullptr,nullptr,nullptr,&state));
    assert(state=="42883");
    std::cout<<"[PL QUERY DESTINATION] column shape, zero rows, PERFORM/INTO and original-error priority passed\n";
}
