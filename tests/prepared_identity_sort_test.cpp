#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "catalog/systables.h"
#include <algorithm>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    struct Case {
        std::string type;
        std::vector<std::string> input,ascending;
        size_t tiesCount,tiesExpected;
    };
    const std::string negative128(1,static_cast<char>(0x80));
    const std::string negative1(1,static_cast<char>(0xff));
    const Case cases[]={
        {"oid",{"10","2","4294967295","0","2"},{"0","2","2","10","4294967295"},2,3},
        {"name",{"a","10","Z","2","a"},{"10","2","Z","a","a"},4,5},
        {"\"char\"",{"Z",negative1,"2","",negative128,"2"},{negative128,negative1,"","2","2","Z"},4,5}
    };
    size_t checked=0,failures=0;
    const auto require=[&](bool good,const std::string& role){++checked;failures+=!good;std::cout<<"PREPARED_IDENTITY_SORT "<<role<<" pass="<<good<<'\n';};
    for(const auto& item:cases) {
        QueryRowDescriptor descriptor={{"key",item.type,false,0,mapBuiltinTypeNameToOid(item.type)}};
        QueryBindingMetadata metadata;
        metadata.relation=[&](const std::string& name){return QueryRelationMetadata{"public",name,descriptor,{}};};
        const auto execute=[&](const std::string& sql) {
            auto query=std::make_shared<PreparedQuery>(prepareQuery(sql,{},metadata));
            TableSchema schema;Column column;column.dataName="key";column.dataType=item.type;
            column.isVariableLength=true;schema.append(column);
            auto values=std::make_shared<PreparedQueryRows>();
            for(const auto& value:item.input)values->push_back({ExprValue(item.type,value,false)});
            values->push_back({ExprValue(item.type,"",true)});
            auto source=std::make_unique<PreparedSourceRowsOp>(descriptor,
                [values](size_t index,std::vector<ExprValue>& row){if(index>=values->size())return false;row=values->at(index);return true;});
            auto* select=static_cast<SelectStmt*>(query->ast.get());
            auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,"",query,select,schema,std::move(source));
            auto result=QueryPlanner::executePlanChecked(std::move(plan));result.throwIfFailed();
            return result;
        };
        for(bool descending:{false,true})for(bool nullFirst:{false,true}) {
            const auto sql="SELECT key FROM input ORDER BY key "+std::string(descending?"DESC":"ASC")+
                " NULLS "+(nullFirst?"FIRST":"LAST");
            auto ordered=item.ascending;if(descending)std::reverse(ordered.begin(),ordered.end());
            std::vector<std::vector<std::string>> expected;
            std::vector<std::vector<bool>> nulls;
            if(nullFirst){expected.push_back({""});nulls.push_back({true});}
            for(const auto& value:ordered){expected.push_back({value});nulls.push_back({false});}
            if(!nullFirst){expected.push_back({""});nulls.push_back({true});}
            const auto result=execute(sql);
            require(result.structuredRows==expected && result.structuredNulls==nulls,item.type+" "+sql);
        }
        const auto ties=execute("SELECT key FROM input ORDER BY key ASC NULLS LAST FETCH FIRST "+std::to_string(item.tiesCount)+" ROWS WITH TIES");
        std::vector<std::vector<std::string>> expectedTies;
        for(size_t i=0;i<item.tiesExpected;++i)expectedTies.push_back({item.ascending.at(i)});
        require(ties.structuredRows==expectedTies,item.type+" actual peer equality");
        auto unique=item.ascending;unique.erase(std::unique(unique.begin(),unique.end()),unique.end());
        std::vector<std::vector<std::string>> expectedUnique;
        for(const auto& value:unique)expectedUnique.push_back({value});expectedUnique.push_back({""});
        const auto distinct=execute("SELECT DISTINCT key FROM input ORDER BY key ASC NULLS LAST");
        require(distinct.structuredRows==expectedUnique && distinct.structuredNulls.back()==std::vector<bool>{true},item.type+" DISTINCT keeps actual NULL/empty identity");
    }
    std::cout<<"PREPARED_IDENTITY_SORT_CHECKED="<<checked<<" FAILED="<<failures<<'\n';
    return checked==18 && !failures?0:1;
}
