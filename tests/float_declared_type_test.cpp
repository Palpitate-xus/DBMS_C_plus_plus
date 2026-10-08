#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    struct Case {std::string declaration,type,state;};
    const std::vector<Case> cases={
        {"FLOAT","double precision",""},{"FLOAT(1)","real",""},
        {"FLOAT(24)","real",""},{"FLOAT(25)","double precision",""},
        {"FLOAT(53)","double precision",""},{"FLOAT(0)","","22023"},
        {"FLOAT(54)","","22023"},{"FLOAT(-1)","","42601"},
        {"FLOAT(+1)","","42601"},{"FLOAT(24,25)","","42601"}
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;std::cout<<"FLOAT_DECLARATION "<<role<<" pass="<<pass<<'\n';
    };
    SQLParser parser;ExprEvaluator evaluator;
    for(const auto& item:cases) {
        bool pass=false;
        try {const auto declaration=SQLParser::parseTypeSpecification(item.declaration);
            pass=item.state.empty() && declaration.typeName==item.type && declaration.typeMods.empty();}
        catch(const DbError& error){pass=error.sqlState()==item.state;}
        require(pass,"type envelope "+item.declaration);
        for(const auto& expression:{item.declaration+" '1.25'","CAST('1.25' AS "+item.declaration+")","'1.25'::"+item.declaration})
            for(bool binding:{false,true}) {
                pass=false;
                try {
                    const auto parsed=binding?parser.parseForBinding("SELECT "+expression):parser.parse("SELECT "+expression);
                    const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                    if(!select)pass=item.state=="42601";
                    else {
                        const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
                        pass=item.state.empty() && !value.isNull && value.typeName==item.type && value.value=="1.25";
                    }
                } catch(const DbError& error){pass=!item.state.empty() && error.sqlState()==item.state;}
                require(pass,(binding?"binding ":"ordinary ")+expression);
            }
    }
    std::cout<<"FLOAT_DECLARATION_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==70 && !failed?0:1;
}
