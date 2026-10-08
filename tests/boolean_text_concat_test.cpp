#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include <iostream>

int main() {
    using namespace dbms;
    SQLParser parser;ExprEvaluator evaluator;
    struct Case {std::string expression,value;bool null=false;};
    const std::vector<Case> cases={
        {"TEXT 'x' || TRUE","xtrue"},{"TRUE || TEXT 'x'","truex"},
        {"TEXT 'x' || FALSE","xfalse"},{"FALSE || TEXT 'x'","falsex"},
        {"TEXT 'x' || CAST(true AS boolean)","xtrue"},
        {"TEXT 'x' || CAST(false AS boolean)","xfalse"},
        {"TEXT 'x' || NULL::boolean","",true},{"NULL::boolean || TEXT 'x'","",true},
        {"'x' || TRUE","xtrue"},{"TRUE || 'x'","truex"},
        {"TEXT 'x' || 1","x1"},{"1 || TEXT 'x'","1x"},
        {"CAST('a' AS CHAR(3)) || TRUE","atrue"},
        {"FALSE || CAST('a' AS CHAR(3))","falsea"}
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"BOOLEAN_TEXT_CONCAT "<<role<<" pass="<<pass<<'\n';};
    for(const auto& item:cases) {
        for(bool binding:{false,true}) {
            bool pass=false;
            const auto parsed=binding?parser.parseForBinding("SELECT "+item.expression):parser.parse("SELECT "+item.expression);
            const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
            if(select){const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});pass=value.isNull==item.null && value.typeName=="text" && value.value==item.value;}
            require(pass,(binding?"binding ":"ordinary ")+item.expression);
        }
        const auto value=ExprHelper::evalString(item.expression,{},{});
        require(value.ok && value.isNull==item.null && value.typeName=="text" && value.value==item.value,"stored-expression "+item.expression);
    }
    std::cout<<"BOOLEAN_TEXT_CONCAT_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==42 && !failed?0:1;
}
