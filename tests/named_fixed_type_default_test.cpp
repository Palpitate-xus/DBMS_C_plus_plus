#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    ExprEvaluator evaluator;
    struct Case {std::string declaration,input,literal,cast,type;};
    const std::vector<Case> cases={
        {"CHAR","abc","abc","a","character"},
        {"CHARACTER","abc","abc","a","character"},
        {"bpchar","abc","abc","abc","character"},
        {"pg_catalog.bpchar","abc","abc","abc","character"},
        {"\"bpchar\"","abc","abc","abc","character"},
        {"BIT","01","01","0","bit"},
        {"pg_catalog.bit","01","01","01","bit"},
        {"\"bit\"","01","01","01","bit"},
        {"CHAR(2)","abc","ab","ab","character"},
        {"bpchar(2)","abc","ab","ab","character"},
        {"pg_catalog.bpchar(2)","abc","ab","ab","character"},
        {"BIT(2)","01","01","01","bit"},
        {"pg_catalog.bit(2)","01","01","01","bit"}
    };
    size_t checked=0,failed=0;
    const auto check=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;
        std::cout<<"NAMED_FIXED_DEFAULT "<<role<<" pass="<<pass<<'\n';
    };
    for(const auto& item:cases) {
        const std::vector<std::string> expressions={
            item.declaration+" '"+item.input+"'",
            "CAST('"+item.input+"' AS "+item.declaration+")",
            "'"+item.input+"'::"+item.declaration};
        for(size_t i=0;i<expressions.size();++i) {
            const auto& expression=expressions[i];
            const auto& expected=i?item.cast:item.literal;
            for(bool binding:{false,true}) {
                bool pass=false;
                try {
                    const auto parsed=binding?parser.parseForBinding("SELECT "+expression):parser.parse("SELECT "+expression);
                    const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                    if(select) {
                        const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
                        pass=!value.isNull && value.value==expected &&
                            ExprHelper::canonicalResultTypeName(value.typeName)==item.type;
                    }
                }catch(const std::exception& error) {std::cout<<error.what()<<'\n';}
                check(pass,(binding?"binding ":"ordinary ")+expression);
            }
            const auto value=ExprHelper::evalString(expression,{},{});
            check(value.ok && !value.isNull && value.value==expected &&
                ExprHelper::canonicalResultTypeName(value.typeName)==item.type,
                "stored-expression "+expression);
        }
    }
    std::cout<<"NAMED_FIXED_DEFAULT_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==117 && !failed?0:1;
}
