#include "parser/parser.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    struct Control {std::string sql,role;std::vector<std::string> casts;};
    const std::vector<Control> controls={
        {"SELECT 'a'::TEXT LIKE 'a'", "LIKE", {"TEXT"}},
        {"SELECT 'a'::CHAR(2) LIKE 'a'", "LIKE", {"CHAR(2)"}},
        {"SELECT 'Ann'::NAME ILIKE 'a%'", "ILIKE", {"NAME"}},
        {"SELECT 'a'::TEXT SIMILAR TO '%'", "SIMILAR TO", {"TEXT"}},
        {"SELECT 1::INT IN(1,2)", "IN", {"INT"}},
        {"SELECT 1::INT BETWEEN 1 AND 2", "BETWEEN", {"INT"}},
        {"SELECT NULL LIKE (nextval('calls'))::TEXT ESCAPE '#'", "LIKE ESCAPE", {"TEXT"}},
        {"SELECT NULL::BYTEA LIKE '%'::BYTEA ESCAPE '##'::BYTEA", "LIKE ESCAPE", {"BYTEA","BYTEA","BYTEA"}},
        {"SELECT 'a'::TEXT NOT LIKE 'b'::TEXT", "NOT LIKE", {"TEXT","TEXT"}},
        {"SELECT 'a'::TEXT NOT ILIKE 'b'", "NOT ILIKE", {"TEXT"}},
        {"SELECT 'a'::TEXT NOT SIMILAR TO '%'", "NOT SIMILAR TO", {"TEXT"}},
        {"SELECT 1::DOUBLE PRECISION >= 0", ">=", {"DOUBLE PRECISION"}},
        {"SELECT NULL::TIME(2) WITH TIME ZONE IS NULL", "IS NULL", {"TIME(2) WITH TIME ZONE"}},
        {"SELECT NULL::INTERVAL DAY TO SECOND(3) LIKE '%'", "LIKE", {"INTERVAL DAY TO SECOND(3)"}},
        {"SELECT NULL::TEXT[] LIKE '%'", "LIKE", {"TEXT[]"}},
        {"SELECT NULL::\"like\" LIKE '%'", "LIKE", {"\"like\""}},
    };
    size_t failures=0;
    for(const auto& control:controls) {
        SQLParser parser;auto parsed=parser.parseForBinding(control.sql);
        const auto* select=parsed.isValid()?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
        const Expr* root=select && select->selectList.size()==1 ? select->selectList[0].expr.get() : nullptr;
        std::string role;
        if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(root))role=binary->op;
        else if(const auto* function=dynamic_cast<const FunctionCallExpr*>(root))role=function->funcName;
        else if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(root))role=unary->op;
        std::vector<std::string> casts;
        const auto collect=[&](const auto& self,const Expr* node)->void {
            if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(node)) {
                self(self,binary->left.get());
                if(binary->op=="::") {
                    const auto* type=dynamic_cast<const LiteralExpr*>(binary->right.get());
                    casts.push_back(type?type->value:"<missing type>");
                } else self(self,binary->right.get());
            } else if(const auto* call=dynamic_cast<const FunctionCallExpr*>(node)) {
                for(const auto& argument:call->args)self(self,argument.get());
            } else if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(node))self(self,unary->operand.get());
        };
        collect(collect,root);
        std::cout<<"PATTERN CAST PARSER "<<control.sql<<" role="<<role<<" casts=";
        for(const auto& type:casts)std::cout<<'['<<type<<']';
        const bool valid=role==control.role && casts==control.casts;
        std::cout<<" valid="<<valid<<std::endl;
        if(!valid)++failures;
    }
    std::cout<<"[PATTERN CAST PARSER] failures="<<failures<<std::endl;
    assert(failures==0);
}
