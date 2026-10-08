#include "parser/parser.h"
#include "parser/ast.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    struct Case {std::string type,name;std::vector<std::string> mods;};
    const std::vector<Case> cases={
        {"BIT VARYING(2)","BIT VARYING",{"2"}},
        {"BIT VARYING(2)[]","BIT VARYING[]",{"2"}},
        {"BIT VARYING(2)[3][4]","BIT VARYING[]",{"2"}},
        {"TIME(2) WITHOUT TIME ZONE","time",{"2"}},
        {"TIME(2) WITH TIME ZONE","timetz",{"2"}},
        {"TIMESTAMP(2) WITHOUT TIME ZONE","timestamp",{"2"}},
        {"TIMESTAMP(2) WITH TIME ZONE","timestamptz",{"2"}},
        {"TIME(2) WITH TIME ZONE[]","timetz[]",{"2"}},
        {"NUMERIC(5,-2)","NUMERIC",{"5","-2"}},
        {"NUMERIC(5,-2)[]","NUMERIC[]",{"5","-2"}},
        {"pg_catalog.varbit(2)","pg_catalog.varbit",{"2"}},
        {"\"pg_catalog\".\"varbit\"(2)[]","\"pg_catalog\".\"varbit\"[]",{"2"}},
        {"\"Odd Schema\".\"Odd Type\"(2)[]","\"Odd Schema\".\"Odd Type\"[]",{"2"}},
        {"DOUBLE PRECISION[]","DOUBLE PRECISION[]",{}},
        {"CHARACTER VARYING(4)","CHARACTER VARYING",{"4"}},
        {"INTEGER","INTEGER",{}},
        {"INTEGER[]","INTEGER[]",{}},
        {"VARBIT","VARBIT",{}}
    };
    size_t checked=0,failed=0;std::cout<<std::unitbuf;
    const auto require=[&](bool good,const std::string& role) {
        ++checked;if(!good)++failed;
        std::cout<<"CAST_DECLARED_TYPE "<<role<<" pass="<<good<<'\n';
    };
    SQLParser parser;
    for(bool binding:{false,true})for(const auto& item:cases) {
        const auto sql="SELECT CAST($1 AS "+item.type+") AS chosen";
        auto parsed=binding?parser.parseForBinding(sql):parser.parse(sql);
        const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
        const auto* cast=select && select->selectList.size()==1
            ?dynamic_cast<const CastExpr*>(select->selectList.front().expr.get()):nullptr;
        const bool good=cast && cast->typeName==item.name && cast->typeMods==item.mods;
        require(good,(binding?"binding ":"ordinary ")+sql);
    }
    // A postfix cast consumes the declaration only, not its output alias.
    // The parameter's original byte provenance remains on its actual node.
    for(const auto& item:cases) {
        const auto sql="SELECT $1::"+item.type+" AS chosen";
        auto parsed=parser.parseForBinding(sql);
        const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
        const auto* cast=select && select->selectList.size()==1
            ?dynamic_cast<const BinaryOpExpr*>(select->selectList.front().expr.get()):nullptr;
        const auto* declaration=cast?dynamic_cast<const LiteralExpr*>(cast->right.get()):nullptr;
        const auto* input=cast?dynamic_cast<const ParameterExpr*>(cast->left.get()):nullptr;
        bool good=cast && cast->op=="::" && declaration && input && select->selectList.front().alias=="chosen";
        if(good) {
            const auto envelope=SQLParser::parseTypeSpecification(declaration->value);
            const auto expected=SQLParser::parseTypeSpecification(item.type);
            good=envelope.typeName==expected.typeName && envelope.typeMods==expected.typeMods &&
                envelope.isArray==expected.isArray && input->sourceBegin==7 && input->sourceEnd==9;
        }
        require(good,sql);
    }
    for(const auto& sql:{
        "SELECT $1::INTEGER chosen", "SELECT $1::INTEGER + 1 AS chosen",
        "SELECT $1::NUMERIC(5,2)::TEXT AS chosen", "SELECT $1::TEXT COLLATE \"C\" AS chosen"}) {
        const auto parsed=parser.parseForBinding(sql);
        const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
        require(select && select->selectList.size()==1 && select->selectList.front().alias=="chosen",sql);
    }
    for(const auto& sql:{"SELECT CAST($1 AS)", "SELECT $1::",
        "SELECT CAST($1 AS INTEGER[)", "SELECT $1::INTEGER[",
        "SELECT CAST($1 AS NUMERIC(5,))", "SELECT $1::NUMERIC(5,)",
        "SELECT CAST($1 AS pg_catalog.)", "SELECT $1::pg_catalog.",
        "SELECT CAST($1 AS NUMERIC(5 2))", "SELECT $1::NUMERIC(5 2)"}) {
        require(!parser.parseForBinding(sql).success,sql);
    }
    std::cout<<"CAST_DECLARED_TYPE_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    assert(checked==68 && failed==0);
}
