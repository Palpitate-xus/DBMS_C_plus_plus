#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();SQLParser parser;ExprEvaluator evaluator;
    const std::vector<std::string> words={
        "all","analyse","analyze","and","any","array","as",
        "asc","asymmetric","between","both","case","cast","check",
        "coalesce","collate","column","constraint","create","current_catalog","current_date",
        "current_role","current_time","current_timestamp","current_user","default","deferrable","desc",
        "distinct","do","else","end","except","exists","extract",
        "false","fetch","for","foreign","from","grant","greatest",
        "group","grouping","having","in","initially","inout","intersect",
        "into","json_array","json_arrayagg","json_exists","json_object","json_objectagg","json_query",
        "json_scalar","json_serialize","json_table","json_value","lateral","leading","least",
        "limit","localtime","localtimestamp","merge_action","none","normalize","not",
        "null","nullif","offset","on","only","or","order",
        "out","overlay","placing","position","precision","primary","references",
        "returning","row","select","session_user","setof","some","substring",
        "symmetric","system_user","table","then","to","trailing","treat",
        "trim","true","union","unique","user","using","values",
        "variadic","when","where","window","with","xmlattributes","xmlconcat",
        "xmlelement","xmlexists","xmlforest","xmlnamespaces","xmlparse","xmlpi","xmlroot",
        "xmlserialize","xmltable"
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"TYPE_KEYWORD_ROLE "<<role<<" pass="<<pass<<'\n';};
    for(const auto& word:words) {
        bool pass=false;
        try {(void)SQLParser::parseTypeSpecification(word);}catch(const DbError& error){pass=error.sqlState()=="42601";}
        require(pass,"unquoted "+word);
        const auto quoted="\""+word+"\"";
        try {const auto type=SQLParser::parseTypeSpecification(quoted);pass=type.typeName==quoted;}
        catch(const DbError&){pass=false;}
        require(pass,"quoted identifier "+word);
    }
    for(const auto& expression:{"TRUE 'x'","NULL 'x'","ARRAY 'x'","SELECT 'x'"})for(bool binding:{false,true}) {
        const auto parsed=binding?parser.parseForBinding(std::string("SELECT ")+expression):parser.parse(std::string("SELECT ")+expression);
        require(!parsed.success && parsed.sqlState=="42601",expression);
    }
    for(const auto& expression:{"CASE 'x' WHEN 'x' THEN 1 ELSE 2 END","CASE true WHEN true THEN 1 ELSE 2 END"})for(bool binding:{false,true}) {
        bool pass=false;
        try {
            const auto parsed=binding?parser.parseForBinding(std::string("SELECT ")+expression):parser.parse(std::string("SELECT ")+expression);
            const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
            if(select){const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});pass=!value.isNull && value.value=="1";}
        }catch(const DbError&){pass=false;}
        require(pass,expression);
    }
    std::cout<<"TYPE_KEYWORD_ROLE_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==254 && !failed?0:1;
}
