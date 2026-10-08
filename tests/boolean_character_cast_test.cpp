#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "parser/query_binding.h"
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner; ExprEvaluator evaluator; size_t controls=0, failures=0;
    const auto require=[&](bool valid,const std::string& label) {
        ++controls; if(!valid){++failures;std::cerr<<"[BOOLEAN CHARACTER FAIL] "<<label<<'\n';}
    };
    const std::vector<std::pair<std::string,std::string>> targets={{"text","text"},{"varchar","character varying"},
        {"char","character"},{"varchar(3)","character varying"},{"char(6)","character"}};
    for(const auto& type:{"boolean","bool","text"}) for(const auto& source:std::vector<std::optional<std::string>>{"t","f","true","false","1","0",{}})
        for(const auto& target:targets) {
            const bool boolean=std::string(type)!="text"; std::string expected=source.value_or("");
            if(boolean && source) expected=ExprValue(type,*source).asBool()?"true":"false";
            if(target.first=="char") expected=expected.substr(0,1),expected.resize(1,' ');
            if(target.first=="varchar(3)") expected=expected.substr(0,3);
            if(target.first=="char(6)") expected=expected.substr(0,6),expected.resize(6,' ');
            const auto sql="SELECT CAST($1 AS "+target.first+") AS value"; auto parsed=SQLParser().parse(sql);
            const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
            RowContext row;row.setParameters({ExprValue(type,source.value_or(""),!source)});
            try {
                const auto value=evaluator.eval(select->selectList.front().expr.get(),row);
                require(ExprHelper::canonicalResultTypeName(value.typeName)==target.second && value.isNull==!source && (!source || value.value==expected),
                    std::string("real cast evaluator ")+type+" "+source.value_or("NULL")+" "+target.first);
                auto prepared=std::make_shared<PreparedQuery>(owner.prepareBoundQuery("unused",sql,
                    {{"owned-bool-character-input","p",type,{},true,source,1}}));
                auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&owner,"unused",prepared,prepared->ast.get()),prepared->output);
                std::vector<ExprValue> cells;
                require(prepared->output.size()==1 && ExprHelper::canonicalResultTypeName(prepared->output[0].type)==target.second && cursor->next(cells) &&
                    cells.size()==1 && ExprHelper::canonicalResultTypeName(cells[0].typeName)==target.second && cells[0].isNull==!source && (!source || cells[0].value==expected) && !cursor->next(cells),
                    std::string("actual bound cursor ")+type+" "+source.value_or("NULL")+" "+target.first);
                cursor->close();
            }catch(const DbError& error){require(false,sql+" "+error.sqlState());}
        }
    require(controls==210,"all 105 canonical BOOLEAN/alias/TEXT datum pairs reached both owners");
    std::cout<<"[BOOLEAN CHARACTER CAST] all "<<controls<<" evaluator/binder/cursor/NULL/width/TEXT passthrough controls; failures="<<failures<<'\n';
    return failures?1:0;
}
