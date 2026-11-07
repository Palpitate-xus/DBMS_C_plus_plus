#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/IdentityValue.h"
#include "parser/parser.h"
#include "parser/query_binding.h"
#include <iostream>
#include <stdexcept>

using namespace dbms;
namespace {
unsigned checked=0;
void need(bool value,const std::string& why) {
    ++checked;if(!value)throw std::runtime_error(why);
}
const FunctionCallExpr* identityCall(const Expr* value) {
    if(const auto* cast=dynamic_cast<const CastExpr*>(value))return identityCall(cast->operand.get());
    return dynamic_cast<const FunctionCallExpr*>(value);
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        QueryBindingMetadata metadata;
        metadata.relation=[](const std::string& name) {
            need(name=="conflict_rows","actual target metadata lookup");
            return QueryRelationMetadata{"public","conflict_rows",
                {{"k","integer"},{"id","smallint",false,'a'},{"v","bigint"},{"note","text"}}, {}};
        };
        size_t identityLookups=0,defaultLookups=0,routineLookups=0;
        metadata.identityDefault=[&](const std::string& schema,const std::string& table,const std::string& column)
            ->std::optional<uint32_t> {
            need(schema=="public" && table=="conflict_rows","physical default owner independent of EXCLUDED");
            if(column=="id"){++identityLookups;return 12001;}
            return std::nullopt;
        };
        metadata.updateDefault=[&](const std::string&,const std::string&,const std::string& column)
            ->std::optional<std::string> {
            ++defaultLookups;
            if(column=="v")return "nextval('stored_sequence')";
            return std::nullopt;
        };
        metadata.functionType=[&](const FunctionCallExpr* call) {
            ++routineLookups;
            if(call->funcName=="nextval")return std::string("bigint");
            throw DbError("42883","function does not exist");
        };
        const std::string prefix="INSERT INTO conflict_rows(k,v,note) VALUES(1,0,'incoming') ON CONFLICT(k) DO UPDATE ";
        auto prepared=prepareQuery(prefix+"SET id=DEFAULT,v=DEFAULT,note=DEFAULT WHERE false RETURNING k,id,v,note",{},metadata);
        const auto* insert=dynamic_cast<const InsertStmt*>(prepared.ast.get());
        need(insert && insert->conflictUpdateSet.size()==3,"all conflict assignments retained");
        const auto* call=identityCall(insert->conflictUpdateSet[0].second.get());
        need(identity_value_detail::bound(call) && call->identitySequenceOid==12001,"real metadata OID bound into identity value");
        identity_value_detail::validate(*call);
        need(prepared.output.size()==4 && prepared.output[1].type=="smallint","assignment and RETURNING retain declared target type");
        need(identityLookups==1 && routineLookups==1 && defaultLookups==2,"identity value uses no SQL function metadata callback");
        const auto* ordinary=identityCall(insert->conflictUpdateSet[1].second.get());
        need(ordinary && ordinary->funcName=="nextval" && !identity_value_detail::bound(ordinary),"ordinary stored default keeps separate SQL callee");
        need(prepared.parameters.empty(),"stored default does not capture caller parameters");
        for(const auto& item:std::vector<std::pair<std::string,std::string>>{
            {"SET id=999","428C9"},{"SET id=NULL WHERE false","428C9"},
            {"SET id=missing_function()","42883"},{"SET id=999 WHERE missing_column=1","42703"},
            {"SET id=999 RETURNING missing_column","42703"},{"SET absent=DEFAULT","42703"},
            {"SET id=DEFAULT,id=DEFAULT","42601"}}) {
            std::string state;
            try{(void)prepareQuery(prefix+item.first,{},metadata);}
            catch(const DbError& error){state=error.sqlState();}
            need(state==item.second,"complete pure preflight SQLSTATE: "+item.first+" actual "+state);
        }
        metadata.updateDefault=[](const std::string&,const std::string&,const std::string& column)
            ->std::optional<std::string>{return column=="v"?std::optional<std::string>{"k"}:std::nullopt;};
        std::string state;
        try{(void)prepareQuery(prefix+"SET v=DEFAULT",{{"pl:k","k","integer",{},true,"1",1}},metadata);}
        catch(const DbError& error){state=error.sqlState();}
        need(state=="42703","stored defaults cannot capture INSERT/EXCLUDED or PL value namespaces");
        for(const auto& tail:{"SET id=DEFAULT+1","SET id=DEFAULT,","SET id=DEFAULT, WHERE false",
                "SET id=DEFAULT note=excluded.note","SET id=DEFAULT junk","SET id=DEFAULT WHERE"}) {
            SQLParser parser;need(!parser.parse(prefix+tail).success,"invalid conflict grammar consumes no ignored tail");
        }
        std::cout<<"[IDENTITY CONFLICT DEFAULT BINDING] checks="<<checked
                 <<" actual_sequence_binding_and_pure_analysis=true\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr<<"IDENTITY_CONFLICT_DEFAULT_BINDING_FAIL "<<error.what()<<'\n';return 1;
    }
}
