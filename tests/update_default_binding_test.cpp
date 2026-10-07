#include "parser/query_binding.h"
#include "parser/parser.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    metadata.relation=[](const std::string& spelling) {
        assert(spelling=="rows");
        return QueryRelationMetadata{"public","rows",{{"id","integer"},{"v","integer"}}, {}};
    };
    size_t lookups=0,routines=0;
    std::string definition="writer()+1";
    metadata.updateDefault=[&](const std::string& schema,const std::string& relation,const std::string& column)
        ->std::optional<std::string> {
        assert(schema=="public" && relation=="rows" && column=="v");
        ++lookups;return definition;
    };
    metadata.functionType=[&](const FunctionCallExpr* call) {
        assert(call->funcName=="writer" && call->args.empty());
        ++routines;return std::string("integer");
    };
    auto prepared=prepareQuery("UPDATE rows SET v=DEFAULT WHERE false RETURNING v",{},metadata);
    auto* update=dynamic_cast<UpdateStmt*>(prepared.ast.get());assert(update);
    auto* binary=dynamic_cast<BinaryOpExpr*>(update->setClauses.front().second.get());assert(binary && binary->op=="+");
    const auto* call=dynamic_cast<const FunctionCallExpr*>(binary->left.get());
    assert(call && call->resolvedResultType=="integer" && lookups==1 && routines==1);
    assert(prepared.parameters.empty() && prepared.output.size()==1 && prepared.output[0].type=="integer");
    // A stored default must not capture a caller's row/PL datum merely
    // because it has the same spelling. There is no execution callback here.
    definition="id";
    bool rejected=false;
    try {
        (void)prepareQuery("UPDATE rows SET v=DEFAULT",{{"pl:id","id","integer",{},true,"7",1}},metadata);
    } catch(const DbError& error) {rejected=error.sqlState()=="42703";}
    assert(rejected);
    // Native schema APIs can store SQL that DDL would prohibit. No child
    // statement/source ownership may escape this independent value binder.
    definition="(SELECT 1)";
    rejected=false;
    try {(void)prepareQuery("UPDATE rows SET v=DEFAULT",{},metadata);}
    catch(const DbError& error){rejected=error.sqlState()=="0A000";}
    assert(rejected);
    metadata.updateDefault=[](const std::string&,const std::string&,const std::string&)->std::optional<std::string>{return std::nullopt;};
    prepared=prepareQuery("UPDATE rows SET v=DEFAULT RETURNING v",{},metadata);
    update=dynamic_cast<UpdateStmt*>(prepared.ast.get());assert(update);
    const auto* cast=dynamic_cast<const CastExpr*>(update->setClauses.front().second.get());
    assert(cast && cast->implicit && cast->typeName=="integer");
    const auto* literal=dynamic_cast<const LiteralExpr*>(cast->operand.get());
    assert(literal && SQLParser::toLower(literal->value)=="null");
    std::cout<<"[UPDATE DEFAULT BINDING] pure definitions and independent namespaces passed\n";
}
