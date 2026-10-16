#pragma once

#include "catalog/catalog.h"
#include "common/DbError.h"
#include "common/NetworkValue.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"

namespace dbms::network_input_detail {

inline std::string builtinType(const std::string& spelling) {
    const auto tokens = SQLParser::tokenize(spelling);
    if (tokens.size()!=1 && !(tokens.size()==3 && tokens[1]==".")) return {};
    std::string name;
    for (const auto& token : tokens) name += token;
    CatalogManager::QualifiedName qualified;
    if (!CatalogManager::parseQualifiedName(name,qualified,true) ||
        (!qualified.schema.empty() && qualified.schema!="pg_catalog")) return {};
    return qualified.name=="inet" || qualified.name=="cidr" ? qualified.name : std::string{};
}

inline void validateCast(const std::string& source,const std::string& target) {
    const auto input = ExprHelper::canonicalResultTypeName(source);
    if (input!="unknown" && input!="text" && input!="character varying" && input!="character" &&
        input!="name" && input!="inet" && input!="cidr")
        throw DbError("42846","cannot cast type " + input + " to " + target);
}

// Parse only a genuine UNKNOWN string constant during transformation. Do
// not execute a typed value, function, parameter or prepared child query.
inline void validateUnknownInput(const Expr* source,const std::string& spelling) {
    const auto target = builtinType(spelling);
    if (target.empty() || !source || source->preparedSubquery) return;
    const auto* literal = dynamic_cast<const LiteralExpr*>(source);
    if (!literal || !literal->typeName.empty()) return;
    const auto tokens = SQLParser::tokenize(literal->value);
    if (tokens.size()!=1 || tokens[0].size()<2 || tokens[0].front()!='\'' || tokens[0].back()!='\'') return;
    ExprEvaluator evaluator;
    const auto value = evaluator.eval(literal,RowContext{});
    NetworkAddressValue address;
    if (!parseNetworkAddress(value.value,address,target=="cidr"))
        throw DbError("22P02","invalid input for type " + target);
}

} // namespace dbms::network_input_detail
