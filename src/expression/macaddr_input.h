#pragma once

#include "catalog/catalog.h"
#include "common/DbError.h"
#include "common/NetworkValue.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"

namespace dbms::macaddr_input_detail {

inline std::string builtinIdentity(const std::string& spelling) {
    const auto tokens = SQLParser::tokenize(spelling);
    if (tokens.size()!=1 && !(tokens.size()==3 && tokens[1]==".")) return {};
    std::string name;
    for (const auto& token : tokens) name += token;
    CatalogManager::QualifiedName qualified;
    if (!CatalogManager::parseQualifiedName(name,qualified,true) ||
        (!qualified.schema.empty() && qualified.schema!="pg_catalog")) return {};
    return qualified.name;
}

inline std::string builtinType(const std::string& spelling) {
    const auto type=builtinIdentity(spelling);
    return type=="macaddr" || type=="macaddr8" ? type : std::string{};
}

inline void validateCast(const std::string& source,const std::string& target) {
    const auto input = ExprHelper::canonicalResultTypeName(source);
    if (input!="unknown" && input!="text" && input!="character varying" && input!="character" &&
        builtinIdentity(source)!="name" && input!="macaddr" && input!="macaddr8")
        throw DbError("42846","cannot cast type " + input + " to " + target);
}

inline void validateUnknownInput(const Expr* source,const std::string& spelling) {
    const auto target = builtinType(spelling);
    if (target.empty() || !source || source->preparedSubquery) return;
    const auto* literal = dynamic_cast<const LiteralExpr*>(source);
    if (!literal || !literal->typeName.empty()) return;
    const auto tokens = SQLParser::tokenize(literal->value);
    if (tokens.size()!=1 || tokens[0].size()<2 || tokens[0].front()!='\'' || tokens[0].back()!='\'') return;
    ExprEvaluator evaluator;
    const auto value = evaluator.eval(literal,RowContext{});
    std::array<uint8_t,8> address{};
    MacAddressInputError failure;
    if (!parseMacAddress(value.value,target=="macaddr"?6:8,address,&failure))
        throw DbError(failure==MacAddressInputError::OutOfRange?"22003":"22P02",
                      "invalid input for type " + target);
}

} // namespace dbms::macaddr_input_detail
