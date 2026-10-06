#pragma once

#include "catalog/catalog.h"
#include "common/DbError.h"
#include "common/GeometryValue.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

namespace dbms::geometric_input_detail {

// Raw SQL spelling is resolved once. A quoted mixed-case/custom-schema type
// must not inherit a builtin input function just because its bytes case-fold.
inline std::string builtinType(const std::string& spelling) {
    const auto tokens=SQLParser::tokenize(spelling);
    if(tokens.size()!=1 && !(tokens.size()==3 && tokens[1]==".")) return {};
    std::string name;for(const auto& token:tokens)name+=token;
    CatalogManager::QualifiedName qualified;
    if(!CatalogManager::parseQualifiedName(name,qualified,true) ||
        (!qualified.schema.empty() && qualified.schema!="pg_catalog"))return {};
    static const std::set<std::string> names={"point","line","lseg","box","path","polygon","circle"};
    return names.count(qualified.name)?qualified.name:std::string{};
}

// Only a genuine unknown SQL string input is transformed during analysis.
// The codec is the same one used by typed literals, storage and runtime casts.
// Parameters, typed CAST chains, routines and prepared query children stay
// dynamic; numeric/arithmetic evaluation is not an input transformation.
inline void validateUnknownInput(const Expr* source,const std::string& spelling) {
    const auto type=builtinType(spelling);
    if(type.empty() || !source || source->preparedSubquery)return;
    const auto* literal=dynamic_cast<const LiteralExpr*>(source);
    if(!literal || !literal->typeName.empty())return;
    const auto tokens=SQLParser::tokenize(literal->value);
    if(tokens.size()!=1 || tokens[0].size()<2 || tokens[0].front()!='\'' || tokens[0].back()!='\'')return;
    ExprEvaluator evaluator;
    const auto datum=evaluator.eval(literal,RowContext{});
    std::string normalized;
    if(!normalizeGeometryText(datum.value,type,normalized))
        throw DbError("22P02","invalid input syntax for type "+type);
}

} // namespace dbms::geometric_input_detail
