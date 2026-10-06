#include "query_binding.h"
#include "parser/parser.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include "expression/common_type.h"
#include "expression/equality_type.h"
#include "expression/expr_helper.h"
#include "expression/array_type.h"
#include <algorithm>
#include <map>
#include <set>

namespace dbms {
namespace {
std::string identifier(const std::string& input) {
    CatalogManager::QualifiedName name;
    // A single quoted identifier containing '.' is one component.
    if (!CatalogManager::parseQualifiedName(input, name, true) || !name.schema.empty())
        throw DbError("42601", "invalid identifier: " + input);
    return name.name;
}
struct Range {
    std::string schema, name;
    QueryRowDescriptor columns;
    std::set<std::string> hiddenUnqualified;
    size_t occurrence = 0;
    bool mergedUsing = false;
    std::string relationSchema, relationName;
    const Stmt* cteStatement = nullptr;
    std::shared_ptr<PreparedQuery> viewQuery;
};
using Namespace = std::vector<Range>;
void checkRangeConflicts(const Namespace& left, const Namespace& right) {
    for (const auto& a : left) for (const auto& b : right) {
        if (a.name.empty() || a.name != b.name) continue;
        // PostgreSQL permits distinct unaliased physical relations with the
        // same basename, because their real schema qualifiers disambiguate
        // them. An alias (schema hidden), CTE or repeated physical occurrence
        // does not gain that exception.
        if (!a.schema.empty() && !b.schema.empty() &&
            !a.relationName.empty() && !b.relationName.empty() &&
            (a.relationSchema != b.relationSchema || a.relationName != b.relationName)) continue;
        throw DbError("42712", "table name \"" + a.name + "\" specified more than once");
    }
}
struct CteDescription { QueryRowDescriptor columns; const Stmt* statement = nullptr; };
using Ctes = std::map<std::string, CteDescription>;
bool decimalIntegerConstant(const std::string& value) {
    size_t begin = !value.empty() && (value.front() == '+' || value.front() == '-') ? 1 : 0;
    return begin < value.size() && std::all_of(value.begin() + begin, value.end(),
        [](unsigned char character) { return character >= '0' && character <= '9'; });
}
void validateArrayConstant(const Expr* source, const std::string& type) {
    const auto* literal = dynamic_cast<const LiteralExpr*>(source);
    if (!literal || literal->preparedSubquery || !literal->typeName.empty()) return;
    const auto tokens = SQLParser::tokenize(literal->value);
    if (tokens.size()!=1 || tokens.front().empty() ||
        (tokens.front().front()!='\'' && SQLParser::toLower(tokens.front())!="null")) return;
    CastExpr cast; cast.typeName=type; cast.operand=std::make_unique<LiteralExpr>(*literal);
    ExprEvaluator pure; (void)pure.eval(&cast,RowContext{});
}

class Binder {
public:
    PreparedQuery result;
    const std::vector<QueryBindingDatum>& datums;
    const QueryBindingMetadata& metadata;
    std::map<std::string, size_t> slots;
    size_t depth = 0;
    size_t nextSourceOccurrence = 0;
    std::vector<const Stmt*> statementOwners;
    std::vector<const Ctes*> cteScopes;
    std::map<const Stmt*, std::vector<const Expr*>> projectionLeaves;
    Binder(const std::string& sql, const std::vector<QueryBindingDatum>& values,
           const QueryBindingMetadata& descriptions) : datums(values), metadata(descriptions) {
        result.source = sql;
    }

    Range registerSource(Range range, const FromItem* source = nullptr) {
        range.occurrence = nextSourceOccurrence++;
        result.sourceRanges.push_back({range.occurrence,
            statementOwners.empty() ? nullptr : statementOwners.back(), source,
            range.schema, range.name, range.columns, range.mergedUsing,
            range.relationSchema, range.relationName, range.cteStatement,
            range.hiddenUnqualified, range.viewQuery});
        return range;
    }

    std::string parameter(ExprPtr& node, const QueryBindingDatum& datum) {
        if (node->sourceBegin == std::string::npos || node->sourceEnd > result.source.size())
            throw DbError("XX000", "parameter reference has no source provenance");
        auto [position, inserted] = slots.emplace(datum.identity, result.parameters.size());
        if (inserted) result.parameters.emplace_back(datum.type,
            datum.value.value_or(""), !datum.value.has_value());
        auto bound = std::make_unique<ParameterExpr>();
        bound->slot = position->second; bound->declaredType = datum.type;
        bound->sourceBegin = node->sourceBegin; bound->sourceEnd = node->sourceEnd;
        result.uses.push_back({bound->sourceBegin, bound->sourceEnd, bound->slot});
        node = std::move(bound); return datum.type;
    }

    void coerceCaseInput(ExprPtr& input, const std::string& sourceType,
                         const std::string& targetType) {
        if (!input) {
            auto null = std::make_unique<LiteralExpr>();
            null->value = "null";
            input = std::move(null);
        }
        if (common_type_detail::canonical(sourceType) == targetType) return;
        auto cast = std::make_unique<CastExpr>();
        cast->typeName = targetType;
        cast->implicit = true;
        cast->sourceBegin = input->sourceBegin; cast->sourceEnd = input->sourceEnd;
        cast->operand = std::move(input);
        // Only conversion of an actual UNKNOWN string/NULL literal belongs
        // to transformation. No function, parameter, row, arithmetic or
        // already-typed CAST is executed to prepare a CASE expression.
        const auto* literal = dynamic_cast<const LiteralExpr*>(cast->operand.get());
        if (common_type_detail::canonical(sourceType) == "unknown" && literal &&
            !literal->preparedSubquery && literal->typeName.empty()) {
            const auto tokens = SQLParser::tokenize(literal->value);
            if (tokens.size() == 1 && (SQLParser::toLower(tokens.front()) == "null" ||
                (!tokens.front().empty() && tokens.front().front() == '\''))) {
                if (metadata.assignmentInput)
                    metadata.assignmentInput({"",targetType},literal,"unknown");
                ExprEvaluator evaluator;
                (void)evaluator.eval(cast.get(),RowContext{});
            }
        }
        input = std::move(cast);
    }

    static std::string arrayElement(const std::string& type) {
        auto canonical = ExprHelper::canonicalResultTypeName(type);
        if (canonical.size() < 2 || canonical.compare(canonical.size()-2,2,"[]") != 0) return {};
        canonical.resize(canonical.size()-2); return canonical;
    }
    std::string expression(ExprPtr& node, const std::vector<Namespace>& scopes,
                           const std::string& arrayContext = {},bool allowSetReturning=false) {
        if (!node) return "unknown";
        switch (node->type) {
        case ExprType::Parameter: {
            const auto* value = static_cast<ParameterExpr*>(node.get());
            if (value->declaredType.empty()) {
                for (const auto& datum : datums) if (datum.position == value->slot + 1)
                    return parameter(node, datum);
                throw DbError("42P02", "unbound parameter " + value->toString());
            }
            if (value->slot >= result.parameters.size()) throw DbError("42P02", "invalid prepared parameter");
            return value->declaredType;
        }
        case ExprType::ColumnRef: {
            auto* column = static_cast<ColumnRefExpr*>(node.get());
            if (column->column == "*") return "record";
            const QueryBindingDatum* datum = nullptr;
            for (const auto& candidate : datums) {
                if (candidate.name != column->column) continue;
                const bool qualified = !column->table.empty() || !column->schema.empty();
                if ((!qualified && candidate.visible) ||
                    (column->schema.empty() && !column->table.empty() &&
                     std::find(candidate.qualifiers.begin(), candidate.qualifiers.end(),
                               column->table) != candidate.qualifiers.end())) {
                    datum = &candidate;
                    break; // frames arrive innermost first
                }
            }
            size_t matches = 0;
            bool rangeFound = false;
            std::string sourceType;
            QueryColumnBinding resolved;
            for (size_t scopeDepth = 0; scopeDepth < scopes.size(); ++scopeDepth) {
                const auto& scope = scopes[scopeDepth];
                for (const auto& range : scope) {
                    if (!column->table.empty() && column->table != range.name) continue;
                    if (!column->schema.empty() && column->schema != range.schema) continue;
                    if (!column->table.empty()) rangeFound = true;
                    for (size_t ordinal = 0; ordinal < range.columns.size(); ++ordinal) {
                        const auto& field = range.columns[ordinal];
                        if (field.name != column->column) continue;
                        if (column->table.empty() && range.hiddenUnqualified.count(field.name)) continue;
                        ++matches; sourceType = field.type;
                        resolved = {scopeDepth, range.occurrence, ordinal, field.type, range.mergedUsing};
                    }
                }
                if (matches || rangeFound) break; // nearest SQL level wins
            }
            if (matches > 1 || (matches && datum))
                throw DbError("42702", "column reference \"" + column->toString() + "\" is ambiguous");
            if (matches) {
                column->binding = std::move(resolved);
                return sourceType;
            }
            if (datum) {
                return parameter(node, *datum);
            }
            if (!column->table.empty() && !rangeFound)
                throw DbError("42P01", "missing FROM-clause entry for table \"" + column->table + "\"");
            throw DbError("42703", "column \"" + column->toString() + "\" does not exist");
        }
        case ExprType::Literal: {
            auto* literal = static_cast<LiteralExpr*>(node.get());
            if (!literal->typeName.empty()) {
                if (metadata.assignmentInput) metadata.assignmentInput({"", literal->typeName}, literal, literal->typeName);
                return literal->typeName;
            }
            if (literal->value == "*") return "record";
            if (decimalIntegerConstant(literal->value))
                return ExprHelper::inferValuesResultType(literal->value);
            const auto tokens = SQLParser::tokenize(literal->value);
            if (tokens.empty()) return "unknown";
            if (tokens.front() == "(" && tokens.size() > 1 &&
                (SQLParser::toLower(tokens[1]) == "select" || SQLParser::toLower(tokens[1]) == "with"))
                return subquery(*literal, scopes);
            if (tokens.size() != 1)
                throw DbError("0A000", "opaque expression requires structured preparation");
            const auto low = SQLParser::toLower(tokens.front());
            if (low == "true" || low == "false") return "boolean";
            if (low == "null" || tokens.front().front() == '\'') return "unknown";
            return ExprHelper::inferValuesResultType(literal->value);
        }
        case ExprType::CastExpr: {
            auto* cast = static_cast<CastExpr*>(node.get());
            expression(cast->operand, scopes, cast->typeName);
            if (metadata.assignmentInput) metadata.assignmentInput({"", cast->typeName}, cast, cast->typeName);
            return cast->typeName;
        }
        case ExprType::UnaryOp: {
            auto* unary = static_cast<UnaryOpExpr*>(node.get());
            const auto type = expression(unary->operand, scopes);
            const auto op = SQLParser::toLower(unary->op);
            const auto* literal = dynamic_cast<const LiteralExpr*>(unary->operand.get());
            if ((op == "-" || op == "+") && literal && literal->typeName.empty() &&
                !literal->preparedSubquery && decimalIntegerConstant(literal->value)) {
                // PostgreSQL absorbs an integer constant's lexical sign
                // before choosing INT/BIGINT/NUMERIC. This is not arithmetic
                // folding: casts, parameters and routines retain runtime
                // overflow and demand semantics.
                auto signedLiteral = std::make_unique<LiteralExpr>();
                signedLiteral->value = literal->value;
                if (op == "-") {
                    if (signedLiteral->value.front() == '-') signedLiteral->value.erase(0,1);
                    else {
                        if (signedLiteral->value.front() == '+') signedLiteral->value.erase(0,1);
                        signedLiteral->value.insert(signedLiteral->value.begin(),'-');
                    }
                }
                signedLiteral->sourceBegin = node->sourceBegin; signedLiteral->sourceEnd = node->sourceEnd;
                const auto signedType = ExprHelper::inferValuesResultType(signedLiteral->value);
                node = std::move(signedLiteral); return signedType;
            }
            return op == "not" || op.rfind("is ", 0) == 0 ? "boolean" : type;
        }
        case ExprType::BinaryOp: {
            auto* binary = static_cast<BinaryOpExpr*>(node.get());
            const auto* castTarget = binary->op=="::" ? dynamic_cast<const LiteralExpr*>(binary->right.get()) : nullptr;
            const auto left = expression(binary->left, scopes, castTarget ? castTarget->value : std::string());
            if (binary->op == "::") {
                const auto* type = dynamic_cast<const LiteralExpr*>(binary->right.get());
                if (!type) throw DbError("42601", "cast requires a type name");
                if (metadata.assignmentInput) metadata.assignmentInput({"", type->value}, binary, type->value);
                return type->value; // grammar type, not a SQL value namespace
            }
            const auto right = expression(binary->right, scopes);
            if (binary->op=="||") {
                binary->arrayConcat = ExprHelper::resolveArrayConcatTypes(left,right);
                if (binary->arrayConcat) {
                    validateArrayConstant(binary->left.get(),binary->arrayConcat->leftType);
                    validateArrayConstant(binary->right.get(),binary->arrayConcat->rightType);
                    if (metadata.assignmentInput) {
                        metadata.assignmentInput({"",binary->arrayConcat->leftType},binary->left.get(),left);
                        metadata.assignmentInput({"",binary->arrayConcat->rightType},binary->right.get(),right);
                    }
                    return binary->arrayConcat->elementType + "[]";
                }
            }
            static const std::set<std::string> predicates = {"=", "<>", "!=", "<", ">", "<=", ">=",
                "AND", "OR", "LIKE", "ILIKE", "IN", "NOT IN", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"};
            return predicates.count(binary->op) ? "boolean" : left;
        }
        case ExprType::QuantifiedComparison: {
            auto* quantified = static_cast<QuantifiedComparisonExpr*>(node.get());
            const auto left = expression(quantified->left, scopes);
            auto right = expression(quantified->right, scopes);
            const bool query = quantified->right && quantified->right->preparedSubquery;
            if(query && right=="unknown") {
                // A SELECT output finalizes UNKNOWN as TEXT before resolving
                // the outer operator (unlike an ARRAY's context argument).
                right="text";
                auto* child=dynamic_cast<SelectStmt*>(quantified->right->preparedSubquery.get());
                if(!child)throw DbError("42601","quantified child requires SELECT/VALUES");
                if(child->command==SqlCommand::Values) {
                    for(auto& row:child->valuesRows) {
                        if(row.size()!=1)throw DbError("42601","subquery must return only one column");
                        coerceCaseInput(row.front(),"unknown",right);
                    }
                } else {
                    if(child->selectList.size()!=1)throw DbError("42601","subquery must return only one column");
                    coerceCaseInput(child->selectList.front().expr,"unknown",right);
                }
                result.statementOutputs.at(child).front().type=right;
                static_cast<LiteralExpr*>(quantified->right.get())->typeName=right;
            }
            const auto element = query ? right : arrayElement(right);
            if (element.empty()) throw DbError("42809", "op ANY/ALL (array) requires array on right side");
            quantified->comparison = ExprEvaluator::resolveComparison(quantified->op,left,element);
            const auto lcollation = ExprEvaluator::analyzeExplicitResultCollation(quantified->left.get());
            const auto rcollation = ExprEvaluator::analyzeExplicitResultCollation(quantified->right.get());
            if (!lcollation.empty() && !rcollation.empty() && lcollation != rcollation)
                throw DbError("42P21", "collation mismatch between explicit collations");
            quantified->comparison->collation = lcollation.empty() ? rcollation : lcollation;
            if (metadata.assignmentInput) {
                metadata.assignmentInput({"",quantified->comparison->leftType},quantified->left.get(),left);
                // A child SQL output is already prepared at its own boundary.
                if (!query) metadata.assignmentInput({"",right},quantified->right.get(),right);
            }
            return "boolean";
        }
        case ExprType::FunctionCall: {
            auto* call = static_cast<FunctionCallExpr*>(node.get());
            std::string first = "unknown";
            for (size_t i = 0; i < call->args.size(); ++i) {
                if (i == 0 && call->schema.empty() && SQLParser::toLower(call->funcName) == "extract") {
                    if (const auto* field = dynamic_cast<ColumnRefExpr*>(call->args[i].get())) {
                        if (!field->table.empty() || !field->schema.empty())
                            throw DbError("42601", "EXTRACT requires an unqualified field name");
                        continue; // grammar field, not a value name
                    }
                }
                if (call->schema.empty() && SQLParser::toLower(call->funcName) == "exists") {
                    if (auto* child = dynamic_cast<LiteralExpr*>(call->args[i].get())) {
                        subquery(*child, scopes, false); continue;
                    }
                }
                const auto type = expression(call->args[i], scopes); if (i == 0) first = type;
            }
            for (auto& arg : call->namedArgs) expression(arg.value, scopes);
            expression(call->filter, scopes); window(call->over, scopes);
            call->setReturning=metadata.setReturning?metadata.setReturning(call):std::nullopt;
            if(call->setReturning) {
                if(!allowSetReturning)throw DbError("0A000","set-returning functions are not allowed in this expression context");
                call->setReturning->elementType=ExprHelper::canonicalResultTypeName(call->setReturning->elementType);
                return call->setReturning->elementType;
            }
            const auto type = metadata.functionType ? metadata.functionType(call) : std::string();
            return type.empty() ? first : type;
        }
        case ExprType::CaseExpr: {
            auto* conditional = static_cast<CaseExpr*>(node.get());
            const auto switchType = expression(conditional->switchExpr, scopes);
            if (conditional->switchExpr && switchType == "unknown")
                coerceCaseInput(conditional->switchExpr,switchType,"text");
            conditional->simpleComparisonTypes.clear();
            std::vector<std::string> thenTypes;
            for (auto& clause : conditional->whenClauses) {
                const auto conditionType = expression(clause.first, scopes);
                if (!conditional->switchExpr) {
                    if (common_type_detail::canonical(conditionType) != "unknown" &&
                        common_type_detail::canonical(conditionType) != "boolean")
                        throw DbError("42804","argument of CASE/WHEN must be type boolean");
                    coerceCaseInput(clause.first,conditionType,"boolean");
                } else {
                    const auto equality=resolveBuiltinEquality(
                        switchType=="unknown"?"text":switchType,conditionType);
                    conditional->simpleComparisonTypes.push_back(equality);
                    coerceCaseInput(clause.first,conditionType,equality.second);
                }
                thenTypes.push_back(expression(clause.second,scopes));
            }
            // Transform source WHEN/THEN clauses before ELSE; choose the
            // common result type and perform input conversions ELSE first.
            const auto elseType = expression(conditional->elseExpr,scopes);
            std::vector<std::string> resultTypes{elseType};
            resultTypes.insert(resultTypes.end(),thenTypes.begin(),thenTypes.end());
            const auto type = selectCommonType(resultTypes,"CASE");
            coerceCaseInput(conditional->elseExpr,elseType,type);
            for (size_t i=0;i<thenTypes.size();++i)
                coerceCaseInput(conditional->whenClauses[i].second,thenTypes[i],type);
            return type;
        }
        case ExprType::ArrayExpr: {
            auto* array = static_cast<ArrayExpr*>(node.get());
            if (arrayContext.size()>=2 && arrayContext.compare(arrayContext.size()-2,2,"[]")==0)
                array->elementType = ExprHelper::canonicalResultTypeName(arrayContext.substr(0,arrayContext.size()-2));
            std::vector<std::string> types;
            for (auto& element : array->elements) {
                auto type = expression(element,scopes,arrayContext);
                if(const auto* literal=dynamic_cast<const LiteralExpr*>(element.get());
                    literal && literal->typeName.empty() && !literal->preparedSubquery)
                    type=ExprHelper::inferValuesResultType(literal->value);
                types.push_back(type);
            }
            array->nestedElements=std::any_of(types.begin(),types.end(),array_detail::isArray);
            if(!types.empty()) (void)array_detail::commonElement(types);
            if (array->elementType.empty()) {
                array->elementType=array_detail::commonElement(types);
            }
            if(!arrayContext.empty())for(const auto& type:types)
                array_detail::checkExplicitElementCast(type,array->elementType);
            for (size_t i=0;i<array->elements.size();++i) {
                validateArrayConstant(array->elements[i].get(),array->elementType);
                if (metadata.assignmentInput)
                    metadata.assignmentInput({"array element",array->elementType},array->elements[i].get(),types[i]);
            }
            return array->elementType + "[]";
        }
        case ExprType::RowExpr:
            for (auto& element : static_cast<RowExpr*>(node.get())->elements) expression(element, scopes);
            return "record";
        default: throw DbError("0A000", "expression requires structured preparation");
        }
    }

    // Historical scalar/EXISTS nodes carry SQL text. Parse their raw source,
    // not toString(), into a real child before binding, and retain byte offsets.
    std::string subquery(LiteralExpr& literal, const std::vector<Namespace>& scopes, bool scalar = true) {
        if (literal.sourceBegin == std::string::npos || literal.sourceEnd > result.source.size())
            throw DbError("0A000", "subquery has no source provenance");
        const size_t begin = result.source.find('(', literal.sourceBegin) + 1;
        const size_t end = result.source.rfind(')', literal.sourceEnd - 1);
        if (begin == 0 || end < begin) throw DbError("42601", "invalid subquery");
        SQLParser parser;
        auto parsed = parser.parseForBinding(result.source.substr(begin, end - begin));
        if (!parsed.isValid()) throw DbError("42601", parsed.error);
        if (parsed.stmt->command != SqlCommand::Select && parsed.stmt->command != SqlCommand::Values)
            throw DbError("42601", "subquery requires a SELECT or VALUES primary statement");
        // Child coordinate rebasing is structural, not identifier matching.
        rebase(*parsed.stmt, begin);
        const auto columns = statement(*parsed.stmt, scopes, cteScopes.empty() ? Ctes{} : *cteScopes.back());
        literal.preparedSubquery = std::move(parsed.stmt);
        if (scalar && columns.size() != 1) throw DbError("42601", "subquery must return only one column");
        literal.typeName = scalar ? columns.front().type : "boolean";
        return literal.typeName;
    }
    void window(WindowDef& value, const std::vector<Namespace>& scopes) {
        for (auto& field : value.partitionBy) expression(field, scopes);
        for (auto& field : value.orderBy) expression(field.first, scopes);
        expression(value.frameStart, scopes); expression(value.frameEnd, scopes);
    }
    static std::string projectionTypeLabel(const std::string& spelling) {
        auto type=common_type_detail::canonical(spelling);
        if(common_type_detail::array(type))type.resize(type.size()-2);
        static const std::map<std::string,std::string> catalogNames={
            {"boolean","bool"},{"smallint","int2"},{"integer","int4"},{"bigint","int8"},
            {"real","float4"},{"double precision","float8"},{"bit varying","varbit"}};
        if(const auto found=catalogNames.find(type);found!=catalogNames.end())return found->second;
        if(TypeRegistry::instance().findType(type))return type;
        CatalogManager::QualifiedName name;
        return CatalogManager::parseQualifiedName(spelling,name,true)?name.name:type;
    }
    static std::pair<std::string,int> projectionLabel(const Expr* expression) {
        if(!expression)return {"",0};
        if(const auto* column=dynamic_cast<const ColumnRefExpr*>(expression))return {column->column,2};
        if(const auto* call=dynamic_cast<const FunctionCallExpr*>(expression))
            return call->funcName.find(' ')==std::string::npos?std::make_pair(identifier(call->funcName),2):std::make_pair(std::string(),0);
        const Expr* operand=nullptr;std::string castType;
        if(const auto* cast=dynamic_cast<const CastExpr*>(expression)){operand=cast->operand.get();castType=cast->typeName;}
        else if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(expression);binary&&binary->op=="::") {
            operand=binary->left.get();
            if(const auto* target=dynamic_cast<const LiteralExpr*>(binary->right.get()))castType=target->value;
        }
        if(!castType.empty()) {
            const auto label=projectionLabel(operand);
            return label.second==2?label:std::make_pair(projectionTypeLabel(castType),1);
        }
        if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(expression);
            unary&&SQLParser::toLower(unary->op).rfind("collate ",0)==0)return projectionLabel(unary->operand.get());
        if(const auto* conditional=dynamic_cast<const CaseExpr*>(expression)) {
            const auto label=projectionLabel(conditional->elseExpr.get());
            return label.second>1?label:std::make_pair(std::string("case"),1);
        }
        if(dynamic_cast<const ArrayExpr*>(expression))return {"array",1};
        if(dynamic_cast<const RowExpr*>(expression))return {"row",1};
        if(const auto* literal=dynamic_cast<const LiteralExpr*>(expression);literal&&!literal->typeName.empty())
            return {projectionTypeLabel(literal->typeName),1};
        return {"",0};
    }
    QueryRowDescriptor project(std::vector<SelectItem>& items, const std::vector<Namespace>& scopes,
                               std::vector<const Expr*>* leaves = nullptr) {
        QueryRowDescriptor columns;
        for (auto& item : items) {
            if (!item.expr) throw DbError("42601", "query projection has no expression");
            if (item.expr && item.expr->type == ExprType::Literal &&
                static_cast<LiteralExpr*>(item.expr.get())->value == "*") {
                if (scopes.empty()) throw DbError("42601", "SELECT * has no source");
                for (const auto& range : scopes.front())
                    for (const auto& column : range.columns)
                        if (!range.hiddenUnqualified.count(column.name)) {
                            columns.push_back(column);
                            if (leaves) leaves->push_back(nullptr);
                        }
                continue;
            }
            if (const auto* star = dynamic_cast<ColumnRefExpr*>(item.expr.get()); star && star->column == "*") {
                bool found = false;
                if (!scopes.empty()) for (const auto& range : scopes.front()) {
                    if (range.name != star->table || (!star->schema.empty() && range.schema != star->schema)) continue;
                    found = true; columns.insert(columns.end(), range.columns.begin(), range.columns.end());
                    if (leaves) leaves->insert(leaves->end(), range.columns.size(), nullptr);
                }
                if (!found) throw DbError("42P01", "missing FROM-clause entry for qualified star");
                continue;
            }
            std::string name = "?column?";
            const bool columnLabel = dynamic_cast<ColumnRefExpr*>(item.expr.get());
            if (!item.alias.empty()) name = identifier(item.alias);
            else if(const auto label=projectionLabel(item.expr.get());label.second)name=label.first;
            const bool queryTarget=!statementOwners.empty() && statementOwners.back()->command==SqlCommand::Select;
            const auto type = expression(item.expr, scopes,{},queryTarget);
            if (columnLabel && item.alias.empty() && item.expr->type == ExprType::Parameter) {
                if (item.sourceExpressionEnd == std::string::npos)
                    throw DbError("XX000", "projection has no source provenance");
                std::string quoted = "\"";
                for (char c : name) { quoted += c; if (c == '"') quoted += c; }
                quoted += '"';
                item.alias = quoted;
                result.projectionAliases.emplace_back(item.sourceExpressionEnd, quoted);
            }
            columns.push_back({name, type});
            if (leaves) leaves->push_back(item.expr.get());
        }
        return columns;
    }
    QueryRowDescriptor returning(std::vector<SelectItem>& items,
        const ReturningOptions& options,const Range& target,std::vector<Namespace> scopes) {
        if (items.empty()) return {};
        const auto add = [&](const std::string& name,bool explicitAlias) {
            const bool exists = std::any_of(scopes.begin(),scopes.end(),[&](const auto& scope){
                return std::any_of(scope.begin(),scope.end(),[&](const auto& range){return range.name==name;});
            });
            if (exists) {
                if (explicitAlias) throw DbError("42712","table name \""+name+"\" specified more than once");
                return;
            }
            // RETURNING OLD/NEW names are table-only transition namespaces.
            // Unqualified columns and a bare star retain the target shape.
            Range transition{"",name,target.columns};
            for(const auto& column:transition.columns) transition.hiddenUnqualified.insert(column.name);
            scopes.front().push_back(registerSource(std::move(transition)));
        };
        if(options.oldAliased) add(identifier(options.oldAlias),true);
        if(options.newAliased) add(identifier(options.newAlias),true);
        if(!options.oldAliased) add("old",false);
        if(!options.newAliased) add("new",false);
        return project(items,scopes);
    }
    Range relation(const std::string& source, const std::string& alias, const Ctes& ctes) {
        CatalogManager::QualifiedName name;
        if (!CatalogManager::parseQualifiedName(source, name, true))
            throw DbError("42601", "invalid relation name");
        if (name.schema.empty()) {
            const auto cte = ctes.find(name.name);
            if (cte != ctes.end()) {
                if (cte->second.columns.empty())
                    throw DbError("0A000", "WITH query does not have a RETURNING clause: " + name.name);
                Range range{"", alias.empty() ? name.name : identifier(alias), cte->second.columns};
                range.cteStatement = cte->second.statement;
                return range;
            }
        }
        if (!metadata.relation) throw DbError("42P01", "relation does not exist: " + source);
        const auto description = metadata.relation(source);
        Range range{alias.empty() ? description.schema : "", alias.empty() ? description.name : identifier(alias), description.columns};
        range.relationSchema = description.schema;
        range.relationName = description.name;
        if (!description.viewSql.empty()) {
            // Preparing a view does not inherit the caller's PL datums or
            // SQL ranges. The retained child owns all of its AST identities.
            static thread_local size_t viewDepth = 0;
            if (++viewDepth > 64) {
                --viewDepth;
                throw DbError("54001", "view binding nesting limit exceeded");
            }
            struct ViewDepth { size_t& depth; ~ViewDepth() { --depth; } } guard{viewDepth};
            range.viewQuery = std::make_shared<PreparedQuery>(prepareQuery(description.viewSql, {}, metadata));
            // UNKNOWN strings/NULL finalize to text at the view relation
            // boundary, just as for a derived/CTE source. This is static
            // metadata; never execute a first row to discover the type.
            auto* viewSelect = dynamic_cast<SelectStmt*>(range.viewQuery->ast.get());
            for (size_t i = 0; i < range.viewQuery->output.size(); ++i) {
                auto& column = range.viewQuery->output[i];
                if (column.type != "unknown") continue;
                const auto castOutput = [](ExprPtr& expression) {
                    auto cast = std::make_unique<CastExpr>();
                    cast->typeName = "text"; cast->implicit = true;
                    cast->sourceBegin = expression->sourceBegin; cast->sourceEnd = expression->sourceEnd;
                    cast->operand = std::move(expression); expression = std::move(cast);
                };
                if (viewSelect && viewSelect->command == SqlCommand::Values) {
                    for (auto& row : viewSelect->valuesRows) castOutput(row.at(i));
                } else if (viewSelect && viewSelect->selectList.size() == range.viewQuery->output.size())
                    castOutput(viewSelect->selectList[i].expr);
                else throw DbError("0A000", "view UNKNOWN output requires additional projection lowering");
                column.type = "text";
            }
            range.viewQuery->statementOutputs[range.viewQuery->ast.get()] = range.viewQuery->output;
            if (range.columns.empty()) range.columns = range.viewQuery->output;
            if (range.columns.size() != range.viewQuery->output.size())
                throw DbError("XX000", "view output width differs from its catalog descriptor");
        }
        return range;
    }
    Namespace from(FromItem* item, const std::vector<Namespace>& outer, const Ctes& ctes) {
        if (!item) return {};
        const auto aliases=[&](Range range) {
            if(item->columnAliases.size()>range.columns.size())
                throw DbError("42P10","source alias has more column names than its relation");
            for(size_t i=0;i<item->columnAliases.size();++i)range.columns[i].name=identifier(item->columnAliases[i]);
            return registerSource(std::move(range),item);
        };
        if (item->type == FromItem::Type::Table)
            return {aliases(relation(item->tableName, item->alias, ctes))};
        if (item->type == FromItem::Type::Subquery) {
            if (!item->subquery) throw DbError("42601", "derived query is missing");
            auto derivedOuter = outer;
            // A non-LATERAL source cannot see this SQL level's siblings,
            // but its lexical level still counts for ancestor provenance.
            derivedOuter.insert(derivedOuter.begin(), Namespace{});
            auto columns = statement(*item->subquery, derivedOuter, ctes);
            for (auto& column : columns) if (column.type == "unknown") column.type = "text";
            return {aliases({"", item->alias.empty() ? "" : identifier(item->alias), std::move(columns)})};
        }
        if (item->type != FromItem::Type::Join) throw DbError("0A000", "source requires structured preparation");
        auto left = from(item->left.get(), outer, ctes);
        auto right = from(item->right.get(), outer, ctes);
        checkRangeConflicts(left, right);
        std::vector<std::string> keys;
        std::set<std::string> seenKeys;
        for (const auto& spelling : item->usingCols) {
            const auto key = identifier(spelling);
            if (!seenKeys.insert(key).second)
                throw DbError("42701", "column \"" + key + "\" appears more than once in USING clause");
            keys.push_back(key);
        }
        if (SQLParser::toLower(item->joinType).rfind("natural", 0) == 0) {
            for (const auto& a : left) for (const auto& x : a.columns)
                for (const auto& b : right) for (const auto& y : b.columns)
                    if (!a.hiddenUnqualified.count(x.name) && !b.hiddenUnqualified.count(y.name) &&
                        x.name == y.name && seenKeys.insert(x.name).second) keys.push_back(x.name);
        }
        Range merged;
        for (const auto& key : keys) {
            size_t leftCount = 0, rightCount = 0; std::string leftType,rightType;
            for (auto& range : left) for (const auto& col : range.columns)
                if (col.name == key && !range.hiddenUnqualified.count(key)) { ++leftCount; leftType = col.type; }
            for (auto& range : right) for (const auto& col : range.columns)
                if (col.name == key && !range.hiddenUnqualified.count(key)) { ++rightCount; rightType = col.type; }
            if (!leftCount || !rightCount) throw DbError("42703", "column \"" + key + "\" specified in USING does not exist");
            if (leftCount > 1 || rightCount > 1) throw DbError("42702", "USING column is ambiguous");
            const auto hide = [&](Range& range) {
                range.hiddenUnqualified.insert(key);
                const auto found = std::find_if(result.sourceRanges.begin(), result.sourceRanges.end(),
                    [&](const auto& source) { return source.ordinal == range.occurrence; });
                if (found == result.sourceRanges.end()) throw DbError("XX000", "JOIN lost its source occurrence");
                found->hiddenUnqualified = range.hiddenUnqualified;
            };
            for (auto& range : left) hide(range);
            for (auto& range : right) hide(range);
            merged.columns.push_back({key, selectCommonType({leftType,rightType},"JOIN/USING")});
        }
        left.insert(left.end(), right.begin(), right.end());
        auto scopes = outer; scopes.insert(scopes.begin(), left);
        expression(item->joinCondition, scopes);
        if (!merged.columns.empty()) {
            merged.mergedUsing = true;
            left.insert(left.begin(), registerSource(std::move(merged), item));
        }
        return left;
    }
    QueryRowDescriptor statement(Stmt& node, const std::vector<Namespace>& outer, Ctes ctes) {
        auto columns = statementImpl(node, outer, std::move(ctes));
        result.statementOutputs[&node] = columns;
        return columns;
    }
    QueryRowDescriptor statementImpl(Stmt& node, const std::vector<Namespace>& outer, Ctes ctes) {
        if (++depth > 128) throw DbError("54001", "query binding nesting limit exceeded");
        struct Depth { size_t& value; ~Depth() { --value; } } guard{depth};
        statementOwners.push_back(&node);
        struct OwnerScope { std::vector<const Stmt*>& owners; ~OwnerScope() { owners.pop_back(); } } ownerScope{statementOwners};
        cteScopes.push_back(&ctes);
        struct CteScope { std::vector<const Ctes*>& stack; ~CteScope() { stack.pop_back(); } } cteScope{cteScopes};
        const auto bindDefinitions = [&](std::vector<SelectStmt::CTE>& definitions) {
            // WITH definitions cannot see sibling FROM ranges of this SQL
            // level. Retain its empty lexical frame for ancestor provenance.
            auto cteOuter = outer;
            cteOuter.insert(cteOuter.begin(), Namespace{});
            for (auto& cte : definitions) {
                if (!cte.query) throw DbError("42601", "WITH query is missing");
                const std::string name = identifier(cte.name);
                QueryRowDescriptor columns;
                auto* recursive = dynamic_cast<SelectStmt*>(cte.query.get());
                if (cte.recursive && recursive && recursive->setOp != SetOp::None) {
                    // Seed only from the non-recursive term; never execute it.
                    StmtPtr tail = std::move(recursive->setOpRhs);
                    columns = statement(*recursive, cteOuter, ctes);
                    recursive->setOpRhs = std::move(tail);
                    applyNames(columns, cte.columnNames);
                    auto inner = ctes; inner[name] = {columns, cte.query.get()};
                    if (recursive->setOpRhs) {
                        auto rhs = statement(*recursive->setOpRhs, cteOuter, inner);
                        if (rhs.size() != columns.size()) throw DbError("42601", "recursive query column count mismatch");
                    }
                } else columns = statement(*cte.query, cteOuter, ctes);
                applyNames(columns, cte.columnNames);
                // A relation's unknown output is finalized as text. Only a
                // direct INSERT SELECT literal retains assignment context;
                // equal bytes from a derived/CTE TEXT cell cannot gain it.
                for (auto& column : columns) if (column.type == "unknown") column.type = "text";
                result.statementOutputs[cte.query.get()] = columns;
                ctes[name] = {std::move(columns), cte.query.get()};
            }
        };
        if (auto* envelope = dynamic_cast<WithStmt*>(&node)) {
            if (!envelope->statement) throw DbError("42601", "WITH primary statement is missing");
            bindDefinitions(envelope->ctes);
            return statement(*envelope->statement, outer, ctes);
        }
        if (auto* select = dynamic_cast<SelectStmt*>(&node)) {
            bindDefinitions(select->ctes);
            if (select->setOpLhs) {
                auto columns = statement(*select->setOpLhs, outer, ctes);
                if (select->setOpRhs && statement(*select->setOpRhs, outer, ctes).size() != columns.size())
                    throw DbError("42601", "set query column count mismatch");
                return columns;
            }
            auto scopes = outer;
            scopes.insert(scopes.begin(), from(select->fromClause.get(), outer, ctes));
            auto columns = project(select->selectList, scopes, &projectionLeaves[select]);
            if (select->command == SqlCommand::Values && !select->valuesRows.empty()) {
                columns.clear();
                const size_t width=select->valuesRows.front().size();
                std::vector<std::vector<std::string>> inputs;
                // VALUES transforms every row before choosing a column's
                // common type. A late routine/name error cannot be hidden by
                // premature conversion of an earlier UNKNOWN string.
                for(auto& row:select->valuesRows) {
                    std::vector<std::string> types;
                    for(auto& value:row)types.push_back(expression(value,scopes));
                    if(row.size()!=width)throw DbError("42601","VALUES lists must all be the same length");
                    inputs.push_back(std::move(types));
                }
                for(size_t column=0;column<width;++column) {
                    std::vector<std::string> types;
                    for(const auto& row:inputs)types.push_back(row[column]);
                    const auto common=selectCommonType(types,"VALUES");
                    columns.push_back({"column"+std::to_string(column+1),common});
                    for(size_t row=0;row<inputs.size();++row)
                        coerceCaseInput(select->valuesRows[row][column],inputs[row][column],common);
                }
            }
            expression(select->whereClause, scopes);
            for (auto& value : select->distinctOn) expression(value, scopes);
            for (auto& value : select->groupBy) expression(value, scopes);
            for (auto& group : select->groupByElems) for (auto& value : group.exprs) expression(value, scopes);
            expression(select->having, scopes);
            for (auto& value : select->windowDefs) window(value, scopes);
            for (auto& order : select->orderBy) {
                const auto* ref = dynamic_cast<ColumnRefExpr*>(order.expr.get());
                const bool outputAlias = ref && ref->table.empty() && ref->schema.empty() &&
                    std::any_of(columns.begin(), columns.end(), [&](const auto& col) { return col.name == ref->column; });
                if (!outputAlias) expression(order.expr, scopes);
            }
            if(select->command!=SqlCommand::Values)
                for (auto& row : select->valuesRows) for (auto& value : row) expression(value, scopes);
            if (select->setOpRhs && statement(*select->setOpRhs, outer, ctes).size() != columns.size())
                throw DbError("42601", "set query column count mismatch");
            return columns;
        }
        if (auto* insert = dynamic_cast<InsertStmt*>(&node)) {
            auto target = registerSource(relation(insert->tableName, "", {}));
            QueryRowDescriptor assignments;
            std::set<std::string> assignedNames;
            if (insert->columns.empty()) assignments = target.columns;
            else for (const auto& spelling : insert->columns) {
                const std::string name = identifier(spelling);
                if (!assignedNames.insert(name).second)
                    throw DbError("42701", "INSERT column specified more than once: " + name);
                const auto found = std::find_if(target.columns.begin(), target.columns.end(),
                    [&](const auto& column) { return column.name == name; });
                if (found == target.columns.end()) throw DbError("42703", "INSERT target column does not exist: " + name);
                assignments.push_back(*found);
            }
            const auto validateWidth = [&](size_t width) {
                if (width > assignments.size() || (!insert->columns.empty() && width != assignments.size()))
                    throw DbError("42601", "INSERT target column count does not match expressions");
            };
            for (auto& row : insert->values) {
                std::vector<std::string> types;
                for (auto& value : row) {
                    const auto* literal = dynamic_cast<const LiteralExpr*>(value.get());
                    const bool defaultValue = literal && !literal->preparedSubquery && literal->typeName.empty() &&
                        SQLParser::toLower(literal->value) == "default";
                    types.push_back(defaultValue ? "default" : expression(value, outer));
                }
                // Expression transformation (unknown names and typed input
                // literals) precedes target-width validation. Contextual
                // conversion of a bare unknown string follows that check.
                validateWidth(row.size());
                if (row.size() != insert->values.front().size())
                    throw DbError("42601", "VALUES rows have different widths");
                // Bind all siblings before contextual input conversion,
                // then process the next row. A later row cannot supersede
                // this row's malformed constant input or cause side effects.
                for (size_t i = 0; i < row.size(); ++i)
                    if (types[i] != "default") {
                            if (assignments[i].generated || (assignments[i].identity == 'a' && insert->override_.empty()))
                                throw DbError("428C9", "cannot insert a non-DEFAULT value into column " + assignments[i].name);
                            if (metadata.assignmentInput) metadata.assignmentInput(assignments[i],row[i].get(),types[i]);
                    }
            }
            if (insert->selectSource) {
                const auto inputs = statement(*insert->selectSource, outer, ctes);
                validateWidth(inputs.size());
                const auto* direct = dynamic_cast<const SelectStmt*>(insert->selectSource.get());
                const auto leaves = projectionLeaves.find(direct);
                const bool directValues = direct && direct->setOp == SetOp::None && !direct->setOpLhs &&
                    leaves != projectionLeaves.end() && leaves->second.size() == inputs.size();
                for (size_t i = 0; i < inputs.size(); ++i)
                    if (assignments[i].generated || (assignments[i].identity == 'a' && insert->override_.empty()))
                        throw DbError("428C9", "cannot insert a non-DEFAULT value into column " + assignments[i].name);
                if (metadata.assignmentInput)
                    for (size_t i = 0; i < inputs.size(); ++i) {
                        const Expr* leaf = directValues ? leaves->second[i] : nullptr;
                        metadata.assignmentInput(assignments[i],leaf,inputs[i].type);
                    }
            }
            auto scopes = outer; scopes.insert(scopes.begin(), {target});
            auto conflictScopes = scopes;
            if (SQLParser::toLower(insert->conflictAction) == "do update") {
                // EXCLUDED is a logical transition row with the complete
                // target descriptor, not another physical table. Its columns
                // participate in value namespaces (including unqualified
                // ambiguity), but the row is not visible in RETURNING.
                conflictScopes.front().push_back(registerSource({"","excluded",target.columns}));
            }
            for (auto& value : insert->conflictUpdateSet) expression(value.second, conflictScopes);
            expression(insert->conflictWhere, conflictScopes);
            return returning(insert->returning,insert->returningOptions,target,std::move(scopes));
        }
        if (auto* update = dynamic_cast<UpdateStmt*>(&node)) {
            auto ranges = from(update->fromClause.get(), outer, ctes);
            const auto target = registerSource(relation(update->tableName, update->alias, {}));
            checkRangeConflicts({target}, ranges);
            ranges.insert(ranges.begin(), target);
            auto scopes = outer; scopes.insert(scopes.begin(), std::move(ranges));
            for (auto& value : update->setClauses) {
                const auto name = identifier(value.first);
                const auto& target = scopes.front().front().columns;
                if (std::none_of(target.begin(), target.end(), [&](const auto& column) { return column.name == name; }))
                    throw DbError("42703", "UPDATE target column does not exist: " + name);
                expression(value.second, scopes);
            }
            expression(update->whereClause, scopes);
            return returning(update->returning,update->returningOptions,scopes.front().front(),scopes);
        }
        if (auto* remove = dynamic_cast<DeleteStmt*>(&node)) {
            auto ranges = from(remove->usingClause.get(), outer, ctes);
            const auto target = registerSource(relation(remove->tableName, remove->alias, {}));
            checkRangeConflicts({target}, ranges);
            ranges.insert(ranges.begin(), target);
            auto scopes = outer; scopes.insert(scopes.begin(), std::move(ranges));
            expression(remove->whereClause, scopes);
            return returning(remove->returning,remove->returningOptions,scopes.front().front(),scopes);
        }
        if (auto* explain = dynamic_cast<ExplainStmt*>(&node)) {
            if (!explain->query) throw DbError("42601", "EXPLAIN query is missing");
            statement(*explain->query, outer, ctes); return {{"QUERY PLAN","text"}};
        }
        if (auto* create = dynamic_cast<CreateTableStmt*>(&node)) {
            if (create->preparedAsQuery) return statement(*create->preparedAsQuery, outer, ctes);
            for (auto& column : create->columns) expression(column.defaultValue, outer);
            return {};
        }
        throw DbError("0A000", "statement requires structured query preparation");
    }
    static void applyNames(QueryRowDescriptor& columns, const std::vector<std::string>& names) {
        if (names.size() > columns.size()) throw DbError("42P10", "query has fewer columns than column names");
        for (size_t i = 0; i < names.size(); ++i) columns[i].name = identifier(names[i]);
    }
    // Visit a separately parsed scalar child before attaching it to its
    // parent's original source coordinates. Used solely for source rebasing.
    static void rebase(Stmt& node, size_t offset);
};
} // namespace

void Binder::rebase(Stmt& root, size_t offset) {
    std::function<void(ExprPtr&)> expr;
    std::function<void(Stmt&)> stmt;
    std::function<void(FromItem*)> from;
    const auto window = [&](WindowDef& def) {
        for (auto& value : def.partitionBy) expr(value);
        for (auto& value : def.orderBy) expr(value.first);
        expr(def.frameStart); expr(def.frameEnd);
    };
    const auto items = [&](std::vector<SelectItem>& list) { for (auto& item : list) {
        expr(item.expr); if (item.sourceExpressionEnd != std::string::npos) item.sourceExpressionEnd += offset;
    } };
    expr = [&](ExprPtr& value) {
        if (!value) return;
        if (value->sourceBegin != std::string::npos) value->sourceBegin += offset;
        if (value->sourceEnd != std::string::npos) value->sourceEnd += offset;
        if (auto* e = dynamic_cast<UnaryOpExpr*>(value.get())) expr(e->operand);
        else if (auto* e = dynamic_cast<BinaryOpExpr*>(value.get())) { expr(e->left); expr(e->right); }
        else if (auto* e = dynamic_cast<QuantifiedComparisonExpr*>(value.get())) { expr(e->left); expr(e->right); }
        else if (auto* e = dynamic_cast<CastExpr*>(value.get())) expr(e->operand);
        else if (auto* e = dynamic_cast<FunctionCallExpr*>(value.get())) {
            for (auto& arg : e->args) expr(arg);
            for (auto& arg : e->namedArgs) expr(arg.value);
            expr(e->filter); window(e->over);
        } else if (auto* e = dynamic_cast<CaseExpr*>(value.get())) {
            expr(e->switchExpr); expr(e->elseExpr);
            for (auto& arm : e->whenClauses) { expr(arm.first); expr(arm.second); }
        } else if (auto* e = dynamic_cast<ArrayExpr*>(value.get())) for (auto& element : e->elements) expr(element);
        else if (auto* e = dynamic_cast<RowExpr*>(value.get())) for (auto& element : e->elements) expr(element);
    };
    from = [&](FromItem* source) {
        if (!source) return;
        from(source->left.get()); from(source->right.get()); expr(source->joinCondition);
        if (source->subquery) stmt(*source->subquery);
    };
    stmt = [&](Stmt& statement) {
        const auto definitions = [&](std::vector<SelectStmt::CTE>& list) {
            for (auto& cte : list) {
                if (cte.queryBegin != std::string::npos) cte.queryBegin += offset;
                if (cte.queryEnd != std::string::npos) cte.queryEnd += offset;
                if (cte.query) stmt(*cte.query);
            }
        };
        if (auto* envelope = dynamic_cast<WithStmt*>(&statement)) {
            definitions(envelope->ctes);
            if (envelope->statementBegin != std::string::npos) envelope->statementBegin += offset;
            if (envelope->statementEnd != std::string::npos) envelope->statementEnd += offset;
            if (envelope->statement) stmt(*envelope->statement);
        } else if (auto* select = dynamic_cast<SelectStmt*>(&statement)) {
            definitions(select->ctes);
            if (select->setOpLhs) stmt(*select->setOpLhs);
            if (select->setOpRhs) stmt(*select->setOpRhs);
            items(select->selectList); from(select->fromClause.get()); expr(select->whereClause);
            for (auto& value : select->groupBy) expr(value);
            for (auto& group : select->groupByElems) for (auto& value : group.exprs) expr(value);
            for (auto& value : select->distinctOn) expr(value);
            expr(select->having); for (auto& order : select->orderBy) expr(order.expr);
            for (auto& row : select->valuesRows) for (auto& value : row) expr(value);
            for (auto& def : select->windowDefs) window(def);
        } else if (auto* insert = dynamic_cast<InsertStmt*>(&statement)) {
            for (auto& row : insert->values) for (auto& value : row) expr(value);
            if (insert->selectSource) stmt(*insert->selectSource);
            for (auto& value : insert->conflictUpdateSet) expr(value.second);
            expr(insert->conflictWhere); items(insert->returning);
        } else if (auto* update = dynamic_cast<UpdateStmt*>(&statement)) {
            for (auto& value : update->setClauses) expr(value.second);
            from(update->fromClause.get()); expr(update->whereClause); items(update->returning);
        } else if (auto* remove = dynamic_cast<DeleteStmt*>(&statement)) {
            from(remove->usingClause.get()); expr(remove->whereClause); items(remove->returning);
        } else if (auto* explain = dynamic_cast<ExplainStmt*>(&statement)) {
            if (explain->query) stmt(*explain->query);
        } else if (auto* create = dynamic_cast<CreateTableStmt*>(&statement)) {
            if (create->preparedAsQuery) stmt(*create->preparedAsQuery);
            for (auto& column : create->columns) expr(column.defaultValue);
        }
    };
    stmt(root);
}

PreparedQuery prepareQuery(const std::string& sql, const std::vector<QueryBindingDatum>& datums,
                          const QueryBindingMetadata& metadata) {
    Binder binder(sql, datums, metadata);
    SQLParser parser; auto parsed = parser.parseForBinding(sql);
    if (!parsed.isValid()) throw DbError("42601", parsed.error);
    binder.result.output = binder.statement(*parsed.stmt, {}, {});
    binder.result.ast = std::move(parsed.stmt);
    return std::move(binder.result);
}

std::string PreparedQuery::legacySql() const {
    auto positions = uses;
    for (size_t i = 0; i < projectionAliases.size(); ++i)
        positions.push_back({projectionAliases[i].first, projectionAliases[i].first, parameters.size() + i});
    std::sort(positions.begin(), positions.end(), [](const Use& a, const Use& b) { return a.begin > b.begin; });
    std::string sql = source;
    size_t previous = source.size();
    for (const auto& use : positions) {
        if (use.begin == use.end && use.slot >= parameters.size()) {
            if (use.begin > previous) throw DbError("XX000", "invalid projection provenance");
            sql.insert(use.begin, " AS " + projectionAliases[use.slot - parameters.size()].second);
            previous = use.begin; continue;
        }
        if (use.end > previous || use.begin >= use.end || use.slot >= parameters.size())
            throw DbError("XX000", "invalid prepared parameter provenance");
        const auto& cell = parameters[use.slot];
        std::string value = "NULL";
        if (!cell.isNull) {
            value = "'";
            for (char c : cell.value) { value += c; if (c == '\'') value += c; }
            value += "'";
        }
        if (cell.typeName.empty()) throw DbError("42P18", "prepared parameter has no declared type");
        sql.replace(use.begin, use.end - use.begin, "CAST(" + value + " AS " + cell.typeName + ")");
        previous = use.begin;
    }
    return sql;
}
} // namespace dbms
