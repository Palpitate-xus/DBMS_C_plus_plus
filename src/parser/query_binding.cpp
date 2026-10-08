#include "query_binding.h"
#include "common/GeometryValue.h"
#include "expression/unary_type.h"
#include "parser/parser.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include "expression/common_type.h"
#include "expression/equality_type.h"
#include "expression/expr_helper.h"
#include "expression/array_type.h"
#include "expression/geometric_input.h"
#include "expression/between_input.h"
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
    std::map<const Expr*,uint32_t> valueTypeOids;
    const QueryRowDescriptor* setOrderScope = nullptr;
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

    bool rangeMatches(const ColumnRefExpr& reference, const Range& range) const {
        if (!reference.table.empty() && reference.table != range.name) return false;
        if (reference.schema.empty() || reference.schema == range.schema) return true;
        // The copied catalog descriptor owns the canonical physical identity.
        // A requested namespace alias (e.g. pg_temp) may denote that same
        // relation, but an SQL range alias deliberately hides its schema.
        if (range.schema.empty() || range.relationName.empty() ||
            range.name != range.relationName || reference.table.empty() || !metadata.relation)
            return false;
        const auto quoted = [](const std::string& name) {
            std::string result = "\"";
            for (const char character : name) {
                result += character;
                if (character == '"') result += character;
            }
            return result + '"';
        };
        const auto relation = metadata.relation(quoted(reference.schema) + "." + quoted(reference.table));
        return relation.schema == range.relationSchema && relation.name == range.relationName;
    }

    std::string parameter(ExprPtr& node, const QueryBindingDatum& datum) {
        if (node->sourceBegin == std::string::npos || node->sourceEnd > result.source.size())
            throw DbError("XX000", "parameter reference has no source provenance");
        auto [position, inserted] = slots.emplace(datum.identity, result.parameters.size());
        if (inserted) result.parameters.emplace_back(datum.type,
            datum.value.value_or(""), !datum.value.has_value());
        auto bound = std::make_unique<ParameterExpr>();
        bound->slot = position->second; bound->declaredType = datum.type;
        bound->origin = datum.origin;
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
    static bool patternOperator(const std::string& op) {
        return op=="LIKE" || op=="NOT LIKE" || op=="ILIKE" || op=="NOT ILIKE" ||
               op=="SIMILAR TO" || op=="NOT SIMILAR TO";
    }
    uint32_t expressionTypeOid(const Expr* node) const {
        const auto resolved=valueTypeOids.find(node);
        if(resolved!=valueTypeOids.end())return resolved->second;
        const auto* column=dynamic_cast<const ColumnRefExpr*>(node);
        if(column && column->binding)return column->binding->typeOid;
        const auto* unary=dynamic_cast<const UnaryOpExpr*>(node);
        return unary && unary->op.rfind("COLLATE ",0)==0
            ? expressionTypeOid(unary->operand.get()) : 0;
    }
    std::optional<QueryEnumType> enumType(const std::string& spelling,const Expr* node) const {
        return metadata.enumType?metadata.enumType(spelling,expressionTypeOid(node)):std::nullopt;
    }
    static void validateEnumLiteral(const Expr* input,const QueryEnumType& type) {
        const auto* literal=dynamic_cast<const LiteralExpr*>(input);
        if(!literal || literal->preparedSubquery || !literal->typeName.empty())return;
        // Only the parser's genuine UNKNOWN literal is transformed. Never
        // evaluate a function, parameter, subquery or row to discover a type.
        if(ExprHelper::inferValuesResultType(literal->value)!="unknown")return;
        ExprEvaluator pure;
        const auto value=pure.eval(literal,RowContext{});
        if(!value.isNull && std::find(type.labels.begin(),type.labels.end(),value.value)==type.labels.end())
            throw DbError("22P02","invalid input value for enum "+type.typeName+": "+value.value);
    }
    void bindEnumCast(const Expr* expression,const Expr* input,const std::string& spelling) {
        if(const auto type=enumType(spelling,nullptr)) {
            validateEnumLiteral(input,*type);
            valueTypeOids[expression]=type->typeOid;
        }
    }
    std::optional<QueryComparisonBinding> enumComparison(const std::string& rawOp,
        ExprPtr& left,ExprPtr& right,const std::string& leftType,const std::string& rightType) {
        const auto lhs=enumType(leftType,left.get()),rhs=enumType(rightType,right.get());
        if(!lhs && !rhs)return std::nullopt;
        const auto& type=lhs?*lhs:*rhs;
        if((lhs && rhs && lhs->typeOid!=rhs->typeOid) ||
           (!lhs && common_type_detail::canonical(leftType)!="unknown") ||
           (!rhs && common_type_detail::canonical(rightType)!="unknown"))
            throw DbError("42883","operator does not exist: "+leftType+" "+rawOp+" "+rightType);
        if(!lhs)validateEnumLiteral(left.get(),type);
        if(!rhs)validateEnumLiteral(right.get(),type);
        if(!lhs)coerceCaseInput(left,leftType,type.typeName);
        if(!rhs)coerceCaseInput(right,rightType,type.typeName);
        QueryComparisonBinding binding;
        binding.op=(rawOp=="IS DISTINCT FROM" || rawOp=="IS NOT DISTINCT FROM")?"=":rawOp=="!="?"<>":rawOp;
        binding.leftType=type.typeName;binding.rightType=type.typeName;
        binding.strict=true;binding.enumTypeOid=type.typeOid;binding.enumLabels=type.labels;
        binding.identity=type.identity+":"+binding.op;
        return binding;
    }
    std::optional<QueryComparisonBinding> enumSortComparison(const std::string& spelling,uint32_t typeOid) const {
        const auto type=metadata.enumType?metadata.enumType(spelling,typeOid):std::nullopt;
        if(!type)return std::nullopt;
        QueryComparisonBinding binding;
        binding.op="<";binding.leftType=type->typeName;binding.rightType=type->typeName;
        binding.strict=true;binding.enumTypeOid=type->typeOid;binding.enumLabels=type->labels;
        binding.identity=type->identity+":<";
        return binding;
    }
    std::string patternBaseType(const std::string& declared,const Expr* node) const {
        return ExprHelper::canonicalResultTypeName(metadata.baseType
            ? metadata.baseType(declared,expressionTypeOid(node)) : declared);
    }
    void resolvePattern(const std::string& op,ExprPtr& left,ExprPtr& right,
                        const std::string& leftType,const std::string& rightType) {
        const auto lhs=patternBaseType(leftType,left.get());
        const auto rhs=patternBaseType(rightType,right.get());
        const bool binary=(op=="LIKE" || op=="NOT LIKE") && (lhs=="bytea" || rhs=="bytea");
        const auto textual=[](const std::string& type) {
            return type=="unknown" || type=="text" || type=="character varying" ||
                   type=="character" || type=="name";
        };
        if(binary ? ((lhs!="bytea" && lhs!="unknown") || (rhs!="bytea" && rhs!="unknown"))
                  : (!textual(lhs) || !textual(rhs)))
            throw DbError("42883","operator does not exist: "+leftType+" "+op+" "+rightType);
        const std::string target=binary?"bytea":"text";
        if(lhs=="unknown" || binary)coerceCaseInput(left,leftType,target);
        coerceCaseInput(right,rightType,target);
    }
    ExprPtr targetDefault(const Range& range, const QueryOutputColumn& column) {
        if (!metadata.updateDefault) return {};
        const auto definition=metadata.updateDefault(range.relationSchema,range.relationName,column.name);
        if (!definition) return {};
        // Parse the genuine stored definition once, in an independent empty
        // value namespace. SELECT is only the parser's expression envelope;
        // no caller CTE/target/PL variable or query provider is inherited.
        SQLParser parser;
        auto parsed=parser.parseForBinding("SELECT "+*definition);
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        if (!select || select->selectList.size()!=1 || !select->selectList.front().expr ||
            !select->selectList.front().alias.empty() || select->fromClause || select->whereClause ||
            select->having || !select->groupBy.empty() || !select->orderBy.empty() ||
            !select->ctes.empty() || select->setOpRhs || select->limit || select->offset || select->signedFetchCount)
            throw DbError("XX001","invalid stored target default expression");
        auto value=std::move(select->selectList.front().expr);
        const std::vector<QueryBindingDatum> noDatums;
        Binder defaultBinder("",noDatums,metadata);
        const auto type=defaultBinder.expression(value,{});
        if (!defaultBinder.result.sourceRanges.empty() || !defaultBinder.result.parameters.empty())
            throw DbError("0A000","stored target default cannot contain a query or parameter");
        const auto targetType=ExprHelper::canonicalResultTypeName(column.type);
        defaultBinder.coerceCaseInput(value,type,targetType);
        return value;
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
            if (setOrderScope) {
                if (!column->table.empty() || !column->schema.empty())
                    throw DbError("42P01", "set ORDER BY has no branch FROM namespace");
                const QueryOutputColumn* found = nullptr;
                for (const auto& output : *setOrderScope) if (output.name == column->column) {
                    if (found) throw DbError("42702", "ORDER BY name is ambiguous");
                    found = &output;
                }
                if (!found) throw DbError("42703", "ORDER BY column does not exist: " + column->column);
                return found->type;
            }
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
                    if (!rangeMatches(*column, range)) continue;
                    if (!column->table.empty()) rangeFound = true;
                    for (size_t ordinal = 0; ordinal < range.columns.size(); ++ordinal) {
                        const auto& field = range.columns[ordinal];
                        if (field.name != column->column) continue;
                        if (column->table.empty() && range.hiddenUnqualified.count(field.name)) continue;
                        ++matches; sourceType = field.type;
                        resolved = {scopeDepth, range.occurrence, ordinal, field.type, range.mergedUsing, field.typeOid};
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
                if (isGeometryTypeName(literal->typeName)) {
                    GeometryValue value;
                    if (!parseGeometryValue(literal->value, literal->typeName, value))
                        throw DbError("22P02", "invalid input syntax for type " + literal->typeName);
                }
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
            geometric_input_detail::validateUnknownInput(cast->operand.get(),cast->typeName);
            if (metadata.assignmentInput) metadata.assignmentInput({"", cast->typeName}, cast, cast->typeName);
            bindEnumCast(cast,cast->operand.get(),cast->typeName);
            if(const auto geometry=geometric_input_detail::builtinType(cast->typeName);!geometry.empty())
                return geometry;
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
            if(op=="+" || op=="-") {
                const auto target=resolveBuiltinUnary(op,type);
                coerceCaseInput(unary->operand,type,target);
                return target;
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
                geometric_input_detail::validateUnknownInput(binary->left.get(),type->value);
                if (metadata.assignmentInput) metadata.assignmentInput({"", type->value}, binary, type->value);
                bindEnumCast(binary,binary->left.get(),type->value);
                if(const auto geometry=geometric_input_detail::builtinType(type->value);!geometry.empty())
                    return geometry;
                return type->value; // grammar type, not a SQL value namespace
            }
            // Slice bounds are this parser's retained grammar envelope, not
            // an opaque SQL value/name expression (analogous to ::'s label).
            const auto right = binary->op=="[:]"?std::string("unknown"):expression(binary->right, scopes);
            if(binary->op=="[]" || binary->op=="[:]") {
                const Expr* receiver=binary->left.get();
                if(binary->op=="[]")while(const auto* item=dynamic_cast<const BinaryOpExpr*>(receiver)) {
                    if(item->op!="[]")break;
                    receiver=item->left.get();
                }
                const auto type=ExprHelper::inferParsedResultType(receiver,{});
                const auto element=arrayElement(type);
                if(element.empty())throw DbError("42804","cannot subscript a non-array value");
                if(binary->op=="[]")coerceCaseInput(binary->right,right,"integer");
                return binary->op=="[]"?element:type;
            }
            if(patternOperator(binary->op)) {
                resolvePattern(binary->op,binary->left,binary->right,left,right);
                return "boolean";
            }
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
                "AND", "OR", "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE",
                "SIMILAR TO", "NOT SIMILAR TO", "IN", "NOT IN", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"};
            static const std::set<std::string> comparisons={"=","<>","!=","<",">","<=",">=","IS DISTINCT FROM","IS NOT DISTINCT FROM"};
            if(comparisons.count(binary->op)) {
                binary->comparison=enumComparison(binary->op,binary->left,binary->right,left,right);
                // Integer input functions transform genuine UNKNOWN SQL
                // literals during binding, even if no source row is demanded.
                // Parameters, routines, typed casts and query children are
                // not constants and must remain runtime expression sites.
                const auto integer=[](const std::string& type) {
                    const auto canonical=common_type_detail::canonical(type);
                    return canonical=="smallint" || canonical=="integer" || canonical=="bigint";
                };
                const auto unknownLiteral=[](const Expr* input) {
                    const auto* literal=dynamic_cast<const LiteralExpr*>(input);
                    return literal && !literal->preparedSubquery && literal->typeName.empty() &&
                        ExprHelper::inferValuesResultType(literal->value)=="unknown";
                };
                const auto integerInput=[&](ExprPtr& input,const std::string& target) {
                    auto* literal=static_cast<LiteralExpr*>(input.get());
                    if(metadata.assignmentInput)metadata.assignmentInput({"",target},literal,"unknown");
                    CastExpr conversion;conversion.typeName=target;conversion.implicit=true;
                    conversion.operand=std::make_unique<LiteralExpr>(*literal);
                    ExprEvaluator pure;const auto value=pure.eval(&conversion,RowContext{});
                    // PostgreSQL transforms UNKNOWN into a typed Const, not
                    // a runtime CAST. Retain this literal's original source
                    // coordinates and keep the established physical/index
                    // receiver instead of requesting a different query graph.
                    literal->typeName=target;
                    if(!value.isNull)literal->value=value.value;
                };
                if(!binary->comparison && integer(left) && right=="unknown" && unknownLiteral(binary->right.get()))
                    integerInput(binary->right,common_type_detail::canonical(left));
                else if(!binary->comparison && integer(right) && left=="unknown" && unknownLiteral(binary->left.get()))
                    integerInput(binary->left,common_type_detail::canonical(right));
            }
            const auto leftBase=ExprHelper::canonicalResultTypeName(left);
            const auto rightBase=ExprHelper::canonicalResultTypeName(right);
            const bool bitOperand=leftBase=="bit" || leftBase=="bit varying" ||
                                  rightBase=="bit" || rightBase=="bit varying";
            static const std::set<std::string> bitComparisons={"=","<>","!=","<",">","<=",">="};
            if(bitOperand && bitComparisons.count(binary->op)) {
                const auto comparison=ExprEvaluator::resolveComparison(binary->op,left,right);
                coerceCaseInput(binary->left,left,comparison.leftType);
                coerceCaseInput(binary->right,right,comparison.rightType);
            }
            const auto operation=SQLParser::toLower(binary->op);
            if((operation=="in" || operation=="not in") && dynamic_cast<RowExpr*>(binary->right.get())) {
                auto* list=static_cast<RowExpr*>(binary->right.get());
                std::string listLeftType=leftBase;
                std::string bitMemberType;
                bool mixedKnownTypes=false;
                for(const auto& member:list->elements) {
                    const auto memberType=ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedInputType(member.get(),{}));
                    if(memberType=="bit" || memberType=="bit varying")bitMemberType=memberType;
                    else if(memberType!="unknown")mixedKnownTypes=true;
                }
                if(listLeftType=="unknown" && !bitMemberType.empty() && mixedKnownTypes &&
                   dynamic_cast<ParameterExpr*>(binary->left.get())) {
                    for(const auto& member:list->elements) {
                        const auto memberType=ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedInputType(member.get(),{}));
                        if(memberType=="unknown")continue;
                        const auto target=ExprEvaluator::resolveComparison("=","unknown",memberType).leftType;
                        if(target!="bit" && target!="bit varying")
                            throw DbError("42P08","inconsistent types deduced for parameter");
                    }
                }
                if(listLeftType=="unknown" && !bitMemberType.empty() && !mixedKnownTypes) {
                    const auto comparison=ExprEvaluator::resolveComparison("=",listLeftType,bitMemberType);
                    coerceCaseInput(binary->left,listLeftType,comparison.leftType);
                    listLeftType=comparison.leftType;
                }
                for(auto& member:list->elements) {
                    const auto memberType=ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedInputType(member.get(),{}));
                    if(!(listLeftType=="unknown" && !bitMemberType.empty()) &&
                       listLeftType!="bit" && listLeftType!="bit varying" &&
                       memberType!="bit" && memberType!="bit varying")continue;
                    const auto comparison=ExprEvaluator::resolveComparison("=",listLeftType,memberType);
                    if(listLeftType=="unknown") {
                        if(const auto* literal=dynamic_cast<const LiteralExpr*>(binary->left.get());
                           literal && literal->typeName.empty() && !literal->preparedSubquery) {
                            CastExpr conversion;conversion.typeName=comparison.leftType;conversion.implicit=true;
                            conversion.operand=std::make_unique<LiteralExpr>(*literal);
                            ExprEvaluator pure;(void)pure.eval(&conversion,RowContext{});
                        }
                    }
                    coerceCaseInput(member,memberType,comparison.rightType);
                }
            }
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
            const auto grammarOperation = SQLParser::toLower(call->funcName);
            if (call->schema.empty() && call->args.size() == 3 &&
                (grammarOperation == "between" || grammarOperation == "not between")) {
                for (const auto& argument : call->args)
                    between_input_detail::validateLiteralCasts(argument.get());
                std::vector<std::string> types;
                for (auto& arg : call->args)
                    types.push_back(ExprHelper::canonicalResultTypeName(expression(arg, scopes)));
                const bool bitRange = std::any_of(types.begin(), types.end(), [](const auto& type) {
                    return type == "bit" || type == "bit varying";
                });
                const bool integerRange = std::any_of(types.begin(), types.end(), [](const auto& type) {
                    return type == "smallint" || type == "integer" || type == "bigint";
                });
                for (size_t i = 1; i < types.size(); ++i) {
                    if (!bitRange && !integerRange) continue;
                    const auto comparison = ExprEvaluator::resolveComparison(
                        i == 1 ? ">=" : "<=", types[0], types[i]);
                    if (types[0] == "unknown") {
                        if (dynamic_cast<ParameterExpr*>(call->args[0].get())) {
                            // A shared parameter is inferred by the first comparison,
                            // then the second comparison sees that resolved input.
                            coerceCaseInput(call->args[0], types[0], comparison.leftType);
                            types[0] = comparison.leftType;
                        } else if (const auto* literal = dynamic_cast<const LiteralExpr*>(call->args[0].get());
                                   literal && literal->typeName.empty() && !literal->preparedSubquery) {
                            // BETWEEN repeats a literal in two independently typed
                            // comparisons. Validate each input without assigning one
                            // comparison's type to the other occurrence.
                            ExprPtr input = std::make_unique<LiteralExpr>(*literal);
                            coerceCaseInput(input, "unknown", comparison.leftType);
                        }
                    }
                    coerceCaseInput(call->args[i], types[i], comparison.rightType);
                }
                call->resolvedResultType = "boolean";
                return "boolean";
            }
            const std::string escapeSuffix=" ESCAPE";
            if(call->schema.empty() && call->funcName.size()>escapeSuffix.size() &&
               call->funcName.compare(call->funcName.size()-escapeSuffix.size(),escapeSuffix.size(),escapeSuffix)==0 &&
               patternOperator(call->funcName.substr(0,call->funcName.size()-escapeSuffix.size()))) {
                if(call->args.size()!=3 || !call->namedArgs.empty() || call->hasOver || call->filter || call->distinct)
                    throw DbError("42601","invalid pattern ESCAPE expression");
                const auto lhs=expression(call->args[0],scopes);
                const auto rhs=expression(call->args[1],scopes);
                const auto escape=expression(call->args[2],scopes);
                const auto op=call->funcName.substr(0,call->funcName.size()-escapeSuffix.size());
                resolvePattern(op,call->args[0],call->args[1],lhs,rhs);
                const auto target=patternBaseType(rhs,call->args[1].get())=="bytea" ||
                    patternBaseType(lhs,call->args[0].get())=="bytea" ? "bytea" : "text";
                const auto escapeBase=patternBaseType(escape,call->args[2].get());
                const bool textEscape=escapeBase=="text" || escapeBase=="character varying" ||
                    escapeBase=="character" || escapeBase=="name";
                if(escapeBase!="unknown" && (target=="bytea" ? escapeBase!="bytea" : !textEscape))
                    throw DbError("42883","pattern ESCAPE input has no matching operator: "+escape);
                coerceCaseInput(call->args[2],escape,target);
                call->resolvedResultType="boolean";
                return "boolean";
            }
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
                call->resolvedResultType=call->setReturning->elementType;
                return call->resolvedResultType;
            }
            const auto type = metadata.functionType ? metadata.functionType(call) : std::string();
            call->resolvedResultType=type.empty() ? first : type;
            return call->resolvedResultType;
        }
        case ExprType::CaseExpr: {
            auto* conditional = static_cast<CaseExpr*>(node.get());
            const auto switchType = expression(conditional->switchExpr, scopes);
            if (conditional->switchExpr && switchType == "unknown")
                coerceCaseInput(conditional->switchExpr,switchType,"text");
            conditional->simpleComparisonTypes.clear();
            conditional->simpleEnumComparisons.clear();
            std::vector<std::string> thenTypes;
            for (auto& clause : conditional->whenClauses) {
                const auto conditionType = expression(clause.first, scopes);
                if (!conditional->switchExpr) {
                    if (common_type_detail::canonical(conditionType) != "unknown" &&
                        common_type_detail::canonical(conditionType) != "boolean")
                        throw DbError("42804","argument of CASE/WHEN must be type boolean");
                    coerceCaseInput(clause.first,conditionType,"boolean");
                } else {
                    const auto bound=enumComparison("=",conditional->switchExpr,clause.first,
                        switchType=="unknown"?"text":switchType,conditionType);
                    const auto equality=bound?std::make_pair(bound->leftType,bound->rightType):resolveBuiltinEquality(
                        switchType=="unknown"?"text":switchType,conditionType);
                    conditional->simpleComparisonTypes.push_back(equality);
                    if(bound)conditional->simpleEnumComparisons.push_back(bound);
                    else coerceCaseInput(clause.first,conditionType,equality.second);
                }
                thenTypes.push_back(expression(clause.second,scopes));
            }
            // Transform source WHEN/THEN clauses before ELSE; choose the
            // common result type and perform input conversions ELSE first.
            const auto elseType = expression(conditional->elseExpr,scopes);
            std::vector<std::string> resultTypes{elseType};
            resultTypes.insert(resultTypes.end(),thenTypes.begin(),thenTypes.end());
            std::optional<QueryEnumType> resultEnum;
            std::vector<bool> enumInputs(resultTypes.size(),false);
            const auto collectEnum=[&](const Expr* value,const std::string& spelling,size_t position) {
                if(const auto candidate=enumType(spelling,value)) {
                    if(resultEnum && resultEnum->typeOid!=candidate->typeOid)
                        throw DbError("42846","CASE cannot convert between distinct enum types");
                    resultEnum=candidate;
                    enumInputs[position]=true;
                }
            };
            collectEnum(conditional->elseExpr.get(),elseType,0);
            for(size_t i=0;i<thenTypes.size();++i)collectEnum(conditional->whenClauses[i].second.get(),thenTypes[i],i+1);
            if(resultEnum)for(size_t i=0;i<resultTypes.size();++i)
                if(enumInputs[i])resultTypes[i]=resultEnum->typeName;
            const auto type = selectCommonType(resultTypes,"CASE");
            if(resultEnum) {
                if(elseType=="unknown")validateEnumLiteral(conditional->elseExpr.get(),*resultEnum);
                for(size_t i=0;i<thenTypes.size();++i)if(thenTypes[i]=="unknown")
                    validateEnumLiteral(conditional->whenClauses[i].second.get(),*resultEnum);
                valueTypeOids[conditional]=resultEnum->typeOid;
            }
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
        if(scalar)valueTypeOids[&literal]=columns.front().typeOid;
        return literal.typeName;
    }
    void window(WindowDef& value, const std::vector<Namespace>& scopes) {
        for (auto& field : value.partitionBy) expression(field, scopes);
        for (auto& field : value.orderBy) expression(field.first, scopes);
        expression(value.frameStart, scopes); expression(value.frameEnd, scopes);
    }
    static std::string projectionTypeLabel(const std::string& spelling) {
        if(const auto geometry=geometric_input_detail::builtinType(spelling);!geometry.empty())
            return geometry;
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
                              std::vector<const Expr*>* leaves = nullptr,
                              std::vector<PreparedQuery::ProjectionBinding>* bindings = nullptr) {
        QueryRowDescriptor columns;
        for (auto& item : items) {
            if (!item.expr) throw DbError("42601", "query projection has no expression");
            if (item.expr && item.expr->type == ExprType::Literal &&
                static_cast<LiteralExpr*>(item.expr.get())->value == "*") {
                if (scopes.empty()) throw DbError("42601", "SELECT * has no source");
                for (const auto& range : scopes.front())
                    for (size_t i = 0; i < range.columns.size(); ++i) {
                        const auto& column = range.columns[i];
                        if (!range.hiddenUnqualified.count(column.name)) {
                            columns.push_back(column);
                            if (leaves) leaves->push_back(nullptr);
                            if (bindings) bindings->push_back({item.expr.get(),
                                QueryColumnBinding{0,range.occurrence,i,column.type,range.mergedUsing,column.typeOid}});
                        }
                    }
                continue;
            }
            if (const auto* star = dynamic_cast<ColumnRefExpr*>(item.expr.get()); star && star->column == "*") {
                bool found = false;
                if (!scopes.empty()) for (const auto& range : scopes.front()) {
                    if (!rangeMatches(*star, range)) continue;
                    found = true; columns.insert(columns.end(), range.columns.begin(), range.columns.end());
                    if (leaves) leaves->insert(leaves->end(), range.columns.size(), nullptr);
                    if (bindings)
                        for (size_t i=0;i<range.columns.size();++i)
                            bindings->push_back({item.expr.get(),
                                QueryColumnBinding{0,range.occurrence,i,range.columns[i].type,range.mergedUsing,range.columns[i].typeOid}});
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
            if(item.alias.empty() && item.expr->preparedSubquery) {
                const auto child=result.statementOutputs.find(item.expr->preparedSubquery.get());
                if(child!=result.statementOutputs.end() && child->second.size()==1)
                    name=child->second.front().name;
            }
            if (columnLabel && item.alias.empty() && item.expr->type == ExprType::Parameter) {
                if (item.sourceExpressionEnd == std::string::npos)
                    throw DbError("XX000", "projection has no source provenance");
                std::string quoted = "\"";
                for (char c : name) { quoted += c; if (c == '"') quoted += c; }
                quoted += '"';
                item.alias = quoted;
                result.projectionAliases.emplace_back(item.sourceExpressionEnd, quoted);
            }
            columns.push_back({name, type, false, 0, expressionTypeOid(item.expr.get())});
            if (leaves) leaves->push_back(item.expr.get());
            if (bindings) {
                const auto* column = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
                bindings->push_back({item.expr.get(),column ? column->binding : std::nullopt});
            }
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
                } else if (viewSelect && viewSelect->selectList.size() == range.viewQuery->output.size()) {
                    castOutput(viewSelect->selectList[i].expr);
                    // The retained projection must refer to its actual new
                    // root, not the still-live operand of this implicit cast.
                    auto& projected = range.viewQuery->projectionBindings.at(viewSelect).at(i);
                    projected.expression = viewSelect->selectList[i].expr.get();
                    projected.column.reset();
                }
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
    void coerceSetUnknown(Stmt* statement,size_t ordinal,const std::string& targetType) {
        auto* select=dynamic_cast<SelectStmt*>(statement);
        if(!select)throw DbError("XX000","set operand lost its prepared SELECT");
        auto bindings=result.projectionBindings.find(select);
        if(bindings==result.projectionBindings.end() || ordinal>=bindings->second.size())
            throw DbError("XX000","unknown set output has no projection provenance");
        const auto* original=bindings->second[ordinal].expression;
        auto item=std::find_if(select->selectList.begin(),select->selectList.end(),
            [&](const auto& value){return value.expr.get()==original;});
        if(item==select->selectList.end() || bindings->second[ordinal].column)
            throw DbError("XX000","unknown set output has no direct value site");
        coerceCaseInput(item->expr,"unknown",targetType);
        // Replacing the genuine value root must preserve expanded-output
        // identity. Source bytes/parameter uses and output labels stay put.
        for(auto& binding:bindings->second)
            if(binding.expression==original)binding.expression=item->expr.get();
        auto& leaves=projectionLeaves[select];
        for(auto& leaf:leaves)if(leaf==original)leaf=item->expr.get();
        if(auto output=result.statementOutputs.find(statement);output!=result.statementOutputs.end()) {
            output->second.at(ordinal).type=targetType;
            output->second.at(ordinal).typeOid=0;
        }
    }
    QueryRowDescriptor setColumns(SelectStmt& select,QueryRowDescriptor left,QueryRowDescriptor right) {
        if(left.size()!=right.size())throw DbError("42601","set query column count mismatch");
        auto output=left;
        for(size_t i=0;i<left.size();++i) {
            const auto common=selectCommonType({left[i].type,right[i].type},"set query");
            // A common result is not the left branch's physical type identity
            // when coercion or a different catalog type produced its value.
            if(!left[i].typeOid || left[i].typeOid!=right[i].typeOid ||
               common_type_detail::canonical(left[i].type)!=common_type_detail::canonical(common) ||
               common_type_detail::canonical(right[i].type)!=common_type_detail::canonical(common))
                output[i].typeOid=0;
            if(common_type_detail::canonical(left[i].type)=="unknown") {
                coerceSetUnknown(select.setOpLhs?select.setOpLhs.get():&select,i,common);
                left[i].type=common;
            }
            if(common_type_detail::canonical(right[i].type)=="unknown") {
                coerceSetUnknown(select.setOpRhs.get(),i,common);
                right[i].type=common;
            }
            output[i].type=common;
        }
        result.setOperationInputs[&select]={std::move(left),std::move(right)};
        if(select.withTies && select.orderBy.empty())throw DbError("42601","WITH TIES cannot be specified without ORDER BY");
        auto& keys = result.setOrderColumns[&select];
        for (auto& order : select.orderBy) {
            // Transform names, routine signatures and analysis-time inputs
            // before rejecting an additional expression as a set sort key.
            // This scope is the genuine set output descriptor; no physical
            // source occurrence or executable datum is fabricated.
            const auto previous=setOrderScope;setOrderScope=&output;
            struct RestoreOrderScope { const QueryRowDescriptor*& slot;const QueryRowDescriptor* previous;~RestoreOrderScope(){slot=previous;} } guard{setOrderScope,previous};
            expression(order.expr,{});
            const auto* column = dynamic_cast<const ColumnRefExpr*>(order.expr.get());
            const auto* literal = dynamic_cast<const LiteralExpr*>(order.expr.get());
            size_t ordinal = output.size();
            if (column) {
                if (!column->schema.empty() || !column->table.empty())
                    throw DbError("42P01", "set ORDER BY has no branch FROM namespace");
                for (size_t i=0;i<output.size();++i) if (output[i].name == column->column) {
                    if (ordinal != output.size()) throw DbError("42702", "ORDER BY name is ambiguous");
                    ordinal = i;
                }
                if (ordinal == output.size()) throw DbError("42703", "ORDER BY column does not exist: " + column->column);
            } else if (literal && literal->typeName.empty() && !literal->preparedSubquery &&
                       !literal->value.empty() && std::all_of(literal->value.begin(),literal->value.end(),
                           [](unsigned char value){return value>='0' && value<='9';})) {
                size_t position = 0;
                for (const auto value : literal->value) {
                    if (position > output.size()) break;
                    position = position * 10 + size_t(value-'0');
                }
                if (!position || position > output.size()) throw DbError("42P10", "ORDER BY position is not in select list");
                ordinal = position-1;
            } else if (literal && !literal->preparedSubquery && literal->typeName.empty()) {
                if(decimalIntegerConstant(literal->value))throw DbError("42P10","ORDER BY position is not in select list");
                throw DbError("42601","non-integer constant in ORDER BY");
            } else throw DbError("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause");
            keys.push_back(ordinal);
        }
        return output;
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
                if(!select->setOpRhs)throw DbError("42601","set query right operand is missing");
                return setColumns(*select,std::move(columns),statement(*select->setOpRhs,outer,ctes));
            }
            auto scopes = outer;
            scopes.insert(scopes.begin(), from(select->fromClause.get(), outer, ctes));
            auto& projected = result.projectionBindings[select]; projected.clear();
            auto columns = project(select->selectList, scopes, &projectionLeaves[select], &projected);
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
            const auto condition = expression(select->whereClause, scopes);
            if (select->whereClause) {
                const auto type = common_type_detail::canonical(condition);
                if (type != "unknown" && type != "boolean")
                    throw DbError("42804","argument of WHERE must be type boolean");
                // Preserve the source's actual expression and static type.
                // Unknown input acquires boolean context during analysis;
                // row execution must not rediscover this from datum bytes or
                // fall back merely because 'true' was still unknown/TEXT.
                coerceCaseInput(select->whereClause,condition,"boolean");
            }
            for (auto& value : select->distinctOn) expression(value, scopes);
            for (auto& value : select->groupBy) expression(value, scopes);
            for (auto& group : select->groupByElems) for (auto& value : group.exprs) expression(value, scopes);
            expression(select->having, scopes);
            for (auto& value : select->windowDefs) window(value, scopes);
            for (auto& order : select->orderBy) {
                const auto* ref = dynamic_cast<ColumnRefExpr*>(order.expr.get());
                const bool outputAlias = ref && ref->table.empty() && ref->schema.empty() &&
                    std::any_of(columns.begin(), columns.end(), [&](const auto& col) { return col.name == ref->column; });
                std::string orderType;
                uint32_t orderOid=0;
                if(outputAlias) {
                    const auto output=std::find_if(columns.begin(),columns.end(),[&](const auto& col){return col.name==ref->column;});
                    orderType=output->type;orderOid=output->typeOid;
                } else {
                    orderType=expression(order.expr,scopes);
                    orderOid=expressionTypeOid(order.expr.get());
                    const auto* literal=dynamic_cast<const LiteralExpr*>(order.expr.get());
                    if(literal && !literal->value.empty() && std::all_of(literal->value.begin(),literal->value.end(),
                        [](unsigned char c){return c>='0' && c<='9';})) {
                        size_t ordinal=0;
                        for(char c:literal->value){if(ordinal>columns.size())break;ordinal=ordinal*10+size_t(c-'0');}
                        if(ordinal && ordinal<=columns.size()) {
                            orderType=columns[ordinal-1].type;orderOid=columns[ordinal-1].typeOid;
                        }
                    }
                }
                order.enumComparison=enumSortComparison(orderType,orderOid);
            }
            if(select->command!=SqlCommand::Values)
                for (auto& row : select->valuesRows) for (auto& value : row) expression(value, scopes);
            if (select->setOpRhs)
                return setColumns(*select,std::move(columns),statement(*select->setOpRhs,outer,ctes));
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
            auto output=returning(insert->returning,insert->returningOptions,target,std::move(scopes));
            // Analysis of every explicit input and RETURNING site precedes
            // stored-default planning. Retain only actually omitted/DEFAULT
            // physical columns; a fully supplied bad default is not reached.
            insert->preparedDefaults.clear();
            std::vector<bool> needsDefault(target.columns.size(),insert->defaultValues);
            const auto inputOrdinal=[&](size_t physical)->std::optional<size_t> {
                for(size_t i=0;i<assignments.size();++i)
                    if(assignments[i].name==target.columns[physical].name)return i;
                return std::nullopt;
            };
            for(size_t physical=0;physical<target.columns.size();++physical) {
                const auto input=inputOrdinal(physical);
                if(!input)needsDefault[physical]=true;
                else {
                    for(const auto& row:insert->values) {
                        if(*input>=row.size()){needsDefault[physical]=true;continue;}
                        const auto* literal=dynamic_cast<const LiteralExpr*>(row[*input].get());
                        if(literal && !literal->preparedSubquery && literal->typeName.empty() &&
                            SQLParser::toLower(literal->value)=="default")needsDefault[physical]=true;
                    }
                    if(insert->selectSource && *input>=result.statementOutputs.at(insert->selectSource.get()).size())
                        needsDefault[physical]=true;
                }
                const auto& column=target.columns[physical];
                // Sequences owned by IDENTITY and stored/generated values
                // retain their existing storage demand/override contract.
                if(needsDefault[physical] && !column.generated && !column.identity) {
                    auto value=targetDefault(target,column);
                    if(value)insert->preparedDefaults.emplace_back(physical,std::move(value));
                }
            }
            return output;
        }
        if (auto* update = dynamic_cast<UpdateStmt*>(&node)) {
            auto ranges = from(update->fromClause.get(), outer, ctes);
            const auto target = registerSource(relation(update->tableName, update->alias, {}));
            checkRangeConflicts({target}, ranges);
            ranges.insert(ranges.begin(), target);
            auto scopes = outer; scopes.insert(scopes.begin(), std::move(ranges));
            std::vector<std::string> assignmentTypes;
            for (auto& value : update->setClauses) {
                const auto name = identifier(value.first);
                const auto& target = scopes.front().front().columns;
                if (std::none_of(target.begin(), target.end(), [&](const auto& column) { return column.name == name; }))
                    throw DbError("42703", "UPDATE target column does not exist: " + name);
                const auto* literal=dynamic_cast<const LiteralExpr*>(value.second.get());
                if (metadata.updateDefault && literal && !literal->preparedSubquery &&
                    literal->typeName.empty() && SQLParser::toLower(literal->value)=="default") {
                    const auto& range=scopes.front().front();
                    const auto column=std::find_if(target.begin(),target.end(),
                        [&](const auto& item){return item.name==name;});
                    const auto targetType=ExprHelper::canonicalResultTypeName(column->type);
                    value.second=targetDefault(range,*column);
                    if(!value.second) {
                        auto null=std::make_unique<LiteralExpr>();null->value="NULL";
                        value.second=std::move(null);
                        coerceCaseInput(value.second,"unknown",targetType);
                    }
                    assignmentTypes.push_back(targetType);
                    continue;
                }
                assignmentTypes.push_back(expression(value.second, scopes));
            }
            const auto condition = expression(update->whereClause, scopes);
            if (update->whereClause) {
                const auto type = common_type_detail::canonical(condition);
                if (type != "unknown" && type != "boolean")
                    throw DbError("42804","argument of WHERE must be type boolean");
                // WHERE transformation precedes RETURNING and contextual SET
                // assignment coercion. This creates a genuine boolean AST
                // cast and uses the existing pure unknown-input hook.
                coerceCaseInput(update->whereClause,condition,"boolean");
            }
            auto output = returning(update->returning,update->returningOptions,scopes.front().front(),scopes);
            if (metadata.assignmentInput)
                for (size_t i=0;i<update->setClauses.size();++i) {
                    const auto name = identifier(update->setClauses[i].first);
                    const auto column = std::find_if(target.columns.begin(),target.columns.end(),
                        [&](const auto& candidate) { return candidate.name == name; });
                    metadata.assignmentInput(*column,update->setClauses[i].second.get(),assignmentTypes[i]);
                }
            return output;
        }
        if (auto* remove = dynamic_cast<DeleteStmt*>(&node)) {
            auto ranges = from(remove->usingClause.get(), outer, ctes);
            const auto target = registerSource(relation(remove->tableName, remove->alias, {}));
            checkRangeConflicts({target}, ranges);
            ranges.insert(ranges.begin(), target);
            auto scopes = outer; scopes.insert(scopes.begin(), std::move(ranges));
            const auto condition=expression(remove->whereClause,scopes);
            if(remove->whereClause) {
                const auto type=common_type_detail::canonical(condition);
                if(type!="unknown" && type!="boolean")
                    throw DbError("42804","argument of WHERE must be type boolean");
                // DELETE has the same analysis-time predicate context as
                // SELECT/UPDATE, before RETURNING names or target execution.
                coerceCaseInput(remove->whereClause,condition,"boolean");
            }
            return returning(remove->returning,remove->returningOptions,scopes.front().front(),scopes);
        }
        if (auto* explain = dynamic_cast<ExplainStmt*>(&node)) {
            if (!explain->query) throw DbError("42601", "EXPLAIN query is missing");
            statement(*explain->query, outer, ctes);
            return {{"QUERY PLAN",explain->json?"json":explain->xml?"xml":"text",
                false,0,explain->json?114u:explain->xml?142u:25u}};
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
