#include "expr_helper.h"
#include "ExprEvaluator.h"
#include "parser/parser.h"
#include "parser/ast.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <memory>
#include <sstream>
#include <vector>

namespace dbms {

namespace {

std::string toLower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

std::string formatUtcClock(std::time_t value, const char* format) {
    std::tm utc{};
    if (::gmtime_r(&value, &utc) == nullptr) return "";
    char buffer[40];
    if (std::strftime(buffer, sizeof(buffer), format, &utc) == 0) return "";
    return buffer;
}

bool looksLikeNumber(const std::string& s) {
    if (s.empty()) return false;
    size_t i = 0;
    if (s[0] == '+' || s[0] == '-') i = 1;
    bool hasDot = false, hasDigit = false;
    for (; i < s.size(); ++i) {
        if (s[i] >= '0' && s[i] <= '9') { hasDigit = true; continue; }
        if (s[i] == '.' && !hasDot) { hasDot = true; continue; }
        return false;
    }
    return hasDigit;
}

std::string inferType(const std::string& value) {
    if (value.empty()) return "text";
    if (looksLikeNumber(value)) {
        return value.find('.') != std::string::npos ? "double precision" : "integer";
    }
    return "text";
}

std::string canonicalTypeName(const std::string& storageType) {
    std::string t = toLower(storageType);
    if (t == "int2" || t == "int4" || t == "int8" ||
        t == "smallint" || t == "integer" || t == "bigint" ||
        t == "serial" || t == "bigserial") {
        return "integer";
    }
    if (t == "float4" || t == "real") return "real";
    if (t == "float8" || t == "double precision") return "double precision";
    if (t == "numeric" || t == "decimal") return "numeric";
    if (t == "varchar" || t == "character varying" ||
        t == "char" || t == "character" || t == "text" ||
        t == "bpchar") {
        return "text";
    }
    if (t == "bool" || t == "boolean") return "boolean";
    if (t == "timestamp" || t == "datetime") return "timestamp";
    if (t == "timestamptz" || t == "timestamp with time zone")
        return "timestamptz";
    if (t == "date") return "date";
    if (t == "time") return "time";
    if (t == "timetz" || t == "time with time zone") return "timetz";
    if (t == "interval") return "interval";
    if (t == "uuid") return "uuid";
    return t;
}

std::string protocolTypeName(std::string type) {
    type = toLower(type);
    const size_t modifier = type.find('(');
    if (modifier != std::string::npos) type.resize(modifier);
    while (!type.empty() && std::isspace(static_cast<unsigned char>(type.back())))
        type.pop_back();
    if (type == "int" || type == "int4" || type == "serial") return "integer";
    if (type == "int2" || type == "smallserial") return "smallint";
    if (type == "int8" || type == "bigserial") return "bigint";
    if (type == "decimal") return "numeric";
    if (type == "float8" || type == "double" || type == "float")
        return "double precision";
    if (type == "float4") return "real";
    if (type == "bool") return "boolean";
    if (type == "character varying") return "varchar";
    if (type == "character") return "bpchar";
    return type;
}

bool numericProtocolType(const std::string& type) {
    const std::string t = protocolTypeName(type);
    return t == "smallint" || t == "integer" || t == "bigint" ||
           t == "numeric" || t == "real" || t == "double precision";
}

int numericTypeRank(const std::string& type) {
    const std::string t = protocolTypeName(type);
    if (t == "double precision") return 6;
    if (t == "real") return 5;
    if (t == "numeric") return 4;
    if (t == "bigint") return 3;
    if (t == "integer") return 2;
    if (t == "smallint") return 1;
    return 0;
}

std::string mergeProtocolTypes(const std::string& leftRaw,
                               const std::string& rightRaw) {
    const std::string left = protocolTypeName(leftRaw);
    const std::string right = protocolTypeName(rightRaw);
    if (left.empty() || left == "unknown") return right;
    if (right.empty() || right == "unknown") return left;
    if (left == right) return left;
    if (numericProtocolType(left) && numericProtocolType(right)) {
        const int rank = std::max(numericTypeRank(left), numericTypeRank(right));
        if (rank >= 6) return "double precision";
        if (rank == 5) return "real";
        if (rank == 4) return "numeric";
        if (rank == 3) return "bigint";
        if (rank == 2) return "integer";
        return "smallint";
    }
    if ((left == "text" || left == "varchar" || left == "bpchar") &&
        (right == "text" || right == "varchar" || right == "bpchar")) {
        if (left == "text" || right == "text") return "text";
        if (left == "varchar" || right == "varchar") return "varchar";
        return "bpchar";
    }
    return left;
}

std::string inferAstResultType(
    const Expr* expression,
    const std::map<std::string, std::string>& typeHints) {
    if (!expression) return "text";
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expression)) {
        if (!literal->typeName.empty())
            return protocolTypeName(literal->typeName);
        const std::string value = toLower(literal->value);
        if (value == "null" ||
            (literal->value.size() >= 2 && literal->value.front() == '\'' &&
             literal->value.back() == '\'')) return "unknown";
        if (value == "true" || value == "false") return "boolean";
        if (looksLikeNumber(literal->value))
            return literal->value.find_first_of(".eE") == std::string::npos
                ? "integer" : "numeric";
        return "unknown";
    }
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(expression)) {
        for (const std::string& key : {
                 column->toString(), column->column}) {
            auto found = typeHints.find(key);
            if (found != typeHints.end())
                return protocolTypeName(found->second);
        }
        return "text";
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        return protocolTypeName(cast->typeName);
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) {
        const std::string op = toLower(unary->op);
        if (op == "not" || op.find("is ") == 0) return "boolean";
        return inferAstResultType(unary->operand.get(), typeHints);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
        const std::string op = toLower(binary->op);
        static const std::set<std::string> booleanOperators = {
            "and", "or", "=", "<>", "!=", "<", ">", "<=", ">=",
            "like", "not like", "ilike", "not ilike", "in", "not in",
            "between", "not between", "is distinct from",
            "is not distinct from", "similar to", "not similar to"
        };
        if (booleanOperators.count(op)) return "boolean";
        if (op == "||") return "text";
        const std::string left = inferAstResultType(binary->left.get(), typeHints);
        const std::string right = inferAstResultType(binary->right.get(), typeHints);
        if (op == "+" || op == "-") {
            if (left == "date" && right == "interval") return "timestamp";
            if ((left == "timestamp" || left == "timestamptz") &&
                right == "interval") return left;
            if (op == "-" && left == "date" && right == "date") return "integer";
            if (op == "-" && left == "timestamp" && right == "timestamp")
                return "interval";
            if (left == "date" && numericProtocolType(right)) return "date";
        }
        return mergeProtocolTypes(left, right);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expression)) {
        std::string element = "unknown";
        for (const auto& value : array->elements)
            element = mergeProtocolTypes(
                element, inferAstResultType(value.get(), typeHints));
        if (element.empty() || element == "unknown") element = "text";
        return element + "[]";
    }
    if (dynamic_cast<const RowExpr*>(expression)) return "record";
    if (const auto* caseExpression = dynamic_cast<const CaseExpr*>(expression)) {
        std::string result = "unknown";
        for (const auto& clause : caseExpression->whenClauses)
            result = mergeProtocolTypes(
                result, inferAstResultType(clause.second.get(), typeHints));
        if (caseExpression->elseExpr)
            result = mergeProtocolTypes(
                result, inferAstResultType(caseExpression->elseExpr.get(), typeHints));
        return result;
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
        const std::string name = toLower(call->funcName);
        auto argType = [&](size_t index) {
            return index < call->args.size()
                ? inferAstResultType(call->args[index].get(), typeHints)
                : std::string("unknown");
        };
        if (name == "cast" && call->args.size() >= 2) {
            if (const auto* target =
                    dynamic_cast<const ColumnRefExpr*>(call->args[1].get())) {
                return protocolTypeName(target->column);
            }
            if (const auto* target =
                    dynamic_cast<const LiteralExpr*>(call->args[1].get())) {
                return protocolTypeName(target->value);
            }
            return protocolTypeName(call->args[1]->toString());
        }
        if (name == "case_when") {
            std::string result = "unknown";
            for (size_t i = 1; i < call->args.size(); i += 2)
                result = mergeProtocolTypes(result, argType(i));
            if (call->args.size() % 2 == 1)
                result = mergeProtocolTypes(result, argType(call->args.size() - 1));
            return result;
        }
        if (name == "exists" || name == "is_null" || name == "is_not_null" ||
            name == "isdistinct" || name == "isnotdistinct") return "boolean";
        if (name == "count" || name == "row_number" || name == "rank" ||
            name == "dense_rank") return "bigint";
        if (name == "ntile" || name == "width_bucket" || name == "length" ||
            name == "char_length" || name == "character_length" ||
            name == "bit_length" || name == "octet_length" || name == "strpos" ||
            name == "position" || name == "ascii" || name == "gcd" ||
            name == "lcm") return "integer";
        if (name == "percent_rank" || name == "cume_dist" || name == "date_part")
            return "double precision";
        if (name == "extract") return "numeric";
        if (name == "age") return "interval";
        if (name == "to_timestamp") return "timestamptz";
        if (name == "date_trunc") return argType(1);
        if (name == "current_date") return "date";
        if (name == "now" || name == "current_timestamp") return "timestamptz";
        if (name == "string_to_array" || name == "regexp_split_to_array" ||
            name == "regexp_matches") return "text[]";
        if (name == "array_append" || name == "array_prepend" ||
            name == "array_remove" || name == "array_replace")
            return name == "array_prepend" ? argType(1) : argType(0);
        if (name == "array_agg") {
            const std::string element = argType(0);
            return (element.empty() || element == "unknown" ? "text" : element) + "[]";
        }
        if (name == "string_agg") return "text";
        if (name == "json_agg") return "json";
        if (name == "jsonb_agg") return "jsonb";
        if (name == "bool_and" || name == "bool_or" || name == "every")
            return "boolean";
        if (name == "sum") {
            const std::string input = argType(0);
            if (input == "smallint" || input == "integer") return "bigint";
            if (input == "bigint" || input == "numeric") return "numeric";
            if (input == "real" || input == "double precision")
                return "double precision";
        }
        if (name == "avg" || name == "stddev" || name == "stddev_samp" ||
            name == "stddev_pop" || name == "variance" || name == "var_samp" ||
            name == "var_pop") {
            const std::string input = argType(0);
            return input == "real" || input == "double precision"
                ? "double precision" : "numeric";
        }
        if (name == "sign") {
            const std::string input = argType(0);
            return input == "numeric" ? "numeric" : "double precision";
        }
        if (name == "min" || name == "max" || name == "lag" ||
            name == "lead" || name == "first_value" || name == "last_value" ||
            name == "nth_value" || name == "coalesce" || name == "nullif" ||
            name == "greatest" || name == "least" || name == "abs" ||
            name == "round" || name == "trunc" || name == "ceil" ||
            name == "ceiling" || name == "floor" || name == "mod")
            return argType(0);
        if (name == "div") return "numeric";
        if (name == "power")
            return argType(0) == "numeric" ? "numeric" : "double precision";
        if (name == "exp" || name == "ln" || name == "log" || name == "sqrt" ||
            name == "sin" || name == "cos" || name == "tan")
            return argType(0) == "numeric" ? "numeric" : "double precision";
        static const std::set<std::string> textFunctions = {
            "lower", "upper", "initcap", "concat", "concat_ws", "substring",
            "substr", "left", "right", "trim", "btrim", "ltrim", "rtrim",
            "reverse", "replace", "translate", "format", "quote_literal",
            "quote_nullable", "overlay", "to_char"
        };
        if (textFunctions.count(name)) return "text";
        return "text";
    }
    return "text";
}

bool countParsedColumnReferences(
    const Expr* expression, const std::string& columnName, size_t& count) {
    if (!expression) return true;
    switch (expression->type) {
        case ExprType::Literal:
        case ExprType::Parameter:
        case ExprType::A_Star:
            return true;
        case ExprType::ColumnRef: {
            const auto* column =
                dynamic_cast<const ColumnRefExpr*>(expression);
            if (!column) return false;
            if (column->column == columnName) ++count;
            return true;
        }
        case ExprType::UnaryOp: {
            const auto* unary =
                dynamic_cast<const UnaryOpExpr*>(expression);
            return unary && countParsedColumnReferences(
                unary->operand.get(), columnName, count);
        }
        case ExprType::BinaryOp: {
            const auto* binary =
                dynamic_cast<const BinaryOpExpr*>(expression);
            return binary &&
                countParsedColumnReferences(
                    binary->left.get(), columnName, count) &&
                countParsedColumnReferences(
                    binary->right.get(), columnName, count);
        }
        case ExprType::FunctionCall: {
            const auto* function =
                dynamic_cast<const FunctionCallExpr*>(expression);
            if (!function) return false;
            for (const auto& argument : function->args) {
                if (!countParsedColumnReferences(
                        argument.get(), columnName, count)) return false;
            }
            for (const auto& argument : function->namedArgs) {
                if (!countParsedColumnReferences(
                        argument.value.get(), columnName, count)) return false;
            }
            if (!countParsedColumnReferences(
                    function->filter.get(), columnName, count)) return false;
            for (const auto& partition : function->over.partitionBy) {
                if (!countParsedColumnReferences(
                        partition.get(), columnName, count)) return false;
            }
            for (const auto& order : function->over.orderBy) {
                if (!countParsedColumnReferences(
                        order.first.get(), columnName, count)) return false;
            }
            return countParsedColumnReferences(
                       function->over.frameStart.get(), columnName, count) &&
                   countParsedColumnReferences(
                       function->over.frameEnd.get(), columnName, count);
        }
        case ExprType::CastExpr: {
            const auto* cast = dynamic_cast<const CastExpr*>(expression);
            return cast && countParsedColumnReferences(
                cast->operand.get(), columnName, count);
        }
        case ExprType::CaseExpr: {
            const auto* caseExpression =
                dynamic_cast<const CaseExpr*>(expression);
            if (!caseExpression ||
                !countParsedColumnReferences(
                    caseExpression->switchExpr.get(), columnName, count)) {
                return false;
            }
            for (const auto& clause : caseExpression->whenClauses) {
                if (!countParsedColumnReferences(
                        clause.first.get(), columnName, count) ||
                    !countParsedColumnReferences(
                        clause.second.get(), columnName, count)) {
                    return false;
                }
            }
            return countParsedColumnReferences(
                caseExpression->elseExpr.get(), columnName, count);
        }
        case ExprType::ArrayExpr: {
            const auto* array =
                dynamic_cast<const ArrayExpr*>(expression);
            if (!array) return false;
            for (const auto& element : array->elements) {
                if (!countParsedColumnReferences(
                        element.get(), columnName, count)) return false;
            }
            return true;
        }
        case ExprType::RowExpr: {
            const auto* row = dynamic_cast<const RowExpr*>(expression);
            if (!row) return false;
            for (const auto& element : row->elements) {
                if (!countParsedColumnReferences(
                        element.get(), columnName, count)) return false;
            }
            return true;
        }
        case ExprType::Subquery:
            // Stored row expressions must not hide bindings in a subquery.
            return false;
    }
    return false;
}

const Expr* parseStoredExpression(
    const std::string& expression, ParseResult& parsed) {
    SQLParser parser;
    parsed = parser.parse("SELECT " + expression);
    if (!parsed.success || !parsed.stmt) return nullptr;
    const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
    if (!select || select->selectList.size() != 1 ||
        !select->selectList.front().expr) {
        return nullptr;
    }
    return select->selectList.front().expr.get();
}

enum class SourceTokenKind { Identifier, String, Symbol };

struct SourceToken {
    size_t begin = 0;
    size_t end = 0;
    SourceTokenKind kind = SourceTokenKind::Symbol;
    std::string text;
};

bool isExpressionSymbol(char c) {
    switch (c) {
        case '(':
        case ')':
        case ',':
        case ';':
        case '*':
        case '=':
        case '<':
        case '>':
        case '+':
        case '-':
        case '/':
        case '%':
        case '^':
        case '~':
        case '!':
        case '|':
        case '&':
        case '#':
        case '@':
        case '?':
        case ':':
        case '[':
        case ']':
        case '.':
            return true;
        default:
            return false;
    }
}

std::optional<std::vector<SourceToken>> lexExpressionSource(
    const std::string& expression) {
    std::vector<SourceToken> tokens;
    const auto append = [&](size_t begin, size_t end, SourceTokenKind kind) {
        tokens.push_back(
            {begin, end, kind, expression.substr(begin, end - begin)});
    };

    for (size_t index = 0; index < expression.size();) {
        const unsigned char current =
            static_cast<unsigned char>(expression[index]);
        if (std::isspace(current)) {
            ++index;
            continue;
        }
        if (expression.compare(index, 2, "--") == 0) {
            const size_t newline = expression.find('\n', index + 2);
            index = newline == std::string::npos
                ? expression.size() : newline + 1;
            continue;
        }
        if (expression.compare(index, 2, "/*") == 0) {
            const size_t close = expression.find("*/", index + 2);
            if (close == std::string::npos) return std::nullopt;
            index = close + 2;
            continue;
        }

        if (expression[index] == '$') {
            size_t delimiterEnd = index + 1;
            bool validTag = true;
            if (delimiterEnd < expression.size() &&
                std::isdigit(static_cast<unsigned char>(
                    expression[delimiterEnd]))) {
                validTag = false;
            }
            while (validTag && delimiterEnd < expression.size() &&
                   (std::isalnum(static_cast<unsigned char>(
                        expression[delimiterEnd])) ||
                    expression[delimiterEnd] == '_')) {
                ++delimiterEnd;
            }
            if (validTag && delimiterEnd < expression.size() &&
                expression[delimiterEnd] == '$') {
                const std::string delimiter = expression.substr(
                    index, delimiterEnd - index + 1);
                const size_t close = expression.find(
                    delimiter, delimiterEnd + 1);
                if (close == std::string::npos) return std::nullopt;
                const size_t end = close + delimiter.size();
                append(index, end, SourceTokenKind::String);
                index = end;
                continue;
            }
        }

        if (expression[index] == '\'') {
            const size_t begin = index++;
            bool closed = false;
            while (index < expression.size()) {
                if (expression[index] != '\'') {
                    ++index;
                    continue;
                }
                if (index + 1 < expression.size() &&
                    expression[index + 1] == '\'') {
                    index += 2;
                    continue;
                }
                size_t backslashCount = 0;
                for (size_t cursor = index;
                     cursor > begin + 1 && expression[cursor - 1] == '\\';
                     --cursor) {
                    ++backslashCount;
                }
                if (backslashCount % 2 != 0) {
                    ++index;
                    continue;
                }
                ++index;
                closed = true;
                break;
            }
            if (!closed) return std::nullopt;
            append(begin, index, SourceTokenKind::String);
            continue;
        }

        if (expression[index] == '"') {
            const size_t begin = index++;
            bool closed = false;
            while (index < expression.size()) {
                if (expression[index] != '"') {
                    ++index;
                    continue;
                }
                if (index + 1 < expression.size() &&
                    expression[index + 1] == '"') {
                    index += 2;
                    continue;
                }
                ++index;
                closed = true;
                break;
            }
            if (!closed) return std::nullopt;
            append(begin, index, SourceTokenKind::Identifier);
            continue;
        }

        if (isExpressionSymbol(expression[index])) {
            const size_t begin = index;
            size_t length = 1;
            if (index + 1 < expression.size()) {
                const std::string two = expression.substr(index, 2);
                if (two == "<=" || two == ">=" || two == "<>" ||
                    two == "!=" || two == "::" || two == "||" ||
                    two == "->" || two == "~*" || two == "!~" ||
                    two == "@@" || two == "&&" || two == "<<" ||
                    two == ">>" || two == "=>" || two == "#>" ||
                    two == "@>" || two == "<@") {
                    length = 2;
                }
            }
            if (index + 2 < expression.size()) {
                const std::string three = expression.substr(index, 3);
                if (three == "->>" || three == "#>>" || three == "!~*") {
                    length = 3;
                }
            }
            index += length;
            append(begin, index, SourceTokenKind::Symbol);
            continue;
        }

        const size_t begin = index;
        while (index < expression.size() &&
               !std::isspace(static_cast<unsigned char>(expression[index])) &&
               expression[index] != '\'' && expression[index] != '"' &&
               !isExpressionSymbol(expression[index])) {
            ++index;
        }
        if (begin == index) return std::nullopt;
        append(begin, index, SourceTokenKind::Identifier);
    }
    return tokens;
}

bool tokenIs(const SourceToken& token, const std::string& text) {
    return token.text == text;
}

bool tokenIsKeyword(const SourceToken& token, const std::string& keyword) {
    return token.kind == SourceTokenKind::Identifier &&
           toLower(token.text) == keyword;
}

std::vector<size_t> columnReferenceSourceTokens(
    const std::vector<SourceToken>& tokens, const std::string& columnName) {
    std::vector<size_t> references;
    for (size_t index = 0; index < tokens.size(); ++index) {
        if (tokens[index].kind != SourceTokenKind::Identifier ||
            tokens[index].text != columnName) {
            continue;
        }
        const SourceToken* previous =
            index > 0 ? &tokens[index - 1] : nullptr;
        const SourceToken* next =
            index + 1 < tokens.size() ? &tokens[index + 1] : nullptr;

        // A name before '.' is a schema/table qualifier. A name before '('
        // is a function, and a name before '=>' is a named argument.
        if (next && (tokenIs(*next, ".") || tokenIs(*next, "(") ||
                     tokenIs(*next, "=>"))) {
            continue;
        }
        if (next && tokenIs(*next, "=") && index + 2 < tokens.size() &&
            tokenIs(tokens[index + 2], ">")) {
            continue;
        }

        // These grammar positions name types/collations rather than row
        // values. If a more exotic construct remains ambiguous, the AST
        // reference-count check below rejects the whole rewrite safely.
        if (previous &&
            (tokenIs(*previous, "::") ||
             tokenIsKeyword(*previous, "as") ||
             tokenIsKeyword(*previous, "collate"))) {
            continue;
        }
        references.push_back(index);
    }
    return references;
}

} // namespace

std::string ExprHelper::inferResultType(
    const std::string& exprSql,
    const std::map<std::string, std::string>& typeHints) {
    const std::string trimmed = [&] {
        size_t first = exprSql.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) return std::string{};
        size_t last = exprSql.find_last_not_of(" \t\r\n");
        return exprSql.substr(first, last - first + 1);
    }();
    const std::string lower = toLower(trimmed);

    // The expression parser accepts PostgreSQL postfix casts while evaluating,
    // but older AST paths can leave the cast suffix outside the returned root.
    // Read a top-level suffix directly so protocol metadata follows the cast,
    // including typemods such as numeric(12,2).
    size_t postfixCast = std::string::npos;
    int castDepth = 0;
    bool castString = false;
    bool castIdentifier = false;
    for (size_t i = 0; i + 1 < trimmed.size(); ++i) {
        const char ch = trimmed[i];
        if (castString) {
            if (ch == '\'' && i + 1 < trimmed.size() && trimmed[i + 1] == '\'') {
                ++i;
            } else if (ch == '\'') {
                castString = false;
            }
            continue;
        }
        if (castIdentifier) {
            if (ch == '"' && i + 1 < trimmed.size() && trimmed[i + 1] == '"') {
                ++i;
            } else if (ch == '"') {
                castIdentifier = false;
            }
            continue;
        }
        if (ch == '\'') { castString = true; continue; }
        if (ch == '"') { castIdentifier = true; continue; }
        if (ch == '(' || ch == '[') { ++castDepth; continue; }
        if (ch == ')' || ch == ']') { if (castDepth > 0) --castDepth; continue; }
        if (castDepth == 0 && ch == ':' && trimmed[i + 1] == ':') {
            postfixCast = i;
            ++i;
        }
    }
    if (postfixCast != std::string::npos) {
        const std::string target = trimmed.substr(postfixCast + 2);
        if (!target.empty()) return protocolTypeName(target);
    }

    // SQL preprocessing lowers CASE into an evaluator-only case_when wrapper.
    // Conditions in that wrapper are not general SQL, so infer its value slots
    // directly instead of asking the expression parser to read them.
    if (lower.rfind("case_when(", 0) == 0 && trimmed.back() == ')') {
        std::vector<std::string> args;
        std::string current;
        int depth = 0;
        bool quoted = false;
        for (size_t i = 10; i + 1 < trimmed.size(); ++i) {
            const char ch = trimmed[i];
            if (ch == '\'') quoted = !quoted;
            if (!quoted && (ch == '(' || ch == '[')) ++depth;
            if (!quoted && (ch == ')' || ch == ']') && depth > 0) --depth;
            if (!quoted && depth == 0 && ch == ',') {
                args.push_back(current);
                current.clear();
            } else {
                current += ch;
            }
        }
        args.push_back(current);
        std::string result = "unknown";
        for (size_t i = 1; i < args.size(); i += 2)
            result = mergeProtocolTypes(
                result, inferResultType(args[i], typeHints));
        if (args.size() % 2 == 1)
            result = mergeProtocolTypes(
                result, inferResultType(args.back(), typeHints));
        if (!result.empty() && result != "unknown") return result;
    }

    static const std::vector<std::string> typedLiteralTypes = {
        "timestamptz", "timestamp", "interval", "date", "time",
        "numeric", "boolean", "text"
    };
    for (const std::string& type : typedLiteralTypes) {
        const std::string prefix = type + " ";
        if (lower.rfind(prefix, 0) == 0 &&
            trimmed.size() > prefix.size() && trimmed[prefix.size()] == '\'' &&
            trimmed.back() == '\'') return type;
    }
    if (lower.rfind("case", 0) != 0 &&
        (lower.find(" between ") != std::string::npos ||
         lower.find(" is distinct from ") != std::string::npos ||
         lower.find(" is not distinct from ") != std::string::npos ||
         lower.find(" like ") != std::string::npos ||
         lower.find(" ilike ") != std::string::npos)) {
        return "boolean";
    }
    if (lower.rfind("array[", 0) == 0 || lower.rfind("array [", 0) == 0) {
        if (trimmed.find('\'') != std::string::npos) return "text[]";
        if (trimmed.find('.') != std::string::npos) return "numeric[]";
        return "integer[]";
    }

    // Simple CASE has a selector between CASE and the first WHEN.  Some legacy
    // parser paths treat that form as an opaque expression, so merge only its
    // result arms here.  Nested CASE expressions are kept at their own depth.
    if (lower.rfind("case", 0) == 0) {
        struct CaseWord { size_t begin; size_t end; std::string word; };
        std::vector<CaseWord> words;
        int parenDepth = 0;
        int caseDepth = 0;
        bool string = false;
        bool identifier = false;
        for (size_t i = 0; i < trimmed.size();) {
            const char ch = trimmed[i];
            if (string) {
                if (ch == '\'' && i + 1 < trimmed.size() &&
                    trimmed[i + 1] == '\'') i += 2;
                else { if (ch == '\'') string = false; ++i; }
                continue;
            }
            if (identifier) {
                if (ch == '"' && i + 1 < trimmed.size() &&
                    trimmed[i + 1] == '"') i += 2;
                else { if (ch == '"') identifier = false; ++i; }
                continue;
            }
            if (ch == '\'') { string = true; ++i; continue; }
            if (ch == '"') { identifier = true; ++i; continue; }
            if (ch == '(' || ch == '[') { ++parenDepth; ++i; continue; }
            if (ch == ')' || ch == ']') {
                if (parenDepth > 0) --parenDepth;
                ++i;
                continue;
            }
            if (parenDepth == 0 &&
                (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_')) {
                const size_t begin = i++;
                while (i < trimmed.size() &&
                       (std::isalnum(static_cast<unsigned char>(trimmed[i])) ||
                        trimmed[i] == '_' || trimmed[i] == '$')) ++i;
                const std::string word = lower.substr(begin, i - begin);
                if (word == "case") {
                    ++caseDepth;
                } else if (word == "end") {
                    if (caseDepth == 1) words.push_back({begin, i, word});
                    if (caseDepth > 0) --caseDepth;
                } else if (caseDepth == 1 &&
                           (word == "then" || word == "when" ||
                            word == "else")) {
                    words.push_back({begin, i, word});
                }
                continue;
            }
            ++i;
        }
        std::string result = "unknown";
        for (size_t i = 0; i < words.size(); ++i) {
            if (words[i].word != "then" && words[i].word != "else") continue;
            const size_t valueBegin = words[i].end;
            const size_t valueEnd = i + 1 < words.size()
                ? words[i + 1].begin : trimmed.size();
            result = mergeProtocolTypes(
                result, inferResultType(
                            trimmed.substr(valueBegin, valueEnd - valueBegin),
                            typeHints));
        }
        if (!result.empty() && result != "unknown") return result;
    }

    ParseResult parsed;
    const Expr* expression = parseStoredExpression(trimmed, parsed);
    if (!expression) {
        if (lower.find(" is ") != std::string::npos ||
            lower.find(" between ") != std::string::npos ||
            lower.find(" like ") != std::string::npos) return "boolean";
        return "text";
    }
    std::string type = protocolTypeName(
        inferAstResultType(expression, typeHints));
    if (type.empty() || type == "unknown") type = "text";
    return type;
}

std::optional<bool> ExprHelper::referencesColumn(
    const std::string& exprSql, const std::string& columnName) {
    if (exprSql.empty()) return false;
    ParseResult parsed;
    const Expr* expression = parseStoredExpression(exprSql, parsed);
    if (!expression) return std::nullopt;
    size_t count = 0;
    if (!countParsedColumnReferences(expression, columnName, count)) {
        // Unsupported AST shapes remain conservative for DDL dependency
        // checks: callers must not remove a possibly referenced column.
        return true;
    }
    return count != 0;
}

std::optional<std::string> ExprHelper::renameColumnReferences(
    const std::string& exprSql, const std::string& oldName,
    const std::string& newName) {
    if (exprSql.empty() || oldName == newName) return exprSql;

    const auto sourceTokens = lexExpressionSource(exprSql);
    if (!sourceTokens) return std::nullopt;
    const bool containsOldIdentifier = std::any_of(
        sourceTokens->begin(), sourceTokens->end(),
        [&](const SourceToken& token) {
            return token.kind == SourceTokenKind::Identifier &&
                   token.text == oldName;
        });
    if (!containsOldIdentifier) return exprSql;

    ParseResult parsed;
    const Expr* expression = parseStoredExpression(exprSql, parsed);
    if (!expression) return std::nullopt;
    size_t oldReferenceCount = 0;
    size_t existingNewReferenceCount = 0;
    if (!countParsedColumnReferences(
            expression, oldName, oldReferenceCount) ||
        !countParsedColumnReferences(
            expression, newName, existingNewReferenceCount)) {
        return std::nullopt;
    }
    if (oldReferenceCount == 0) return exprSql;

    const std::vector<size_t> referenceTokens =
        columnReferenceSourceTokens(*sourceTokens, oldName);
    if (referenceTokens.size() != oldReferenceCount) return std::nullopt;

    std::string rewritten;
    rewritten.reserve(exprSql.size() +
        oldReferenceCount *
            (newName.size() > oldName.size()
                 ? newName.size() - oldName.size() : 0));
    size_t cursor = 0;
    for (const size_t tokenIndex : referenceTokens) {
        const SourceToken& token = sourceTokens->at(tokenIndex);
        rewritten.append(exprSql, cursor, token.begin - cursor);
        rewritten += newName;
        cursor = token.end;
    }
    rewritten.append(exprSql, cursor, std::string::npos);

    ParseResult rewrittenParse;
    const Expr* rewrittenExpression =
        parseStoredExpression(rewritten, rewrittenParse);
    if (!rewrittenExpression) return std::nullopt;
    size_t remainingOldReferences = 0;
    size_t rewrittenNewReferences = 0;
    if (!countParsedColumnReferences(
            rewrittenExpression, oldName, remainingOldReferences) ||
        !countParsedColumnReferences(
            rewrittenExpression, newName, rewrittenNewReferences) ||
        remainingOldReferences != 0 ||
        rewrittenNewReferences !=
            existingNewReferenceCount + oldReferenceCount) {
        return std::nullopt;
    }
    return rewritten;
}

static ExprEvalResult evalStringImpl(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    const std::set<std::string>* nullColumns,
    const std::string& currentDB,
    const std::string& currentUser) {

    std::string extractFixed;
    // extract(field FROM expr) -> date_part('field', expr): PG-equivalent,
    // and the parser has no special EXTRACT grammar.
    {
        std::string low;
        low.reserve(exprSql.size());
        bool inS = false;
        for (char c : exprSql) {
            if (c == 39) inS = !inS;
            low += (inS ? c : static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
        size_t ep = low.find("extract(");
        (void)ep;
        if (ep != std::string::npos) {
            std::string out;
            out.reserve(exprSql.size());
            size_t i = 0;
            bool q = false;
            while (i < exprSql.size()) {
                bool hit = false;
                if (exprSql[i] == 39) q = !q;
                if (!q && exprSql.size() >= i + 8 &&
                    low.compare(i, 8, "extract(") == 0) {
                    size_t lp = i + 7;
                    int depth = 0; size_t rp = std::string::npos;
                    for (size_t k = lp; k < exprSql.size(); ++k) {
                        if (exprSql[k] == '(') ++depth;
                        else if (exprSql[k] == ')') { --depth; if (depth == 0) { rp = k; break; } }
                    }
                    if (rp != std::string::npos) {
                        std::string inner = exprSql.substr(lp + 1, rp - lp - 1);
                        std::string innerLow;
                        bool q2 = false;
                        for (char c : inner) {
                            if (c == 39) q2 = !q2;
                            innerLow += (q2 ? c : static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
                        }
                        size_t fp = innerLow.find(" from ");
                        if (fp != std::string::npos) {
                            auto trimS = [](const std::string& s2) {
                                size_t a2 = s2.find_first_not_of(" 	");
                                size_t b2 = s2.find_last_not_of(" 	");
                                return (a2 == std::string::npos) ? std::string() : s2.substr(a2, b2 - a2 + 1);
                            };
                            std::string field = trimS(inner.substr(0, fp));
                            std::string src = trimS(inner.substr(fp + 6));
                            out += "date_part(" + std::string(1, 39) + field + std::string(1, 39) + ", " + src + ")";
                            i = rp + 1;
                            hit = true;
                        }
                    }
                }
                if (!hit) out += exprSql[i++];
            }
            extractFixed = out;
        }
    }


    // Unwrap typed literals before parsing: date '2026-08-15' -> '2026-08-15'.
    std::string sql = extractFixed.empty() ? exprSql : extractFixed;
    {
        static const std::string kws[] = {"date ", "timestamp ", "timestamptz ", "interval ", "boolean ", "time ", "numeric ", "int ", "text "};
        std::string out;
        out.reserve(sql.size());
        for (size_t i = 0; i < sql.size();) {
            bool matched = false;
            for (const auto& kw : kws) {
                bool keywordMatch = sql.size() >= i + kw.size();
                for (size_t k = 0; keywordMatch && k < kw.size(); ++k) {
                    keywordMatch = std::tolower(
                        static_cast<unsigned char>(sql[i + k])) == kw[k];
                }
                if (sql.size() >= i + kw.size() + 2 &&
                    keywordMatch &&
                    sql[i + kw.size()] == 39) {
                    if (i == 0 || !isalnum((unsigned char)sql[i - 1])) {
                        size_t close = sql.find(39, i + kw.size() + 1);
                        if (close != std::string::npos) {
                            out += 39;
                            out += sql.substr(i + kw.size() + 1, close - i - kw.size() - 1);
                            out += 39;
                            // Keep the declared type observable downstream:
                            // date '...' becomes '...'::date so arithmetic
                            // sees a date-typed operand (PG semantics),
                            // instead of a bare unknown-type string.
                            std::string tkw = kw;
                            while (!tkw.empty() && tkw.back() == ' ') tkw.pop_back();
                            out += "::" + tkw;
                            i = close + 1;
                            matched = true;
                            break;
                        }
                    }
                }
            }
            if (!matched) out += sql[i++];
        }
        sql = out;
    }

    ExprEvalResult res;
    if (exprSql.empty()) {
        res.error = "empty expression";
        return res;
    }

    // Parse the expression by wrapping it in a SELECT.
    SQLParser parser;
    ParseResult pr = parser.parse("SELECT " + sql);
    if (!pr.success || !pr.stmt) {
        res.error = pr.error.empty() ? "failed to parse expression" : pr.error;
        return res;
    }

    auto* select = dynamic_cast<SelectStmt*>(pr.stmt.get());
    if (!select || select->selectList.empty() || !select->selectList[0].expr) {
        res.error = "expression did not parse to a SELECT item";
        return res;
    }

    // Build row context.
    RowContext ctx;
    for (const auto& [name, value] : row) {
        std::string typeName = inferType(value);
        auto it = typeHints.find(name);
        if (it != typeHints.end() && !it->second.empty()) {
            typeName = canonicalTypeName(it->second);
        }
        const bool isNull = nullColumns
            ? nullColumns->count(name) != 0
            : value.empty();
        ctx.set(name, ExprValue(typeName, value, isNull));
    }
    // PostgreSQL exposes these as special session-value expressions. The
    // parser accepts both the function-like AST form and the bare identifier
    // form, so seed the context as well as registering evaluator functions.
    if (!currentUser.empty()) {
        const ExprValue userValue("name", currentUser, false);
        ctx.set("current_user", userValue);
        ctx.set("session_user", userValue);
    }
    // SQL-standard date/time special values, resolvable as bare identifiers
    // (PG exposes current_date/current_timestamp/localtimestamp both ways).
    // This layer currently uses UTC as its session timezone.
    {
        const std::time_t clock = std::time(nullptr);
        const std::string date = formatUtcClock(clock, "%Y-%m-%d");
        const std::string timestamp =
            formatUtcClock(clock, "%Y-%m-%d %H:%M:%S");
        ctx.set("current_date", ExprValue("date", date, date.empty()));
        ctx.set("current_timestamp",
                ExprValue("timestamptz", timestamp + "+00",
                          timestamp.empty()));
        ctx.set("localtimestamp",
                ExprValue("timestamp", timestamp, timestamp.empty()));
    }

    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    evaluator.registerFunction("current_user", [currentUser](const std::vector<ExprValue>&) {
        return ExprValue("name", currentUser, currentUser.empty());
    }, 's');
    evaluator.registerFunction("session_user", [currentUser](const std::vector<ExprValue>&) {
        return ExprValue("name", currentUser, currentUser.empty());
    }, 's');
    ExprValue v;
    try {
        v = evaluator.eval(select->selectList[0].expr.get(), ctx);
    } catch (const std::exception& e) {
        // Runtime expression errors (division by zero, invalid cast input)
        // surface as evaluation failures carrying the engine message.
        res.error = e.what();
        return res;
    }

    if (v.isUnknown()) {
        res.error = "expression evaluated to an unsupported value";
        return res;
    }

    res.ok = true;
    res.isNull = v.isNull;
    res.value = v.value;
    res.typeName = v.typeName;
    return res;
}

ExprEvalResult ExprHelper::evalString(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    const std::string& currentUser) {
    return evalStringImpl(
        exprSql, row, typeHints, nullptr, currentDB, currentUser);
}

ExprEvalResult ExprHelper::evalStringWithNulls(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::set<std::string>& nullColumns,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    const std::string& currentUser) {
    return evalStringImpl(
        exprSql, row, typeHints, &nullColumns, currentDB, currentUser);
}

bool ExprHelper::evalBool(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    std::string* error,
    const std::string& currentDB,
    const std::string& currentUser) {

    ExprEvalResult r = evalString(exprSql, row, typeHints, currentDB, currentUser);
    if (!r.ok) {
        if (error) *error = r.error;
        return false;
    }
    if (r.isNull) return false;

    // ExprValue::asBool treats "t"/"true"/"1" as true; otherwise truthy.
    ExprValue tmp("boolean", r.value, false);
    return tmp.asBool();
}

bool ExprHelper::evalCheck(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    std::string* error,
    const std::string& currentDB,
    const std::string& currentUser) {

    ExprEvalResult r = evalString(
        exprSql, row, typeHints, currentDB, currentUser);
    if (!r.ok) {
        if (error) *error = r.error;
        return false;
    }
    if (r.isNull) return true;

    ExprValue tmp("boolean", r.value, false);
    return tmp.asBool();
}

} // namespace dbms
