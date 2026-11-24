#pragma once

#include "catalog/PgConstNode.h"
#include "parser/parser.h"

namespace dbms::index_predicate {

[[noreturn]] inline void unsupported() {
    throw DbError("0A000", "index predicate expression is not supported in the catalog");
}

struct OperatorIds {
    Oid op, function;
};

// Built-in operator and function identities from PostgreSQL 18's pg_operator
// and pg_proc declarations. Positions are =, <>, <, <=, >, >=.
inline constexpr std::string_view operatorNames[]{"=", "<>", "<", "<=", ">", ">="};
inline constexpr struct {
    Oid left, right;
    OperatorIds ids[6];
} operators[]{
    {16,16,{{91,60},{85,84}}},
    {20,20,{{410,467},{411,468},{412,469},{414,471},{413,470},{415,472}}},
    {20,21,{{1868,1856},{1869,1857},{1870,1858},{1872,1860},{1871,1859},{1873,1861}}},
    {20,23,{{416,474},{417,475},{418,476},{420,478},{419,477},{430,479}}},
    {21,20,{{1862,1850},{1863,1851},{1864,1852},{1866,1854},{1865,1853},{1867,1855}}},
    {21,21,{{94,63},{519,145},{95,64},{522,148},{520,146},{524,151}}},
    {21,23,{{532,158},{538,164},{534,160},{540,166},{536,162},{542,168}}},
    {23,20,{{15,852},{36,853},{37,854},{80,856},{76,855},{82,857}}},
    {23,21,{{533,159},{539,165},{535,161},{541,167},{537,163},{543,169}}},
    {23,23,{{96,65},{518,144},{97,66},{523,149},{521,147},{525,150}}},
};

struct Node {
    Oid type;
    std::string text;
    int precedence = 5;
};

inline std::optional<int64_t> integerLiteral(const Expr* expr) {
    const auto* literal = dynamic_cast<const LiteralExpr*>(expr);
    std::string text;
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr);
        unary && (unary->op == "+" || unary->op == "-")) {
        literal = dynamic_cast<const LiteralExpr*>(unary->operand.get());
        if (literal && unary->op == "-") text = "-";
    }
    if (!literal || literal->preparedSubquery || !literal->typeName.empty()) return {};
    text += literal->value;
    if (text.empty()) return {};
    int64_t value;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) return {};
    return value;
}

inline Node encode(const Expr* expr, const std::vector<PgAttributeRow>& attributes, size_t depth = 0) {
    if (!expr || expr->preparedSubquery || depth >= 128) unsupported();
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(expr)) {
        if (!column->schema.empty() || !column->table.empty()) unsupported();
        const auto it = std::find_if(attributes.begin(), attributes.end(), [&](const auto& field) {
            return field.attnum > 0 && !field.attisdropped && field.attname == column->column;
        });
        if (it == attributes.end()) unsupported();
        const auto num = std::to_string(it->attnum);
        return {it->atttypid, "{VAR :varno 1 :varattno " + num + " :vartype " + std::to_string(it->atttypid) +
            " :vartypmod " + std::to_string(it->atttypmod) + " :varcollid " + std::to_string(it->attcollation) +
            " :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn " + num + " :location -1}"};
    }
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expr)) {
        const auto keyword = SQLParser::toLower(literal->value);
        if (literal->typeName.empty() && (keyword == "true" || keyword == "false"))
            return {16, pg_const_node::encode({16, -1, 0, 1, true, pg_const_node::datum(keyword == "true")})};
    }
    if (const auto value = integerLiteral(expr)) {
        const bool fitsInt = *value >= INT32_MIN && *value <= INT32_MAX;
        return {fitsInt ? 23U : 20U, pg_const_node::encode({fitsInt ? 23U : 20U, -1, 0,
            static_cast<int16_t>(fitsInt ? 4 : 8), true, pg_const_node::datum(static_cast<uintptr_t>(*value))})};
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        const auto op = SQLParser::toLower(unary->op);
        const auto arg = encode(unary->operand.get(), attributes, depth + 1);
        if (op == "not" && arg.type == 16)
            return {16, "{BOOLEXPR :boolop not :args (" + arg.text + ") :location -1}"};
        if (op == "is null" || op == "is not null")
            return {16, "{NULLTEST :arg " + arg.text + " :nulltesttype " +
                (op == "is null" ? "0" : "1") + " :argisrow false :location -1}"};
        unsupported();
    }
    const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr);
    if (!binary) unsupported();
    auto op = SQLParser::toLower(binary->op);
    if (op == "!=") op = "<>";
    const auto left = encode(binary->left.get(), attributes, depth + 1);
    const auto right = encode(binary->right.get(), attributes, depth + 1);
    const auto args = left.text + ' ' + right.text;
    if ((op == "and" || op == "or") && left.type == 16 && right.type == 16)
        return {16, "{BOOLEXPR :boolop " + op + " :args (" + args + ") :location -1}"};
    for (const auto& signature : operators) if (signature.left == left.type && signature.right == right.type)
        for (size_t i = 0; i < std::size(operatorNames); ++i) if (op == operatorNames[i] && signature.ids[i].op)
            return {16, "{OPEXPR :opno " + std::to_string(signature.ids[i].op) + " :opfuncid " +
                std::to_string(signature.ids[i].function) +
                " :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args (" + args + ") :location -1}"};
    unsupported();
}

inline std::string compile(const std::string& sql, const std::vector<PgAttributeRow>& attributes) {
    if (sql.empty()) return {};
    SQLParser parser;
    auto parsed = parser.parseForBinding("SELECT " + sql);
    const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
    if (!select || select->selectList.size() != 1 || !select->selectList.front().alias.empty() || select->fromClause ||
        select->whereClause || !select->groupBy.empty() || select->having || !select->orderBy.empty() ||
        select->hasRowCountClause || select->setOp != SetOp::None)
        unsupported();
    const auto* expr = select->selectList.front().expr.get();
    const auto* literal = dynamic_cast<const LiteralExpr*>(expr);
    if (literal && literal->typeName.empty() && SQLParser::toLower(literal->value) == "true") return {};
    auto node = encode(expr, attributes);
    if (node.type != 16) unsupported();
    return std::move(node.text);
}

class Reader final : private pg_const_node::Reader {
    const std::vector<PgAttributeRow>& attributes_;
    bool pretty_;

    bool starts(std::string_view prefix) const { return remaining_.substr(0, prefix.size()) == prefix; }

    static std::string operand(const Node& node, int precedence) {
        return node.precedence < precedence ? '(' + node.text + ')' : node.text;
    }

    Node wrap(std::string sql, int precedence) const {
        return {16, pretty_ ? std::move(sql) : '(' + sql + ')', pretty_ ? precedence : 5};
    }

    void location() {
        expect(" :location ");
        (void)number<int32_t>();
        expect("}");
    }

    Node node(size_t depth) {
        if (depth >= 128) unsupported();
        if (starts("{VAR ")) {
            expect("{VAR :varno 1 :varattno ");
            const auto attnum = number<int16_t>();
            expect(" :vartype ");
            const auto type = number<Oid>();
            expect(" :vartypmod ");
            const auto typmod = number<int32_t>();
            expect(" :varcollid ");
            const auto coll = number<Oid>();
            expect(" :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn ");
            if (number<int16_t>() != attnum) invalid();
            location();
            const auto field = std::find_if(attributes_.begin(), attributes_.end(), [&](const auto& attr) {
                return attnum > 0 && attr.attnum == attnum && !attr.attisdropped;
            });
            if (field == attributes_.end() || field->atttypid != type || field->atttypmod != typmod ||
                field->attcollation != coll)
                throw DbError("22023", "index predicate column does not match the relation");
            return {type, SQLParser::quoteIdentifier(field->attname)};
        }
        if (starts("{CONST ")) {
            const auto value = readConstant();
            const auto width = value.type == 16 ? 1 : value.type == 21 ? 2 : value.type == 23 ? 4 : value.type == 20 ? 8 : 0;
            if (!width || !value.bytes || value.length != width || !value.byValue || value.typmod != -1 || value.collation)
                unsupported();
            const auto bits = pg_const_node::read<uintptr_t>(*value.bytes);
            if (value.type == 16) {
                if (bits > 1) invalid();
                return {16, bits ? "true" : "false"};
            }
            const auto integer = value.type == 21 ? static_cast<int16_t>(bits) :
                value.type == 23 ? static_cast<int32_t>(bits) : static_cast<int64_t>(bits);
            const auto text = std::to_string(integer);
            if (value.type == 23 && integer >= 0) return {23, text};
            if (value.type == 20 && integer > INT32_MAX) return {20, text};
            return {value.type, "'" + text + "'::" +
                (value.type == 21 ? "smallint" : value.type == 23 ? "integer" : "bigint")};
        }
        if (starts("{NULLTEST ")) {
            expect("{NULLTEST :arg ");
            const auto arg = node(depth + 1);
            expect(" :nulltesttype ");
            const auto kind = number<int>();
            if (kind != 0 && kind != 1) invalid();
            expect(" :argisrow false");
            location();
            return wrap(operand(arg, 5) + (kind ? " IS NOT NULL" : " IS NULL"), 4);
        }
        if (starts("{OPEXPR ")) {
            expect("{OPEXPR :opno ");
            const auto op = number<Oid>();
            expect(" :opfuncid ");
            const auto function = number<Oid>();
            expect(" :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args (");
            const auto left = node(depth + 1);
            expect(" ");
            const auto right = node(depth + 1);
            expect(")");
            location();
            for (const auto& signature : operators) if (signature.left == left.type && signature.right == right.type)
                for (size_t i = 0; i < std::size(operatorNames); ++i)
                    if (op && op == signature.ids[i].op && function == signature.ids[i].function)
                        return wrap(operand(left, 5) + ' ' + std::string(operatorNames[i]) + ' ' + operand(right, 5), 4);
            unsupported();
        }
        if (!starts("{BOOLEXPR ")) unsupported();
        expect("{BOOLEXPR :boolop ");
        const auto kind = starts("and") ? "and" : starts("or") ? "or" : "not";
        expect(kind);
        expect(" :args (");
        auto first = node(depth + 1);
        if (first.type != 16) invalid();
        if (std::string_view(kind) == "not") {
            expect(")");
            location();
            return wrap("NOT " + operand(first, 3), 3);
        }
        const bool isAnd = std::string_view(kind) == "and";
        const int precedence = isAnd ? 2 : 1;
        std::string sql = operand(first, precedence);
        do {
            expect(" ");
            const auto next = node(depth + 1);
            if (next.type != 16) invalid();
            sql += (isAnd ? " AND " : " OR ") + operand(next, precedence);
        } while (!starts(")"));
        expect(")");
        location();
        return wrap(std::move(sql), precedence);
    }

public:
    Reader(std::string_view text, const std::vector<PgAttributeRow>& attributes, bool pretty)
        : pg_const_node::Reader(text), attributes_(attributes), pretty_(pretty) {}

    std::string expression() {
        auto value = node(0);
        if (!remaining_.empty() || value.type != 16) invalid();
        return std::move(value.text);
    }
};

} // namespace dbms::index_predicate
