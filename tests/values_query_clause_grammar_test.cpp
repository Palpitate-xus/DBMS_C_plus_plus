#include "parser/parser.h"

#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace dbms;
size_t checked = 0, failures = 0;
void check(bool good, const std::string& label) {
    ++checked; failures += !good;
    if (!good) std::cout << "VALUES_QUERY_CLAUSE_GRAMMAR_FAILED " << label << '\n';
}
ParseResult parse(const std::string& sql, bool binding) {
    try { SQLParser parser; return binding ? parser.parseForBinding(sql) : parser.parse(sql); }
    catch (const std::exception& error) { ParseResult result; result.error = error.what(); return result; }
}
const SelectStmt* query(const Stmt* statement) { return dynamic_cast<const SelectStmt*>(statement); }
std::string expression(const Expr* value) {
    if (!value) return "missing";
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(value)) return "L:" + literal->value;
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(value)) return "C:" + column->toString();
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(value))
        return "B:" + binary->op + "(" + expression(binary->left.get()) + "," + expression(binary->right.get()) + ")";
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(value)) {
        std::string result = "F:" + call->funcName + "(";
        for (size_t i = 0; i < call->args.size(); ++i) result += (i ? "," : "") + expression(call->args[i].get());
        return result + ")";
    }
    const auto list = [&](const auto& fields, const char* prefix) {
        std::string result = prefix;
        for (size_t i = 0; i < fields.size(); ++i) result += (i ? "," : "") + expression(fields[i].get());
        return result + "]";
    };
    if (const auto* array = dynamic_cast<const ArrayExpr*>(value)) return list(array->elements, "A:[");
    if (const auto* row = dynamic_cast<const RowExpr*>(value)) return row->constructor ? list(row->elements, "R:[") : "list-row";
    return "unexpected-expression";
}
using Rows = std::vector<std::vector<std::string>>;
bool rows(const SelectStmt* value, const Rows& wanted) {
    if (!value || value->command != SqlCommand::Values || !value->selectList.empty() || value->fromClause ||
        value->valuesRows.size() != wanted.size()) return false;
    for (size_t i = 0; i < wanted.size(); ++i) {
        if (value->valuesRows[i].size() != wanted[i].size()) return false;
        for (size_t j = 0; j < wanted[i].size(); ++j)
            if (expression(value->valuesRows[i][j].get()) != wanted[i][j]) return false;
    }
    return true;
}
struct Key { std::string value; bool ascending = true, first = false; };
bool keys(const SelectStmt* value, const std::vector<Key>& wanted) {
    if (!value || value->orderBy.size() != wanted.size()) return false;
    for (size_t i = 0; i < wanted.size(); ++i) {
        const auto& actual = value->orderBy[i];
        if (expression(actual.expr.get()) != wanted[i].value || actual.asc != wanted[i].ascending ||
            actual.nullsFirst != wanted[i].first || !actual.usingOp.empty() || actual.enumComparison) return false;
    }
    return true;
}
bool counts(const SelectStmt* value, std::optional<size_t> limit = {}, std::optional<size_t> offset = {}) {
    return value && value->limit == limit && value->offset == offset;
}
bool fetch(const SelectStmt* value, bool present = false, std::optional<int64_t> count = {}, bool ties = false) {
    return value && value->fetchFirst == present && value->signedFetchCount == count && value->withTies == ties;
}
struct Valid {
    std::string sql;
    std::vector<Key> order;
    std::optional<size_t> limit = {}, offset = {};
    bool fetched = false;
    std::optional<int64_t> signedCount = {};
    bool ties = false;
    size_t ctes = 0;
    bool nested = false;
};
// The same source remains compilable with the frozen old headers. Absence
// of the real count AST is an observed failure, never a synthesized node.
template<class T, class = void> struct CountTrees {
    static const Expr* limit(const T*) { return nullptr; }
    static const Expr* offset(const T*) { return nullptr; }
    static bool role(const T*) { return false; }
};
template<class T> struct CountTrees<T, std::void_t<decltype(std::declval<T>().limitExpr),
        decltype(std::declval<T>().offsetExpr), decltype(std::declval<T>().hasRowCountClause)>> {
    static const Expr* limit(const T* value) { return value ? value->limitExpr.get() : nullptr; }
    static const Expr* offset(const T* value) { return value ? value->offsetExpr.get() : nullptr; }
    static bool role(const T* value) { return value && value->hasRowCountClause; }
};
std::string countShape(const Expr* value) {
    if (!value) return "missing";
    if (const auto* parameter = dynamic_cast<const ParameterExpr*>(value))
        return "P:" + std::to_string(parameter->slot+1) +
            (parameter->origin == ParameterOrigin::StatementInput ? ":input" : ":wrong-owner");
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(value))
        return "U:" + unary->op + "(" + countShape(unary->operand.get()) + ")";
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(value))
        return "B:" + binary->op + "(" + countShape(binary->left.get()) + "," + countShape(binary->right.get()) + ")";
    if (const auto* cast = dynamic_cast<const CastExpr*>(value))
        return "CAST:" + cast->typeName + "(" + countShape(cast->operand.get()) + ")";
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(value)) {
        std::string result = "F:" + call->funcName + "(";
        for (size_t i = 0; i < call->args.size(); ++i) result += (i ? "," : "") + countShape(call->args[i].get());
        return result + ")";
    }
    if (const auto* conditional = dynamic_cast<const CaseExpr*>(value)) {
        std::string result = "CASE(" + countShape(conditional->switchExpr.get());
        for (const auto& branch : conditional->whenClauses)
            result += ";" + countShape(branch.first.get()) + "=>" + countShape(branch.second.get());
        return result + ";" + countShape(conditional->elseExpr.get()) + ")";
    }
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(value)) {
        if (literal->value.size() >= 2 && literal->value.front() == '(' && literal->value.back() == ')') {
            const auto tokens = SQLParser::tokenize(literal->value.substr(1,literal->value.size()-2));
            if (!tokens.empty() && (SQLParser::toLower(tokens[0]) == "select" ||
                    SQLParser::toLower(tokens[0]) == "values" || SQLParser::toLower(tokens[0]) == "with")) {
                if (literal->preparedSubquery || !literal->typeName.empty()) return "wrong-query-owner";
                std::string result = "Q:";
                for (size_t i = 0; i < tokens.size(); ++i) result += (i ? " " : "") + SQLParser::toLower(tokens[i]);
                return result;
            }
        }
    }
    return expression(value);
}
bool countSpan(const Expr* value, const std::string& sql, const std::string& clause,
               const std::string& bytes, bool binding) {
    if (!value) return bytes.empty();
    if (bytes.empty()) return false;
    if (!binding) return value->sourceBegin == std::string::npos && value->sourceEnd == std::string::npos;
    const auto clauseStart = sql.find(" " + clause + " ");
    const auto begin = clauseStart == std::string::npos ? std::string::npos : sql.find(bytes,clauseStart+clause.size()+2);
    return begin != std::string::npos && value->sourceBegin == begin && value->sourceEnd == begin+bytes.size() &&
        sql.substr(value->sourceBegin,value->sourceEnd-value->sourceBegin) == bytes;
}
}

int main() {
    const std::string base = "VALUES(3,'c'),(1,'a'),(2,'b')";
    const Rows regular{{"L:3","L:'c'"},{"L:1","L:'a'"},{"L:2","L:'b'"}};
    const std::vector<Valid> valid{
        {base,{}},
        {base + " ORDER BY 1",{{"L:1"}}},
        {base + " ORDER BY 2 DESC NULLS LAST,1 ASC NULLS FIRST",{{"L:2",false,false},{"L:1",true,true}}},
        {base + " LIMIT 2",{},2},
        {base + " OFFSET 1",{},{},1},
        {base + " OFFSET 1 ROWS LIMIT 2",{},2,1},
        {base + " LIMIT ALL OFFSET 1 ROW",{},{},1},
        {base + " ORDER BY column1 DESC LIMIT 2 OFFSET 1",{{"C:column1",false,true}},2,1},
        {base + " ORDER BY column1+1 DESC",{{"B:+(C:column1,L:1)",false,true}}},
        {base + " ORDER BY abs(column1),column2 DESC",{{"F:abs(C:column1)"},{"C:column2",false,true}}},
        {base + " ORDER BY 1 FETCH FIRST ROW ONLY",{{"L:1"}},1,{},true,1},
        {base + " ORDER BY 1 FETCH NEXT 2 ROWS ONLY",{{"L:1"}},2,{},true,2},
        {base + " ORDER BY 1 OFFSET 1 ROW FETCH NEXT 1 ROWS ONLY",{{"L:1"}},1,1,true,1},
        {base + " ORDER BY 1 FETCH FIRST -1 ROWS ONLY",{{"L:1"}},{},{},true,-1},
        {base + " ORDER BY 1 FETCH NEXT -9223372036854775808 ROWS ONLY",{{"L:1"}},{},{},true,std::numeric_limits<int64_t>::min()},
        {base + " ORDER BY 1 FETCH FIRST 0 ROWS WITH TIES",{{"L:1"}},0,{},true,0,true},
        {base + " ORDER BY 2 DESC NULLS FIRST FETCH FIRST 2 ROWS WITH TIES",{{"L:2",false,true}},2,{},true,2,true},
        {"((" + base + ")) ORDER BY 1 DESC LIMIT 1",{{"L:1",false,true}},1},
        {"WITH c AS(SELECT 1) " + base + " ORDER BY 1 OFFSET 1 FETCH FIRST 1 ROW ONLY",{{"L:1"}},1,1,true,1,false,1},
        {"VALUES(ARRAY[3,1],ROW(2,'v')),(ARRAY[1,2],ROW(3,'q')) ORDER BY 1",{{"L:1"}},{},{},false,{},false,0,true}
    };
    const Rows nested{{"A:[L:3,L:1]","R:[L:2,L:'v']"},{"A:[L:1,L:2]","R:[L:3,L:'q']"}};
    for (const auto& item : valid) for (const bool binding : {false,true}) {
        const auto label = std::to_string(binding) + ":" + item.sql;
        auto parsed = parse(item.sql,binding); const auto* root = query(parsed.stmt.get());
        check(parsed.isValid(),label + ":accepted " + parsed.error);
        check(rows(root,item.nested ? nested : regular),label + ":original VALUES cells and roles");
        check(keys(root,item.order),label + ":original key tree/direction/NULL flags");
        check(counts(root,item.limit,item.offset),label + ":exact LIMIT/OFFSET including ALL absence");
        check(fetch(root,item.fetched,item.signedCount,item.ties),label + ":exact FETCH role/count/TIES");
        check(root && root->setOp == SetOp::None && !root->setOpLhs && !root->setOpRhs &&
            root->ctes.size() == item.ctes && (!item.ctes || (query(root->ctes[0].query.get()) &&
                root->ctes[0].query.get() != root)),label + ":independent query and CTE owners");
    }

    const std::vector<std::string> invalid{
        "VALUES", "VALUES()", "VALUES(,1)", "VALUES(1 AS x)", "VALUES(1", "VALUES(1),", "VALUES(1,)", "VALUES(1),(2,3)",
        "VALUES(1) ORDER 1", "VALUES(1) ORDER BY", "VALUES(1) ORDER BY 1,", "VALUES(1) ORDER BY 1 NULLS",
        "VALUES(1) ORDER BY 1 NULLS MIDDLE", "VALUES(1) ORDER BY 1 ORDER BY 1", "VALUES(1), ORDER BY 1",
        "VALUES(1) LIMIT", "VALUES(1) LIMIT ALL LIMIT 1", "VALUES(1) LIMIT 2 FETCH FIRST 1 ROW ONLY",
        "VALUES(1) OFFSET", "VALUES(1) OFFSET 1 OFFSET 2", "VALUES(1) FETCH FIRST", "VALUES(1) FETCH FIRST 2 ONLY",
        "VALUES(1) FETCH FIRST 1 ROW", "VALUES(1) FETCH FIRST 1 ROW WITH TIES"
    };
    for (const auto& sql : invalid) for (const bool binding : {false,true}) {
        const auto parsed = parse(sql,binding);
        check(!parsed.isValid() && !parsed.error.empty(),std::to_string(binding) + ":strict rejection " + sql);
    }

    const std::vector<std::string> scopes{
        "(VALUES(4),(2),(1) ORDER BY 1 LIMIT 2) UNION ALL VALUES(3),(0) ORDER BY 1 DESC",
        "VALUES(9),(8) UNION ALL (VALUES(3),(2),(1) ORDER BY 1 OFFSET 1 LIMIT 1) ORDER BY 1 LIMIT 2",
        "(VALUES(9),(8) ORDER BY 1 LIMIT 1) UNION ALL (VALUES(3),(2) ORDER BY 1 DESC LIMIT 1) ORDER BY 1 OFFSET 1",
        "WITH v AS(VALUES(4),(2) ORDER BY 1 LIMIT 1) VALUES(3),(1) ORDER BY 1 DESC",
        "SELECT * FROM(VALUES(3),(1),(2) ORDER BY column1 DESC LIMIT 2) AS q(n) ORDER BY n"
    };
    for (size_t index = 0; index < scopes.size(); ++index) for (const bool binding : {false,true}) {
        auto parsed = parse(scopes[index],binding); const auto* root = query(parsed.stmt.get());
        const auto label = std::to_string(binding) + ":scope " + std::to_string(index);
        check(parsed.isValid(),label + ":accepted " + parsed.error);
        if (index < 3) {
            const auto* left = root ? query(root->setOpLhs.get()) : nullptr;
            const auto* right = root ? query(root->setOpRhs.get()) : nullptr;
            check(root && root->setOp == SetOp::Union && root->setOpAll && left && right && left != right &&
                left != root && right != root && root->valuesRows.empty(),label + ":actual independent set operands");
            check(keys(root,{{"L:1",index != 0,index == 0}}) && counts(root,index == 1 ? std::optional<size_t>(2) : std::nullopt,
                index == 2 ? std::optional<size_t>(1) : std::nullopt),label + ":global clauses stay at set root");
            bool local = false;
            if (index == 0) local = rows(left,{{"L:4"},{"L:2"},{"L:1"}}) && keys(left,{{"L:1"}}) && counts(left,2) &&
                rows(right,{{"L:3"},{"L:0"}}) && keys(right,{}) && counts(right);
            if (index == 1) local = rows(left,{{"L:9"},{"L:8"}}) && keys(left,{}) && counts(left) &&
                rows(right,{{"L:3"},{"L:2"},{"L:1"}}) && keys(right,{{"L:1"}}) && counts(right,1,1);
            if (index == 2) local = rows(left,{{"L:9"},{"L:8"}}) && keys(left,{{"L:1"}}) && counts(left,1) &&
                rows(right,{{"L:3"},{"L:2"}}) && keys(right,{{"L:1",false,true}}) && counts(right,1);
            check(local,label + ":local row cells/order/counts stay with each operand");
        } else if (index == 3) {
            const auto* child = root && root->ctes.size() == 1 ? query(root->ctes[0].query.get()) : nullptr;
            check(rows(root,{{"L:3"},{"L:1"}}) && keys(root,{{"L:1",false,true}}) && counts(root),label + ":root VALUES clauses");
            check(rows(child,{{"L:4"},{"L:2"}}) && keys(child,{{"L:1"}}) && counts(child,1),label + ":CTE local clauses");
            check(child && root && child != root && root->setOp == SetOp::None && child->setOp == SetOp::None,label + ":separate CTE owner");
        } else {
            const auto* source = root ? root->fromClause.get() : nullptr;
            const auto* child = source ? query(source->subquery.get()) : nullptr;
            check(root && root->command == SqlCommand::Select && keys(root,{{"C:n"}}) && counts(root),label + ":parent SELECT owns its key");
            check(rows(child,{{"L:3"},{"L:1"},{"L:2"}}) && keys(child,{{"C:column1",false,true}}) && counts(child,2),label + ":derived VALUES local clauses");
            check(source && source->type == FromItem::Type::Subquery && child && child != root && source->alias == "q" &&
                source->columnAliases == std::vector<std::string>{"n"},label + ":real derived child and alias");
        }
    }

    const std::vector<std::string> provenance{
        "/* lead */ VALUES(47),(3) ORDER BY column1+1 DESC LIMIT 1",
        "WITH c AS(SELECT 9) VALUES(47),(3) ORDER BY column1+1 LIMIT 1",
        "(VALUES(47),(3) ORDER BY column1+1 LIMIT 1) UNION ALL VALUES(0) ORDER BY 1 DESC",
        "SELECT * FROM(VALUES(47),(3) ORDER BY column1+1 LIMIT 1) AS q"
    };
    for (size_t index = 0; index < provenance.size(); ++index) {
        const auto& sql = provenance[index]; auto parsed = parse(sql,true); const auto* root = query(parsed.stmt.get());
        const auto* target = index == 2 ? (root ? query(root->setOpLhs.get()) : nullptr) :
            index == 3 ? (root && root->fromClause ? query(root->fromClause->subquery.get()) : nullptr) : root;
        const Expr* cell = target && !target->valuesRows.empty() && !target->valuesRows[0].empty() ? target->valuesRows[0][0].get() : nullptr;
        const Expr* key = target && target->orderBy.size() == 1 ? target->orderBy[0].expr.get() : nullptr;
        const auto label = "source " + std::to_string(index);
        check(parsed.isValid() && target && target->command == SqlCommand::Values,label + ":actual VALUES owner");
        check(cell && cell->sourceBegin == sql.find("47") && cell->sourceEnd == sql.find("47") + 2,label + ":original value byte span");
        check(key && expression(key) == "B:+(C:column1,L:1)" && key->sourceBegin == sql.find("column1+1") &&
            key->sourceEnd == sql.find("column1+1") + std::string("column1+1").size(),label + ":original ORDER expression byte span");
    }
    // Append role controls after the original 340 observations. Their oracles
    // use legal syntax and actual clause ownership, so the same source still
    // compiles against the frozen old AST without reading a new flag.
    for (const std::string head : {"VALUES(1)","SELECT 1"})
        for (const std::string operation : {" UNION ALL "," EXCEPT "," INTERSECT "})
            for (const std::string tail : {" LIMIT ALL"," FETCH FIRST -1 ROW ONLY"})
                for (const bool binding : {false,true}) {
                    const auto sql = head + tail + operation + "VALUES(2)";
                    const auto parsed = parse(sql,binding);
                    check(!parsed.isValid() && !parsed.error.empty(),
                        std::to_string(binding) + ":bare local row-count clause rejected " + sql);
                }
    for (const std::string head : {"VALUES(1)","SELECT 1"})
        for (const bool negative : {false,true})
            for (const bool binding : {false,true}) {
                const auto sql = "(" + head + (negative ? " FETCH FIRST -1 ROW ONLY" : " LIMIT ALL") +
                    ") UNION ALL VALUES(2)";
                auto parsed = parse(sql,binding); const auto* root = query(parsed.stmt.get());
                const auto* left = root ? query(root->setOpLhs.get()) : nullptr;
                const auto* right = root ? query(root->setOpRhs.get()) : nullptr;
                const auto label = std::to_string(binding) + ":parenthesized row-count role " + sql;
                check(parsed.isValid(),label + ":accepted " + parsed.error);
                check(root && root->setOp == SetOp::Union && root->setOpAll && left && right &&
                    left != root && right != root && left != right && root->valuesRows.empty() &&
                    keys(root,{}) && counts(root) && fetch(root),label + ":outer set does not acquire local count");
                const bool body = head == "VALUES(1)" ? rows(left,{{"L:1"}}) :
                    left && left->command == SqlCommand::Select && left->selectList.size() == 1 &&
                    expression(left->selectList[0].expr.get()) == "L:1";
                check(body && counts(left) && keys(left,{}) &&
                    fetch(left,negative,negative ? std::optional<int64_t>(-1) : std::nullopt) &&
                    rows(right,{{"L:2"}}) && counts(right) && fetch(right),
                    label + ":local LIMIT ALL or signed FETCH remains on its real body");
            }
    for (const bool negative : {false,true}) for (const bool binding : {false,true}) {
        const auto sql = std::string("VALUES(1) UNION ALL VALUES(2)") +
            (negative ? " FETCH FIRST -1 ROW ONLY" : " LIMIT ALL");
        auto parsed = parse(sql,binding); const auto* root = query(parsed.stmt.get());
        const auto* left = root ? query(root->setOpLhs.get()) : nullptr;
        const auto* right = root ? query(root->setOpRhs.get()) : nullptr;
        const auto label = std::to_string(binding) + ":global row-count role " + sql;
        check(parsed.isValid(),label + ":accepted " + parsed.error);
        check(root && root->setOp == SetOp::Union && root->setOpAll && root->valuesRows.empty() &&
            keys(root,{}) && counts(root) &&
            fetch(root,negative,negative ? std::optional<int64_t>(-1) : std::nullopt),
            label + ":global LIMIT ALL or signed FETCH belongs to set");
        check(left && right && left != right && left != root && right != root &&
            rows(left,{{"L:1"}}) && rows(right,{{"L:2"}}) && counts(left) && counts(right) &&
            fetch(left) && fetch(right),label + ":branch bodies have no global row-count clause");
    }
    // NULL and signed VALUES counts extend the original 400 observations.
    // Negative VALUES inputs are valid grammar, with no executable unsigned
    // cap; the real runtime consumer owns their SQL error. Do not read the
    // appended AST fields, so the frozen old-header baseline remains valid.
    struct LiteralCount {
        std::string sql;
        std::optional<size_t> limit = {}, offset = {};
        bool fetched = false, ties = false, ordered = false, values = true;
    };
    const std::vector<LiteralCount> literalCounts{
        {base + " LIMIT NULL"},
        {base + " OFFSET NULL",{},0},
        {base + " LIMIT NULL OFFSET NULL",{},0},
        {base + " ORDER BY 1 FETCH FIRST NULL ROWS ONLY",{},{},true,false,true},
        {base + " ORDER BY 1 OFFSET NULL FETCH NEXT NULL ROWS ONLY",{},0,true,false,true},
        {base + " ORDER BY 1 FETCH FIRST NULL ROWS WITH TIES",{},{},true,true,true},
        {base + " LIMIT +1",1},
        {base + " LIMIT -0",0},
        {base + " OFFSET -0",{},0},
        {base + " LIMIT -1"},
        {base + " OFFSET -1"},
        {base + " LIMIT 1 OFFSET -2",1},
        {base + " LIMIT -3 OFFSET -2"},
        {base + " OFFSET NULL LIMIT +1",1,0},
        {base + " FETCH FIRST NULL ROW ONLY",{},{},true},
        {base + " ORDER BY 1 LIMIT NULL",{},{},false,false,true},
        {"SELECT 1 LIMIT NULL OFFSET NULL",{},0,false,false,false,false},
        {"SELECT 1 FETCH FIRST NULL ROWS ONLY",{},{},true,false,false,false},
        {"SELECT 1 ORDER BY 1 FETCH FIRST NULL ROW WITH TIES",{},{},true,true,true,false},
        {"SELECT 1 ORDER BY 1 OFFSET NULL FETCH NEXT NULL ROWS ONLY",{},0,true,false,true,false}
    };
    for (const auto& item : literalCounts) for (const bool binding : {false,true}) {
        auto parsed = parse(item.sql,binding); const auto* root = query(parsed.stmt.get());
        const auto label = std::to_string(binding) + ":literal count " + item.sql;
        const bool body = item.values ? rows(root,regular) :
            root && root->command == SqlCommand::Select && root->valuesRows.empty() && !root->fromClause &&
            root->selectList.size() == 1 && expression(root->selectList[0].expr.get()) == "L:1";
        check(parsed.isValid() && body,label + ":accepted with original body " + parsed.error);
        check(counts(root,item.limit,item.offset) &&
            keys(root,item.ordered ? std::vector<Key>{{"L:1"}} : std::vector<Key>{}),
            label + ":exact executable cap/OFFSET and original ORDER");
        check(fetch(root,item.fetched,{},item.ties) && root && root->setOp == SetOp::None &&
            !root->setOpLhs && !root->setOpRhs,label + ":NULL FETCH and signed LIMIT stay distinct");
    }
    for (const std::string sql : {
            "VALUES(1) LIMIT NULL UNION ALL VALUES(2)",
            "VALUES(1) OFFSET NULL UNION ALL VALUES(2)",
            "VALUES(1) FETCH FIRST NULL ROW ONLY UNION ALL VALUES(2)",
            "SELECT 1 LIMIT NULL UNION ALL SELECT 2"})
        for (const bool binding : {false,true}) {
            const auto parsed = parse(sql,binding);
            check(!parsed.isValid() && !parsed.error.empty(),
                std::to_string(binding) + ":NULL local clause still requires parentheses " + sql);
        }
    for (const bool binding : {false,true}) {
        const std::string sql = "VALUES(1) UNION ALL VALUES(2) LIMIT NULL OFFSET NULL";
        auto parsed = parse(sql,binding); const auto* root = query(parsed.stmt.get());
        const auto* left = root ? query(root->setOpLhs.get()) : nullptr;
        const auto* right = root ? query(root->setOpRhs.get()) : nullptr;
        const auto label = std::to_string(binding) + ":global NULL count";
        check(parsed.isValid(),label + ":accepted " + parsed.error);
        check(root && root->setOp == SetOp::Union && root->setOpAll && root->valuesRows.empty() &&
            counts(root,{},0) && keys(root,{}) && fetch(root),label + ":NULL counts belong to set root");
        check(rows(left,{{"L:1"}}) && rows(right,{{"L:2"}}) && counts(left) && counts(right) &&
            fetch(left) && fetch(right),label + ":branches do not inherit global NULL counts");
        check(left && right && left != right && left != root && right != root,label + ":actual independent query owners");
    }
    for (const std::string sql : {"SELECT 1 LIMIT -1","SELECT 1 OFFSET -1"})
        for (const bool binding : {false,true}) {
            const auto parsed = parse(sql,binding);
            check(!parsed.isValid() && !parsed.error.empty(),
                std::to_string(binding) + ":existing SELECT negative-count syntax boundary " + sql);
        }
    // Scalar VALUES queries follow the same retained-query grammar role and
    // byte provenance as SELECT/WITH. Parent names remain raw source names
    // here; the actual binder, not a synthetic parameter, resolves them.
    struct ScalarValues {
        std::string sql, scalar, first;
        size_t slot = 0, parentCtes = 0, childCtes = 0;
        bool correlated = false, valuesParent = false, nestedOrder = false;
        std::string second = "L:3";
    };
    const std::vector<ScalarValues> scalarValues{
        {"SELECT (VALUES(47),(3) ORDER BY column1 DESC LIMIT 1)",
            "(VALUES(47),(3) ORDER BY column1 DESC LIMIT 1)","L:47"},
        {"SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) ORDER BY column1 DESC LIMIT 1) FROM values_parent p ORDER BY p.column1",
            "(VALUES(p.column1+10),(p.column1+20) ORDER BY column1 DESC LIMIT 1)","B:+(C:p.column1,L:10)",1,0,0,true,false,false,"B:+(C:p.column1,L:20)"},
        {"SELECT p.column1,(VALUES(column1+10),(column1+20) ORDER BY column1 DESC LIMIT 1) FROM values_parent p ORDER BY p.column1",
            "(VALUES(column1+10),(column1+20) ORDER BY column1 DESC LIMIT 1)","B:+(C:column1,L:10)",1,0,0,true,false,false,"B:+(C:column1,L:20)"},
        {"SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) ORDER BY (SELECT -column1+p.column1) LIMIT 1) FROM values_parent p ORDER BY p.column1",
            "(VALUES(p.column1+10),(p.column1+20) ORDER BY (SELECT -column1+p.column1) LIMIT 1)","B:+(C:p.column1,L:10)",1,0,0,true,false,true,"B:+(C:p.column1,L:20)"},
        {"SELECT (WITH c AS(VALUES(9)) VALUES(47),(3) ORDER BY column1 LIMIT 1)",
            "(WITH c AS(VALUES(9)) VALUES(47),(3) ORDER BY column1 LIMIT 1)","L:47",0,0,1},
        {"VALUES((VALUES(47),(3) ORDER BY column1 DESC LIMIT 1)),(9) ORDER BY column1",
            "(VALUES(47),(3) ORDER BY column1 DESC LIMIT 1)","L:47",0,0,0,false,true},
        {"SELECT (VALUES((SELECT 47)),(3) ORDER BY (SELECT -column1) LIMIT 1)",
            "(VALUES((SELECT 47)),(3) ORDER BY (SELECT -column1) LIMIT 1)","",0,0,0,false,false,true},
        {"WITH c AS(SELECT 9) SELECT (VALUES(47),(3) ORDER BY column1 LIMIT 1)",
            "(VALUES(47),(3) ORDER BY column1 LIMIT 1)","L:47",0,1}
    };
    for (const auto& item : scalarValues) for (const bool binding : {false,true}) {
        auto parsed = parse(item.sql,binding); const auto* root = query(parsed.stmt.get());
        const Expr* value = nullptr;
        if (root && item.valuesParent && !root->valuesRows.empty() && !root->valuesRows[0].empty())
            value = root->valuesRows[0][0].get();
        else if (root && root->selectList.size() > item.slot) value = root->selectList[item.slot].expr.get();
        const auto* literal = dynamic_cast<const LiteralExpr*>(value);
        const auto label = std::to_string(binding) + ":scalar VALUES " + item.sql;
        check(parsed.isValid() && root,label + ":accepted actual parent " + parsed.error);
        const auto tokens = literal ? SQLParser::tokenize(literal->value) : std::vector<std::string>{};
        const bool retained = literal && literal->typeName.empty() && !literal->preparedSubquery &&
            tokens.size() > 2 && tokens.front() == "(" && tokens.back() == ")" &&
            (SQLParser::toLower(tokens[1]) == "values" || SQLParser::toLower(tokens[1]) == "with");
        check(retained,label + ":real raw query literal, not an empty-name function");
        auto childResult = retained ? parse(literal->value.substr(1,literal->value.size()-2),binding) : ParseResult{};
        const auto* child = query(childResult.stmt.get());
        const auto* first = child && child->valuesRows.size() == 2 && child->valuesRows[0].size() == 1 &&
            child->valuesRows[1].size() == 1 ? child->valuesRows[0][0].get() : nullptr;
        const bool input = first && (item.first.empty() ? dynamic_cast<const LiteralExpr*>(first) != nullptr :
            expression(first) == item.first);
        check(childResult.isValid() && child && child->command == SqlCommand::Values && input &&
            expression(child->valuesRows[1][0].get()) == item.second && counts(child,1) &&
            child->orderBy.size() == 1 && child->ctes.size() == item.childCtes && child->selectList.empty(),
            label + ":child rows/parent name bytes/local ORDER and LIMIT");
        const auto* source = root ? root->fromClause.get() : nullptr;
        const bool parent = root && root->ctes.size() == item.parentCtes &&
            (item.valuesParent ? root->command == SqlCommand::Values && root->valuesRows.size() == 2 &&
                keys(root,{{"C:column1"}}) :
                root->command == SqlCommand::Select && root->selectList.size() == item.slot+1 &&
                keys(root,item.correlated ? std::vector<Key>{{"C:p.column1"}} : std::vector<Key>{}));
        const auto* nested = child && child->orderBy.size() == 1 ?
            dynamic_cast<const LiteralExpr*>(child->orderBy[0].expr.get()) : nullptr;
        check(parent && (!item.correlated || (source && source->type == FromItem::Type::Table &&
                source->tableName == "values_parent" && source->alias == "p")) &&
            (!item.nestedOrder || (nested && !nested->preparedSubquery &&
                nested->value.find("SELECT") != std::string::npos)),
            label + ":separate real parent scope and nested ORDER query role");
        const auto begin = item.sql.find(item.scalar);
        check(literal && (binding ? begin != std::string::npos && literal->sourceBegin == begin &&
            literal->sourceEnd == begin+item.scalar.size() &&
            item.sql.substr(literal->sourceBegin,literal->sourceEnd-literal->sourceBegin) == item.scalar :
            literal->sourceBegin == std::string::npos && literal->sourceEnd == std::string::npos),
            label + (binding ? ":exact original scalar query byte span" : ":legacy parse has no binding byte spans"));
    }
    // Genuine VALUES count expressions are grammar/AST observations. Name
    // legality, coercion, negative-count errors and actual demand stay with
    // the binder and real Limit operator exercised by execution/wire tests.
    struct CountExpression {
        std::string suffix, limit = "missing", offset = "missing", limitBytes = {}, offsetBytes = {};
        std::optional<size_t> legacyLimit = {}, legacyOffset = {};
    };
    const std::vector<CountExpression> countExpressions{
        {"LIMIT $1","P:1:input","missing","$1"},
        {"OFFSET $1","missing","P:1:input","","$1"},
        {"LIMIT 1+1","B:+(L:1,L:1)","missing","1+1"},
        {"OFFSET (1+1)","missing","B:+(L:1,L:1)","","1+1"},
        {"LIMIT ($1+1) OFFSET $2","B:+(P:1:input,L:1)","P:2:input","$1+1","$2"},
        {"OFFSET ($2) ROWS LIMIT ($1)","P:1:input","P:2:input","$1","$2"},
        {"LIMIT $1::bigint","B:::(P:1:input,L:bigint)","missing","$1::bigint"},
        {"OFFSET NULL::bigint","missing","B:::(L:null,L:bigint)","","NULL::bigint"},
        {"LIMIT CAST($1 AS bigint)","CAST:bigint(P:1:input)","missing","CAST($1 AS bigint)"},
        {"LIMIT abs(-$1)","F:abs(U:-(P:1:input))","missing","abs(-$1)"},
        {"OFFSET column1","missing","C:column1","","column1"},
        {"LIMIT p.column1","C:p.column1","missing","p.column1"},
        {"LIMIT (SELECT 2)","Q:select 2","missing","(SELECT 2)"},
        {"OFFSET (VALUES(1))","missing","Q:values ( 1 )","","(VALUES(1))"},
        {"LIMIT (WITH c AS(VALUES(2)) SELECT column1 FROM c)",
            "Q:with c as ( values ( 2 ) ) select column1 from c","missing","(WITH c AS(VALUES(2)) SELECT column1 FROM c)"},
        {"LIMIT CASE WHEN true THEN $1 ELSE 2 END","CASE(missing;L:true=>P:1:input;L:2)",
            "missing","CASE WHEN true THEN $1 ELSE 2 END"},
        {"LIMIT (NULL)","L:null","missing","NULL"},
        {"OFFSET (0) LIMIT +2","missing","L:0","","0",2},
        {"LIMIT (2) OFFSET -0","L:2","missing","2","",{},0},
        {"LIMIT 1+$1 OFFSET 1","B:+(L:1,P:1:input)","missing","1+$1","",{},1}
    };
    for (const auto& item : countExpressions) for (const bool binding : {false,true}) {
        const std::string sql = "VALUES(1),(2) ORDER BY column1 DESC " + item.suffix;
        auto parsed = parse(sql,binding); const auto* root = query(parsed.stmt.get());
        const auto* limit = CountTrees<SelectStmt>::limit(root);
        const auto* offset = CountTrees<SelectStmt>::offset(root);
        const auto label = std::to_string(binding) + ":count expression " + sql;
        check(parsed.isValid() && rows(root,{{"L:1"},{"L:2"}}),label + ":accepted with original VALUES rows " + parsed.error);
        check(countShape(limit) == item.limit && countShape(offset) == item.offset,
            label + ":exact original count trees and statement-input parameter owner");
        check(counts(root,item.legacyLimit,item.legacyOffset) && fetch(root),
            label + ":only complete simple integers populate executable cap/OFFSET");
        check(root && CountTrees<SelectStmt>::role(root) && root->setOp == SetOp::None &&
            !root->setOpLhs && !root->setOpRhs && root->ctes.empty() && keys(root,{{"C:column1",false,true}}),
            label + ":real count clause role and independent ORDER scope");
        check(countSpan(limit,sql,"LIMIT",item.limitBytes,binding) &&
            countSpan(offset,sql,"OFFSET",item.offsetBytes,binding),
            label + (binding ? ":exact original count bytes" : ":legacy count trees have no binding spans"));
    }
    const std::vector<std::string> invalidCounts{
        "LIMIT OFFSET 1", "OFFSET LIMIT 1", "LIMIT 1+", "OFFSET 1*", "LIMIT (1+)",
        "LIMIT ALL+1", "OFFSET ALL", "LIMIT DEFAULT", "OFFSET DEFAULT", "LIMIT $1 LIMIT $2",
        "OFFSET $1 OFFSET $2", "LIMIT $1 FETCH FIRST 1 ROW ONLY", "LIMIT CASE WHEN true THEN 1",
        "LIMIT CASE WHEN true 1 END", "OFFSET CASE WHEN true THEN END", "LIMIT $1,2",
        "OFFSET ($1) ROWS ROWS", "LIMIT (SELECT 2) OFFSET", "FETCH FIRST ($1) ROWS ONLY",
        "FETCH FIRST 1+1 ROWS ONLY"
    };
    for (const auto& suffix : invalidCounts) for (const bool binding : {false,true}) {
        const std::string sql = "VALUES(1),(2) ORDER BY column1 " + suffix;
        const auto parsed = parse(sql,binding);
        if (suffix == "FETCH FIRST ($1) ROWS ONLY") {
            const auto* root = query(parsed.stmt.get());
            const auto* input = root ? dynamic_cast<const ParameterExpr*>(CountTrees<SelectStmt>::limit(root)) : nullptr;
            check(parsed.isValid() && rows(root,{{"L:1"},{"L:2"}}) && input && input->slot == 0 &&
                input->origin == ParameterOrigin::StatementInput && input->declaredType.empty() &&
                root->fetchFirst && !root->withTies && !root->limit && !root->signedFetchCount &&
                !CountTrees<SelectStmt>::offset(root) && CountTrees<SelectStmt>::role(root) &&
                countSpan(input,sql,"FETCH FIRST","$1",binding),
                std::to_string(binding) + ":genuine FETCH parameter AST and original byte provenance " + sql);
        } else {
            check(!parsed.isValid() && !parsed.error.empty(),
                std::to_string(binding) + ":strict count syntax rejection " + sql);
        }
    }
    std::cout << "VALUES_QUERY_CLAUSE_GRAMMAR_COMPLETE " << checked << " FAILURES " << failures << '\n';
    if (checked != 860) return 2;
    return failures ? 1 : 0;
}
