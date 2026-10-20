#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <algorithm>
#include <cctype>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "SCALAR_NULL_BITMAP_FAILURE " << label << '\n'; }
    };
    const auto database = testDbPath("scalar_result_null_bitmap");
    check(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create owned database");
    TableSchema schema; schema.tablename = "source";
    schema.append(makeIntColumn("id", false, 2, true));
    schema.append(makeTextColumn("v", true));
    check(g_engine.createTable(database, schema) == DBStatus::OK, "create source");
    const std::vector<std::optional<std::string>> inputs = {
        "NULL", "null", std::nullopt, "", "LLUN", "xNULLx", "a b"};
    bool inserted = true;
    for (size_t i = 0; i < inputs.size(); ++i)
        inserted &= g_engine.insertRow(database, "source",
            {{"id", std::to_string(i + 1)}, {"v", inputs[i]}}) == DBStatus::OK;
    check(inserted, "insert exact text, empty and real NULL");
    struct Profile { std::string name; std::vector<std::string> arguments; };
    const std::vector<Profile> profiles = {
        {"upper", {"v"}}, {"lower", {"v"}}, {"initcap", {"v"}}, {"reverse", {"v"}},
        {"btrim", {"v", "'x'"}}, {"ltrim", {"v", "'x'"}}, {"rtrim", {"v", "'x'"}},
        {"left", {"v", "32"}}, {"right", {"v", "32"}}, {"repeat", {"v", "1"}},
        {"substring", {"v", "1", "32"}}, {"substr", {"v", "1", "32"}},
        {"lpad", {"v", "32", "''"}}, {"rpad", {"v", "32", "''"}},
        {"split_part", {"v", "'|'", "1"}}, {"replace", {"v", "'x'", "''"}},
        {"translate", {"v", "'x'", "''"}}, {"overlay", {"v", "''", "1", "0"}},
        {"concat", {"v"}}, {"concat_ws", {"''", "v"}},
        {"coalesce", {"v", "'NULL'"}}, {"nullif", {"v", "''"}}, {"quote_nullable", {"v"}}
    };
    const auto expected = [&](const std::string& name, size_t ordinal) -> std::optional<std::string> {
        if (!inputs[ordinal]) {
            if (name == "concat" || name == "concat_ws") return std::string{};
            if (name == "coalesce" || name == "quote_nullable") return "NULL";
            return std::nullopt;
        }
        std::string value = *inputs[ordinal];
        if (name == "nullif" && value.empty()) return std::nullopt;
        if (name == "upper") for (char& c : value) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        if (name == "lower") for (char& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (name == "initcap") {
            bool start = true;
            for (char& c : value) {
                const auto byte = static_cast<unsigned char>(c);
                c = static_cast<char>(start ? std::toupper(byte) : std::tolower(byte));
                start = !std::isalnum(byte);
            }
        }
        if (name == "reverse") std::reverse(value.begin(), value.end());
        if (name == "replace" || name == "translate") value.erase(std::remove(value.begin(), value.end(), 'x'), value.end());
        if (name == "btrim" || name == "ltrim") while (!value.empty() && value.front() == 'x') value.erase(value.begin());
        if (name == "btrim" || name == "rtrim") while (!value.empty() && value.back() == 'x') value.pop_back();
        if (name == "quote_nullable") value = "'" + value + "'";
        return value;
    };
    StorageEngine::SelectExpr id; id.colName = "id"; id.displayName = "id";
    StorageEngine::SelectExpr expand; expand.isScalar = true;
    expand.funcName = "unnest"; expand.funcArgs = {"'[7,8]'"}; expand.displayName = "n";
    for (const auto& profile : profiles) for (const bool srf : {false, true})
        for (const bool ascending : {true, false})
            for (const auto cap : std::vector<std::optional<size_t>>{std::nullopt, 0, 1, 9}) {
                StorageEngine::SelectExpr function; function.isScalar = true;
                function.funcName = profile.name; function.funcArgs = profile.arguments; function.displayName = "v";
                std::vector<StorageEngine::SelectExpr> expressions = {id, function};
                if (srf) expressions.push_back(expand);
                StorageEngine::QueryExprExecutionOptions options; options.maxProjectionRows = cap;
                std::vector<std::vector<std::string>> rows;
                std::vector<std::vector<bool>> nulls;
                std::vector<int64_t> rids;
                const std::string label = profile.name + (srf ? " expanded" : " ordinary") +
                    (ascending ? " ascending " : " descending ") + (cap ? std::to_string(*cap) : "unbounded");
                try {
                    const auto legacy = g_engine.queryExpr(database, "source", {}, expressions, {{"id", ascending}}, &rows, &nulls, &rids, options);
                    const size_t available = inputs.size() * (srf ? 2 : 1);
                    const size_t count = cap ? std::min(*cap, available) : available;
                    bool good = rows.size() == count && nulls.size() == count && rids.size() == count && legacy.size() == count;
                    for (size_t row = 0; good && row < count; ++row) {
                        const size_t position = row / (srf ? 2 : 1);
                        const size_t ordinal = ascending ? position : inputs.size() - 1 - position;
                        const auto value = expected(profile.name, ordinal);
                        good = rows[row].size() == (srf ? 3 : 2) && nulls[row].size() == rows[row].size();
                        good = good && rows[row][0] == std::to_string(ordinal + 1) && !nulls[row][0];
                        good = good && nulls[row][1] == !value.has_value();
                        if (value) good = good && rows[row][1] == *value;
                        if (srf) good = good && rows[row][2] == (row % 2 ? "8" : "7") && !nulls[row][2];
                    }
                    check(good && !g_engine.inTransaction(), label);
                } catch (const std::exception& error) { check(false, label + " " + error.what()); }
            }
    struct Error { Profile profile; std::string state; };
    for (const auto& item : std::vector<Error>{
        {{"split_part", {"v", "','", "0"}}, "22023"},
        {{"substring", {"v", "1", "-1"}}, "22011"},
        {{"overlay", {"v", "'X'", "0"}}, "22011"},
        {{"overlay", {"v", "'X'", "2147483647", "1"}}, "22003"},
        {{"repeat", {"v", "'1x'"}}, "22P02"},
        {{"left", {"v", "2147483648"}}, "22003"},
        {{"right", {"v", "-2147483649"}}, "22003"}}) for (const bool srf : {false, true}) {
        StorageEngine::SelectExpr function; function.isScalar = true;
        function.funcName = item.profile.name; function.funcArgs = item.profile.arguments;
        std::vector<StorageEngine::SelectExpr> expressions = {function}; if (srf) expressions.push_back(expand);
        bool rejected = false;
        try { (void)g_engine.queryExpr(database, "source", {}, expressions); }
        catch (const DbError& error) { rejected = error.sqlState() == item.state; }
        catch (const std::exception& error) { rejected = std::string(error.what()).find("SQLSTATE " + item.state) != std::string::npos; }
        check(rejected && !g_engine.inTransaction(), item.profile.name + " exact error " + item.state);
    }
    std::cout << "SCALAR_NULL_BITMAP_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return controls == 385 && failures == 0 ? 0 : 1;
}
