#include "utils/prepared_stmts.h"

#include <iostream>
#include <limits>
#include <string>
#include <vector>

namespace {
int checks = 0;
int failures = 0;
void check(bool ok, const std::string& label) {
    ++checks;
    if (!ok) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}
void valid(const std::string& sql, size_t arity,
           const std::vector<std::string>& values, const std::string& expected) {
    size_t count = 999;
    std::string error = "stale", result;
    const bool analyzed = dbms::ps_analyzeDollarParams(sql, count, error);
    check(analyzed, "analyze " + sql);
    check(analyzed && count == arity, "arity " + sql);
    check(analyzed && error.empty(), "analyze error " + sql);
    error = "stale";
    const bool substituted = dbms::ps_substituteDollarParams(sql, values, result, error);
    check(substituted, "substitute " + sql);
    check(substituted && result == expected, "exact bytes " + sql);
    check(substituted && error.empty(), "substitute error " + sql);
}
}

int main() {
    const std::vector<std::string> identifiers = {
        "value$1", "value$0", "value$12", "value$01", "value$1$2",
        "value$$", "value$tag$", "value$_tag$", "_value$2", "a9$2",
        "value$184467440737095516160", "value$", "MiXeD$3",
        u8"列$0", u8"é$1", u8"α$tag$", "value$$$0"};
    for (const auto& id : identifiers) {
        valid("SELECT " + id + " FROM t", 0, {}, "SELECT " + id + " FROM t");
        valid("SELECT $1 AS " + id, 1, {"41"}, "SELECT 41 AS " + id);
        valid("SELECT " + id + ", $2, $1", 2, {"11", "22"},
              "SELECT " + id + ", 22, 11");
        valid("SELECT t." + id + " + $1", 1, {"3"}, "SELECT t." + id + " + 3");
        valid("SELECT \"" + id + "\", $1", 1, {"7"}, "SELECT \"" + id + "\", 7");
    }
    valid("SELECT $1 + $12 + $01", 12,
          {"1","2","3","4","5","6","7","8","9","10","11","12"},
          "SELECT 1 + 12 + 1");
    valid("$1$2", 2, {"a", "b"}, "ab");
    valid("SELECT 'a$1', \"a$2\", $$ $3 $$, $_tag$ $4 $_tag$, $1", 1, {"8"},
          "SELECT 'a$1', \"a$2\", $$ $3 $$, $_tag$ $4 $_tag$, 8");
    valid("SELECT E'it\\'s $9', e'\\\\ $8', $1", 1, {"6"},
          "SELECT E'it\\'s $9', e'\\\\ $8', 6");
    valid("SELECT a$0 /* $0 /* $99 */ */ + $1 -- $0\r\n", 1, {"5"},
          "SELECT a$0 /* $0 /* $99 */ */ + 5 -- $0\r\n");
    valid("SELECT a$tag$/**/,$1", 1, {"4"}, "SELECT a$tag$/**/,4");
    for (const auto& sql : {std::string("SELECT $0"),
                           std::string("SELECT $184467440737095516160")}) {
        size_t count = 0;
        std::string error, output;
        check(!dbms::ps_analyzeDollarParams(sql, count, error), "invalid analyze " + sql);
        check(!error.empty(), "invalid analyze error " + sql);
        check(!dbms::ps_substituteDollarParams(sql, {"7"}, output, error), "invalid substitute " + sql);
        check(!error.empty(), "invalid substitute error " + sql);
    }
    std::string output, error;
    check(!dbms::ps_substituteDollarParams("SELECT a$1, $2", {"7"}, output, error), "real out of range");
    check(error == "there is no parameter $2", "out of range exact error");
    std::cout << "PREPARED_IDENTIFIER_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
