// Use the real frontend translation unit and globals, never test_stubs.cpp.
#ifndef DBMS_WINDOW_FRONTEND_SOURCE
#define DBMS_WINDOW_FRONTEND_SOURCE "../../src/main.cpp"
#endif
#define main dbms_window_frontend_program_main
#include DBMS_WINDOW_FRONTEND_SOURCE
#undef main

#include <array>
#include <cstring>
#include <new>

int main() {
    TableSchema table;
    table.len = 3;
    for (size_t i = 0; i < table.len; ++i) {
        table.cols[i].dataName = std::array<std::string, 3>{"g", "v", "k"}[i];
        table.cols[i].dataType = "int";
    }
    size_t checks = 0, failures = 0;
    const auto check = [&](bool passed, const std::string& label) {
        ++checks;
        if (!passed) {
            ++failures;
            std::cerr << "WINDOW DEFAULT FAILED " << label << '\n';
        }
    };
    const bool initializedTrue = true;
    std::array<unsigned char, sizeof(bool)> trueBytes;
    std::memcpy(trueBytes.data(), &initializedTrue, sizeof(bool));
    for (unsigned char poison : {0x00, 0xa5}) {
        // Default-initialize exactly as the frontend's `WindowFunc wf;` does.
        // Inspect bytes before ever reading the formerly indeterminate bool.
        // This makes the unfixed-source failure deterministic, without UB.
        alignas(WindowFunc) unsigned char storage[sizeof(WindowFunc)];
        const auto construct = [&]() {
            std::memset(storage, poison, sizeof(storage));
            return ::new (static_cast<void*>(storage)) WindowFunc;
        };
        const auto hasDefault = [&](const WindowFunc& wf) {
            std::array<unsigned char, sizeof(bool)> actual;
            std::memcpy(actual.data(), &wf.orderByAsc, sizeof(bool));
            return actual == trueBytes;
        };
        WindowFunc* wf = construct();
        check(hasDefault(*wf), "default constructor bytes poison=" + std::to_string(poison));
        wf->~WindowFunc();
        const auto parseAndLower = [&](const std::string& sql, bool hasOrder,
                                       bool ascending, const std::map<std::string, std::string>& named = {}) {
            WindowFunc* parsed = construct();
            const bool accepted = parseWindowFunc(sql, *parsed, named);
            // Ordered cases explicitly assign the member during real parsing.
            // Unordered cases must have initialized it during construction.
            const bool safeToRead = hasOrder || hasDefault(*parsed);
            dbms::WindowFunctionSpec spec;
            const bool lowered = accepted && safeToRead &&
                convertToVolcanoWindowSpec(*parsed, table, spec);
            check(lowered && spec.orderAscending == ascending &&
                  spec.orderBy == (hasOrder ? "k" : "") &&
                  spec.name == parsed->name,
                  "actual parse/lowering " + sql + " poison=" + std::to_string(poison));
            parsed->~WindowFunc();
        };
        for (const std::string function : {"sum(v)", "count(*)", "avg(v)", "min(v)", "max(v)"}) {
            parseAndLower(function + " OVER ()", false, true);
            parseAndLower(function + " OVER (PARTITION BY g)", false, true);
            parseAndLower(function + " OVER (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)", false, true);
            parseAndLower(function + " OVER totals", false, true, {{"totals", "PARTITION BY g"}});
        }
        for (const std::string function : {"row_number()", "rank()", "dense_rank()"})
            parseAndLower(function + " OVER (PARTITION BY g)", false, true);
        for (const std::string order : {"k", "k ASC", "k DESC", "k DESC NULLS LAST", "k ASC NULLS FIRST"})
            parseAndLower("sum(v) OVER (PARTITION BY g ORDER BY " + order + ")", true,
                          order.find("DESC") == std::string::npos);
    }
    std::cout << "[WINDOW DEFAULT ORDER REAL FRONTEND] complete checks=" << checks
              << " failures=" << failures << '\n';
    return failures ? 1 : 0;
}
