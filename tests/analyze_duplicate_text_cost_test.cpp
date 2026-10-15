// Preserve actual libc collation results and count calls only on this thread.
// The quantitative control is available on glibc with the required locale;
// other platforms still run the full value/NULL/statistics assertions.
// This is an operation-count bound, not a wall-clock performance assertion.
#include "catalog/collation.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <vector>
#ifdef __GLIBC__
#include <dlfcn.h>
#include <locale.h>
#include <unistd.h>
namespace {
thread_local bool countLocale = false;
thread_local size_t localeCalls = 0;
}
extern "C" int __strcoll_l(const char* left, const char* right, locale_t locale) noexcept {
    using Compare = int (*)(const char*, const char*, locale_t);
    static const auto original = reinterpret_cast<Compare>(dlsym(RTLD_NEXT, "__strcoll_l"));
    if (!original) _exit(91);
    if (countLocale) ++localeCalls;
    return original(left, right, locale);
}
#endif

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const bool savedAnalyze = g_config.autoAnalyzeEnabled;
    g_config.autoAnalyzeEnabled = false; // Isolate explicit ANALYZE, not a product policy.
    struct Restore {bool value; ~Restore(){g_config.autoAnalyzeEnabled=value;}} restore{savedAnalyze};
    const std::string database = testDbPath("analyze_duplicate_text_cost");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "duplicate_text";
    table.append(makeTextColumn("v", true));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    const std::string payload(16384, 'x');
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    for (size_t i = 0; i < 32; ++i)
        assert(g_engine.insertRow(database, table.tablename, {{"v", payload}}) == DBStatus::OK);
    for (size_t i = 0; i < 4; ++i)
        assert(g_engine.insertRow(database, table.tablename, {{"v", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    size_t referenceCalls = 0, analyzedCalls = 0;
    bool counterAvailable = false;
#ifdef __GLIBC__
    localeCalls = 0; countLocale = true;
    const int compared = collation::compare(payload, payload, "en_US.utf8");
    countLocale = false; referenceCalls = localeCalls;
    assert(compared == 0);
    counterAvailable = referenceCalls > 0;
    localeCalls = 0; countLocale = true;
#endif
    const bool analyzed = g_engine.analyzeTable(database, table.tablename);
#ifdef __GLIBC__
    countLocale = false; analyzedCalls = localeCalls;
#endif
    assert(analyzed);
    const auto stats = g_engine.getColumnStats(database, table.tablename, "v");
    bool semantics = stats.cardinality == 1 && stats.nullCount == 4 &&
        stats.minVal == payload && stats.maxVal == payload &&
        stats.mcv == std::vector<std::pair<std::string,size_t>>{{payload,32}} &&
        stats.histogram.size() == 10 && g_engine.getTableRowCount(database, table.tablename) == 36;
    for (const auto& bucket : stats.histogram)
        semantics = semantics && bucket.first == payload && bucket.second == payload;
    std::cout << "ANALYZE_DUPLICATE_TEXT_COST reference=" << referenceCalls
              << " analyzed=" << analyzedCalls << " available=" << counterAvailable
              << " semantics=" << semantics << '\n';
    const bool bounded = !counterAvailable || analyzedCalls <= referenceCalls;
    if (!counterAvailable) std::cout << "ANALYZE_DUPLICATE_TEXT_COST_COUNTER_UNAVAILABLE\n";
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    return semantics && bounded ? 0 : 1;
}
