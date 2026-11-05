#include "catalog/AnalyzedScalarStatistics.h"
#include <iostream>
#include <stdexcept>

namespace {
namespace stats = dbms::analyzed_statistics;
size_t checks = 0;
void require(bool good, const char* message) {
    ++checks; if (!good) throw std::runtime_error(message);
}
template<class F> void rejected(F action) {
    bool failed = false;
    try { action(); } catch (const dbms::DbError& error) { failed = error.sqlState() == "XX001"; }
    require(failed, "malformed analysis input is rejected with XX001");
}
stats::Column declaration(double population, int target = 3) {
    stats::Column c;
    auto type = std::make_shared<dbms::DatumType>();
    type->actualOid = type->codecOid = 23; type->codecSize = 4;
    c.attributeType = type; c.totalRows = population; c.target = target; c.averageWidth = 4;
    return c;
}
using Samples = std::vector<stats::ScalarSample>;
void append(Samples& rows, std::optional<int> value, bool wide = false, uint32_t width = 4) {
    stats::ScalarSample row; row.ordinal = rows.size();
    row.value = dbms::ExprValue("integer", value ? std::to_string(*value) : "", !value);
    row.storedWidth = width; row.tooWide = wide; rows.push_back(std::move(row));
}
std::optional<stats::Column> collect(stats::Column c, const Samples& sample) {
    return stats::collectScalarStatistics(c, sample, 96, 97,
        [](const auto& a, const auto& b) { const int x = std::stoi(a.value), y = std::stoi(b.value); return (x > y) - (x < y); });
}
const stats::Slot* slot(const stats::Column& c, stats::Kind kind) {
    for (const auto& x : c.slots) if (x.kind == kind) return &x;
    return nullptr;
}
std::vector<std::string> values(const stats::Column& c, stats::Kind kind) {
    const auto* s = slot(c, kind); std::vector<std::string> result;
    if (s && s->values) for (const auto& x : s->values->cells) result.push_back(x.value);
    return result;
}
} // namespace
int main() {
    try {
        Samples sample;
        for (int i = 39; i >= 0; --i) append(sample, i);
        auto c = *collect(declaration(40), sample);
        require(c.distinctEstimate == -1 && c.nullFraction == 0 && c.averageWidth == 4, "full-population unique fields");
        require(values(c, stats::Kind::Histogram) == std::vector<std::string>({"0", "13", "26", "39"}), "target+1 quantiles include actual endpoints");
        require(slot(c, stats::Kind::Correlation)->numbers->at(0) == -1, "reverse physical ranks");
        require(!slot(c, stats::Kind::MostCommon), "singleton groups are not MCV");
        const auto indexed = *stats::collectScalarStatistics(declaration(40), sample, 96, 97, {}, {},
            [&](size_t a, size_t b) { const int x = std::stoi(sample[a].value.value), y = std::stoi(sample[b].value.value); return (x > y) - (x < y); });
        require(values(indexed, stats::Kind::Histogram) == values(c, stats::Kind::Histogram) &&
            slot(indexed, stats::Kind::Correlation)->numbers == slot(c, stats::Kind::Correlation)->numbers,
            "indexed comparison preserves typed value and physical-rank results");
        sample.clear();
        for (int i = 0; i < 15; ++i) append(sample, {});
        for (int i = 0; i < 5; ++i) append(sample, i);
        c = *collect(declaration(20), sample);
        require(c.nullRows == 15 && c.nullFraction == .75f && c.distinctEstimate == -.25f, "high NULL unique fraction");
        require(slot(c, stats::Kind::Correlation)->numbers->at(0) == 1, "NULL rows do not enter sortable ranks");
        sample.clear();
        for (int i = 0; i < 20; ++i) append(sample, 1);
        for (int i = 2; i <= 21; ++i) append(sample, i);
        c = *collect(declaration(40), sample);
        require(values(c, stats::Kind::MostCommon) == std::vector<std::string>{"1"} && slot(c, stats::Kind::MostCommon)->numbers->at(0) == .5f, "MCV frequency includes all rows");
        require(values(c, stats::Kind::Histogram) == std::vector<std::string>({"2", "8", "14", "21"}), "histogram excludes the entire MCV population");
        // Reverse vector slots without changing real scan ordinals. A
        // reservoir owner may replace any slot, but physical correlation and
        // representative/quantile order must remain exactly unchanged.
        std::reverse(sample.begin(), sample.end());
        const auto shuffled = *collect(declaration(40), sample);
        require(values(shuffled, stats::Kind::Histogram) == values(c, stats::Kind::Histogram) &&
            slot(shuffled, stats::Kind::Correlation)->numbers == slot(c, stats::Kind::Correlation)->numbers,
            "reservoir slot order cannot change physical-order statistics");
        sample.clear();
        for (int i = 0; i < 18; ++i) append(sample, 1);
        append(sample, 2); append(sample, 3);
        c = *collect(declaration(1000), sample);
        require(c.distinctEstimate == 3 && values(c, stats::Kind::MostCommon) == std::vector<std::string>{"1"}, "sample estimator and significant common item");
        require(slot(c, stats::Kind::MostCommon)->numbers->at(0) == .9f, "sample frequency denominator");
        sample.clear();
        for (int i = 0; i < 10; ++i) { append(sample, i); append(sample, i); }
        c = *collect(declaration(1000), sample);
        require(c.distinctEstimate == 10 && !slot(c, stats::Kind::MostCommon), "weak incomplete MCV tail is removed");
        sample.clear(); append(sample, 1); append(sample, 1);
        append(sample, 9, true, 1025); append(sample, 9, true, 1025);
        append(sample, {}); append(sample, {});
        c = *collect(declaration(6), sample);
        require(c.averageWidth == 514 && c.distinctEstimate == -.5f, "wide values affect width and singleton estimate");
        require(values(c, stats::Kind::MostCommon) == std::vector<std::string>{"1"} &&
            slot(c, stats::Kind::MostCommon)->numbers->at(0) == 1.0f / 3,
            "wide and NULL rows remain in MCV denominator");
        sample.clear(); for (int i = 0; i < 3; ++i) append(sample, {});
        c = *collect(declaration(3), sample);
        require(c.nullFraction == 1 && c.averageWidth == 4 && c.distinctEstimate == 0 && c.slots.empty(), "fixed-width all NULL applicability");
        require(!collect(declaration(0), {}), "empty table has no invented statistics record");
        require(!collect(declaration(3, 0), sample), "statistics target zero disables collection");
        rejected([&] { collect(declaration(2), sample); });
        rejected([&] { collect(declaration(3, 10001), sample); });
        sample.clear(); append(sample, 1); append(sample, 2); sample.back().ordinal = 0;
        rejected([&] { collect(declaration(2), sample); });
        sample.clear(); append(sample, 1, false, UINT32_MAX);
        rejected([&] { collect(declaration(1), sample); });
        std::cout << "ANALYZED_SCALAR_COLLECTOR checks=" << checks << " failures=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ANALYZED_SCALAR_COLLECTOR_FAIL checks=" << checks << ' ' << error.what() << '\n'; return 1;
    }
}
