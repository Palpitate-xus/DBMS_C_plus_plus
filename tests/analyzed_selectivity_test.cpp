#include "executor/AnalyzedSelectivity.h"
#include <iostream>
#include <stdexcept>

namespace {
using namespace dbms;
namespace stats = analyzed_statistics;
size_t checks = 0;
void need(bool pass, const char* label) {
    ++checks; if (!pass) throw std::runtime_error(label);
}
void close(std::optional<double> value, double expected, const char* label) {
    need(value && std::abs(*value - expected) < 2e-6, label);
}
stats::Column fixture() {
    stats::Column c;
    auto type = std::make_shared<DatumType>(); type->actualOid = type->codecOid = 25;
    c.attributeType = type; c.totalRows = c.sampleRows = 100; c.nullRows = 10;
    c.nullFraction = .1f; c.distinctEstimate = 22;
    c.slots = {
        {stats::Kind::MostCommon, 98, 0, std::vector<float>{.4f,.1f},
            stats::Values{type, {{"text","hot\n|;,界"},{"text","NULL"}}}},
        {stats::Kind::Histogram, 664, 0, {}, stats::Values{type,
            {{"text","aa"},{"text","ab"},{"text","ac"},{"text","za"},{"text","zz"}}}}
    };
    return c;
}
auto equal(const std::string& probe) {
    return [probe](const ExprValue& value) -> std::optional<bool> { return value.value == probe; };
}
} // namespace

int main() try {
    using namespace dbms::planner_statistics;
    auto c = fixture();
    close(analyzedEqualityFraction(c,100,equal("hot\n|;,界")),.4,"MCV bytes and actual binary32 probability");
    close(analyzedEqualityFraction(c,100,equal("NULL")),.1,"literal NULL is a nonNULL datum");
    close(analyzedEqualityFraction(c,100,equal("absent")),.02,"residual excludes all MCVs and NULLs");
    close(analyzedPrefixDensity(c,"hot\n|;,"),.4,"newline/delimiter prefix retains full MCV frequency");
    close(analyzedPrefixDensity(c,"a"),.2,"two of four histogram bins use only residual mass");
    close(analyzedPrefixDensity(c,"z"),.1,"one fully covered residual histogram bin");
    close(analyzedPrefixDensity(c,"absent"),0,"unknown/straddling bins add no density evidence");
    need(!analyzedPrefixDensity(c,""),"no invented empty prefix guard");
    c.slots[1].values->cells = {{"text","hot-a"},{"text","hot-b"},{"text","zzz"}};
    close(analyzedPrefixDensity(c,"hot"),.6,"MCV and non-MCV masses add without double counting");
    c = fixture(); c.distinctEstimate = -.22f;
    close(analyzedEqualityFraction(c,100,equal("absent")),.02,"negative ndistinct scales with population");
    close(analyzedEqualityFraction(c,200,equal("absent")),.4/42,"negative ndistinct scales with current rows");
    c.distinctEstimate = 3;
    close(analyzedEqualityFraction(c,100,equal("absent")),.1,"nonMCV cannot exceed smallest common frequency");
    c = fixture(); c.slots.clear(); c.nullRows = 100; c.nullFraction = 1; c.distinctEstimate = 0;
    close(analyzedEqualityFraction(c,100,equal("anything")),0,"allNULL equality is zero");
    close(analyzedPrefixDensity(c,"a"),0,"allNULL prefix has no nonNULL mass");
    c.sampleRows = c.nullRows = 0; c.totalRows = 0; c.nullFraction = 0;
    close(analyzedEqualityFraction(c,0,equal("anything")),0,"empty analyzed population");
    for (size_t i=1; i<=4096; ++i) {
        c = fixture(); c.distinctEstimate = 2 + i; c.totalRows = c.sampleRows = 10000;
        c.nullRows = 1000;
        close(analyzedEqualityFraction(c,10000,equal("absent")),std::min(.1,.4/i),"residual population model");
        close(analyzedEqualityFraction(c,10000,equal("hot\n|;,界")),.4,"hot probability independent of residual distincts");
    }
    for (size_t mutation=0; mutation<13; ++mutation) {
        c = fixture();
        switch (mutation) {
            case 0: c.nullFraction = -1; break;
            case 1: c.sampleRows = 0; c.nullRows = 0; break;
            case 2: c.totalRows = 2; break;
            case 3: c.distinctEstimate = std::numeric_limits<float>::quiet_NaN(); break;
            case 4: c.slots[0].numbers->push_back(.1f); break;
            case 5: (*c.slots[0].numbers)[0] = 1; break;
            case 6: (*c.slots[0].numbers)[0] = -.1f; break;
            case 7: (*c.slots[0].numbers)[0] = std::numeric_limits<float>::infinity(); break;
            case 8: c.slots[0].values->cells[0].isNull = true; break;
            case 9: c.slots[0].values->elementType.reset(); break;
            case 10: c.slots[0].collation = 999; break;
            case 11: c.slots[0].comparisonOperator = 0; break;
            case 12: c.nullRows = 20; break;
        }
        need(!analyzedEqualityFraction(c,100,equal("absent")),"invalid typed evidence declines equality");
        need(!analyzedPrefixDensity(c,"a"),"invalid typed evidence declines prefix");
    }
    c = fixture();
    need(!analyzedEqualityFraction(c,100,[](const ExprValue&) -> std::optional<bool> { return {}; }),
        "unavailable comparison semantics preserve executor input ownership");
    c.slots[1].values->cells[0].isNull = true;
    need(!analyzedPrefixDensity(c,"a"),"NULL histogram boundary declines");
    std::cout << "ANALYZED_SELECTIVITY checks=" << checks << " failures=0\n";
    return 0;
} catch (const std::exception& e) {
    std::cerr << "ANALYZED_SELECTIVITY_FAIL checks=" << checks << ' ' << e.what() << '\n'; return 1;
}
