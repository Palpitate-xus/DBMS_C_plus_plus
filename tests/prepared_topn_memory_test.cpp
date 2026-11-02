#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "parser/query_binding.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <malloc.h>
#include <numeric>
#include <set>

// Observe live allocations at the real child EOF, before output consumption.
// No allocator or comparator is replaced. The unbounded/DISTINCT/WITH TIES
// controls must retain their full input, proving that the probe sees it.
static size_t liveHeapBytes() {
    const auto memory = mallinfo2();
    return memory.uordblks + memory.hblkhd;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& name) {
        return QueryRelationMetadata{"public", name,
            {{"id", "integer"}, {"k", "integer"}, {"payload", "text"}}, {}};
    };
    constexpr size_t count = 12000, width = 2048;
    size_t checked = 0, failures = 0;
    const auto run = [&](size_t limit, size_t offset, bool distinct,
                         bool ties, bool unbounded, bool nullsFirst,
                         bool uniqueOrder, bool requireBounded) {
        std::string sql = distinct ? "SELECT DISTINCT k" : "SELECT id,k,payload";
        sql += " FROM topn_input ORDER BY k DESC NULLS ";
        sql += nullsFirst ? "FIRST" : "LAST";
        if (uniqueOrder) sql += ",id ASC";
        if (offset) sql += " OFFSET " + std::to_string(offset);
        if (!unbounded) sql += ties
            ? " FETCH FIRST " + std::to_string(limit) + " ROWS WITH TIES"
            : " LIMIT " + std::to_string(limit);
        auto query = std::make_shared<PreparedQuery>(prepareQuery(sql, {}, metadata));
        auto* select = dynamic_cast<SelectStmt*>(query->ast.get());
        assert(select && query->sourceRanges.size() == 1);
        auto execution = std::make_shared<PreparedQueryExecution>(query, &engine, "");
        const auto ordinal = query->sourceRanges.front().ordinal;
        size_t reads = 0, eof = 0, before = 0, retained = 0;
        auto source = std::make_unique<PreparedSourceContextsOp>(
            [&, execution, ordinal](size_t position, RowContext& row) {
                if (!position) before = liveHeapBytes();
                if (position == count) {
                    ++eof;
                    const size_t current = liveHeapBytes();
                    retained = current > before ? current - before : 0;
                    return false;
                }
                ++reads;
                row = RowContext{};
                execution->setSourceRow(row, ordinal,
                    {ExprValue("integer", std::to_string(position)),
                     ExprValue("integer", std::to_string(position % 17), position % 13 == 0),
                     ExprValue("text", std::string(width, 'a' + position % 23), position % 29 == 0)});
                return true;
            });
        auto plan = QueryPlanner::buildPreparedSelectPlan(
            &engine, "", query, select, TableSchema{}, std::move(source));
        assert(plan && reads == 0 && eof == 0);

        std::vector<size_t> expected(count);
        std::iota(expected.begin(), expected.end(), 0);
        std::stable_sort(expected.begin(), expected.end(), [&](size_t left, size_t right) {
            const bool aNull = left % 13 == 0, bNull = right % 13 == 0;
            if (aNull != bNull) return aNull == nullsFirst;
            if (!aNull && left % 17 != right % 17) return left % 17 > right % 17;
            return uniqueOrder ? left < right : false;
        });
        const auto key = [](size_t row) { return row % 13 == 0 ? 17 : row % 17; };
        if (distinct) {
            std::set<size_t> seen;
            expected.erase(std::remove_if(expected.begin(), expected.end(),
                [&](size_t row) { return !seen.insert(key(row)).second; }), expected.end());
        }
        const size_t begin = std::min(offset, expected.size());
        size_t end = unbounded ? expected.size() : begin + std::min(limit, expected.size() - begin);
        if (ties && end > begin)
            while (end < expected.size() && key(expected[end]) == key(expected[end - 1])) ++end;
        expected = std::vector<size_t>(expected.begin() + begin, expected.begin() + end);

        bool good = plan->open();
        std::string display;
        size_t position = 0;
        while (good && plan->next(display)) {
            std::vector<ExprValue> cells;
            good = plan->lastStructuredValues(cells) && position < expected.size();
            if (!good) break;
            const size_t id = expected[position++];
            if (distinct) {
                good = cells.size() == 1 && cells[0].isNull == (id % 13 == 0) &&
                    (cells[0].isNull || cells[0].value == std::to_string(id % 17));
            } else {
                good = cells.size() == 3 && cells[0].value == std::to_string(id) &&
                    cells[1].isNull == (id % 13 == 0) &&
                    (cells[1].isNull || cells[1].value == std::to_string(id % 17)) &&
                    cells[2].isNull == (id % 29 == 0) &&
                    (cells[2].isNull || cells[2].value == std::string(width, 'a' + id % 23));
            }
        }
        good = good && !plan->hasError() && position == expected.size();
        plan->close();
        if (!unbounded && !limit)
            good = good && reads == 0 && eof == 0;
        else {
            good = good && reads == count && eof == 1;
            good = good && (requireBounded ? retained < 4 * 1024 * 1024
                                          : retained > count * width / 2);
        }
        ++checked;
        failures += !good;
        std::cout << "TOPN " << sql << " rows=" << position << " reads=" << reads
                  << " retained_bytes=" << retained << " passed=" << good << '\n';
    };
    run(20, 0, false, false, false, false, true, true);
    run(7, 13, false, false, false, false, true, true);
    run(1, 0, false, false, false, true, false, true);
    run(0, 500, false, false, false, false, true, true);
    run(20000, 0, false, false, false, false, true, false);
    run(5, 0, true, false, false, false, false, false);
    run(1, 0, false, true, false, false, false, false);
    run(0, 0, false, false, true, false, true, false);
    std::cout << "TOPN_MEMORY_CHECKED=" << checked << " FAILURES=" << failures << '\n';
    return failures ? 1 : 0;
}
