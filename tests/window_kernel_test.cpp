#include "executor/WindowKernel.h"
#include <algorithm>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dbms;
using namespace dbms::window_detail;

namespace {
size_t checks = 0, failures = 0;
void check(bool valid, const std::string& label) {
    ++checks;
    if (!valid) { ++failures; std::cerr << "WINDOW_KERNEL_FAILURE " << label << '\n'; }
}
bool lookup(const Lookup& value, Lookup::State state, size_t source = 0) {
    return value.state == state && (state != Lookup::State::Found || value.source == source);
}
bool span(const Span& value, size_t first, size_t last) {
    return value.first == first && value.last == last;
}
bool frameIs(const Frame& value, size_t first, size_t last, bool head = true, bool tail = true) {
    return value.first == first && value.last == last &&
        value.headComplete == head && value.tailComplete == tail;
}
WindowFrame frame(std::string mode, WindowFrame::Bound start, WindowFrame::Bound end,
                  std::string startOffset = "0", std::string endOffset = "0") {
    WindowFrame value; value.mode = std::move(mode); value.start = start; value.end = end;
    value.startOffset = ExprValue(value.mode == "range" ? "numeric" : "bigint", std::move(startOffset));
    value.endOffset = ExprValue(value.mode == "range" ? "numeric" : "bigint", std::move(endOffset));
    return value;
}
template<class Function> bool state(Function function, const std::string& expected) {
    try { function(); } catch (const DbError& error) { return error.sqlState() == expected; }
    return false;
}
}

int main() {
    using Bound = WindowFrame::Bound;
    using Status = Lookup::State;
    struct Row { int partition; std::optional<int> key; };
    const std::vector<Row> rows{{1,2},{0,1},{1,std::nullopt},{0,1},
                               {1,2},{0,0},{1,1},{0,std::nullopt}};
    const auto part = [&](size_t a, size_t b) { return (rows[a].partition > rows[b].partition) - (rows[a].partition < rows[b].partition); };
    const auto order = [&](size_t a, size_t b) {
        const auto& x = rows[a].key; const auto& y = rows[b].key;
        if (bool(x) != bool(y)) return x ? -1 : 1;
        return !x ? 0 : (*x > *y) - (*x < *y);
    };
    size_t sorterCalls = 0;
    Layout layout;
    layout.build(rows.size(), part, order, [&](auto first, auto last, auto less) {
        ++sorterCalls; std::stable_sort(first, last, less);
    });
    const std::vector<size_t> expected{5,1,3,7,6,0,4,2};
    const std::vector<size_t> peerFirst{0,1,1,3,4,5,5,7}, peerLast{1,3,3,4,5,7,7,8};
    const std::vector<size_t> ranks{1,2,2,4,1,2,2,4}, dense{1,2,2,3,1,2,2,3};
    // 48 controls: source identity, partitions, ties, sparse and dense ranks.
    for (size_t p = 0; p < expected.size(); ++p) {
        check(layout.sourceAt(p) == expected[p], "stable source ordinal " + std::to_string(p));
        check(layout.positionOf(expected[p]) == p, "inverse source position " + std::to_string(p));
        check(span(layout.partition(p), p < 4 ? 0 : 4, p < 4 ? 4 : 8), "partition " + std::to_string(p));
        check(span(layout.peers(p), peerFirst[p], peerLast[p]), "all peer keys " + std::to_string(p));
        check(layout.rank(p) == ranks[p], "sparse rank " + std::to_string(p));
        check(layout.denseRank(p) == dense[p], "dense rank " + std::to_string(p));
    }
    // 56 controls: no unsigned wraparound, no cross-partition target, including INT64_MIN.
    const std::vector<int64_t> offsets{-9,-1,0,1,9,std::numeric_limits<int64_t>::min(),std::numeric_limits<int64_t>::max()};
    for (size_t p = 0; p < expected.size(); ++p) for (const auto distance : offsets) {
        const auto first = p < 4 ? size_t{0} : size_t{4}; const auto last = first + 4;
        const bool found = distance >= -static_cast<int64_t>(p - first) && distance < static_cast<int64_t>(last - p);
        check(lookup(layout.partitionOffset(p, distance), found ? Status::Found : Status::Outside,
            found ? expected[static_cast<size_t>(static_cast<int64_t>(p) + distance)] : 0), "relative partition seek");
    }
    check(sorterCalls == 1, "actual injected sorter invoked once");
    check(layout.complete() && layout.partitionComplete(7) && layout.peersComplete(7), "full materialization completion");
    bool sorterThrew = false;
    try { layout.build(3, part, order, [](auto, auto, auto) { throw std::runtime_error("primary sort failure"); }); }
    catch (const std::runtime_error& error) { sorterThrew = std::string(error.what()) == "primary sort failure"; }
    check(sorterThrew && layout.size() == 8 && layout.sourceAt(1) == 1, "throwing build preserves original valid layout");
    check(state([&] { layout.buildOrderedPrefix(8, false,
        [](size_t, size_t) -> int { throw DbError("22P02", "primary comparison failure"); }, order); }, "22P02") &&
        layout.size() == 8 && layout.complete() && layout.sourceAt(0) == 5, "throwing index comparison preserves primary error and prior layout");
    check(state([&] { (void)layout.sourceAt(8); }, "XX000"), "invalid sorted position rejected");
    check(state([&] { (void)layout.positionOf(8); }, "XX000"), "invalid original row rejected");
    const auto noKey = [](size_t) -> ExprValue { throw std::runtime_error("ROWS/GROUPS must not demand an ORDER datum"); };
    const auto whole = frame("rows", Bound::UnboundedPreceding, Bound::UnboundedFollowing);
    const auto full = layout.frameBounds(2, whole, noKey);
    check(frameIs(full, 0, 4), "frame limited to actual partition");
    check(lookup(layout.frameNth(2, full, 1, false, Exclusion::Group), Status::Found, 5), "group exclusion first nonpeer");
    check(lookup(layout.frameNth(2, full, 2, false, Exclusion::Group), Status::Found, 7), "group exclusion second nonpeer");
    check(lookup(layout.frameNth(2, full, 3, false, Exclusion::Group), Status::Outside), "group exclusion genuine empty remainder");
    check(lookup(layout.frameNth(2, full, 2, false, Exclusion::Ties), Status::Found, 3), "ties exclusion retains actual current source");
    check(lookup(layout.frameNth(2, full, 2, false, Exclusion::CurrentRow), Status::Found, 1), "current exclusion retains other peer");
    check(lookup(layout.frameNth(2, full, 1, true, Exclusion::Group), Status::Found, 7), "last value counts only included members");
    check(!layout.frameContains(2, full, 1, Exclusion::Group) && layout.frameContains(2, full, 2, Exclusion::Ties), "frame membership uses original peer group");
    check(state([&] { (void)layout.frameNth(2, full, 0); }, "XX000"), "kernel rejects invalid zero ordinal");
    check(state([&] { (void)layout.frameNth(2, Frame{0,8,true,true}, 1); }, "XX000"), "kernel rejects frame crossing an actual partition");
    const auto groups = layout.frameBounds(2, frame("groups", Bound::Preceding, Bound::CurrentRow, "1"), noKey);
    check(frameIs(groups, 0, 3), "GROUPS counts peer groups rather than rows");
    check(frameIs(layout.frameBounds(2, frame("groups", Bound::Following, Bound::Following, "1", "1"), noKey), 3, 4), "GROUPS following bounds");
    check(state([&] { (void)layout.frameBounds(2, frame("rows", Bound::Preceding, Bound::CurrentRow, "-1"), noKey); }, "22013"), "negative ROWS distance preserved");
    auto nullOffset = frame("rows", Bound::Preceding, Bound::CurrentRow); nullOffset.startOffset.isNull = true;
    check(state([&] { (void)layout.frameBounds(2, nullOffset, noKey); }, "22004"), "NULL distance preserved");

    Layout prefix; prefix.buildIdentityPrefix(4, false);
    check(prefix.size() == 4 && !prefix.complete() && !prefix.partitionComplete(2) && !prefix.peersComplete(2), "unfinished single partition and peer");
    check(prefix.rowNumber(2) == 3 && prefix.rank(2) == 1 && prefix.denseRank(2) == 1, "prefix ranks need no EOF");
    check(lookup(prefix.partitionOffset(2, -2), Status::Found, 0), "lag can use known prefix");
    check(lookup(prefix.partitionOffset(2, -3), Status::Outside), "lag before known partition head is outside");
    check(lookup(prefix.partitionOffset(2, 0), Status::Found, 2), "zero offset current row");
    check(lookup(prefix.partitionOffset(2, 2), Status::NeedMore), "lead distinguishes not-yet-read row from EOF");
    check(lookup(prefix.partitionOffset(2, std::numeric_limits<int64_t>::min()), Status::Outside), "minimum signed lag distance");
    check(lookup(prefix.partitionOffset(2, std::numeric_limits<int64_t>::max()), Status::NeedMore), "maximum signed lead distance");
    const auto preceding = prefix.frameBounds(2, frame("rows", Bound::Preceding, Bound::CurrentRow, "1"), noKey);
    check(frameIs(preceding, 1, 3), "finite ROWS frame can finish before input EOF");
    check(lookup(prefix.frameNth(2, preceding, 1), Status::Found, 1), "prefix ROWS first");
    check(lookup(prefix.frameNth(2, preceding, 1, true), Status::Found, 2), "prefix ROWS last");
    check(lookup(prefix.frameNth(2, preceding, 3), Status::Outside), "completed finite ROWS frame outside");
    const auto following = prefix.frameBounds(2, frame("rows", Bound::CurrentRow, Bound::Following, "0", "2"), noKey);
    check(frameIs(following, 2, 4, true, false), "ROWS future tail remains unknown");
    check(lookup(prefix.frameNth(2, following, 2), Status::Found, 3), "known future member can be selected");
    check(lookup(prefix.frameNth(2, following, 3), Status::NeedMore), "unread finite-frame member requests lookahead");
    check(lookup(prefix.frameNth(2, following, 1, true), Status::NeedMore), "last value waits for true finite tail");
    const auto unread = prefix.frameBounds(2, frame("rows", Bound::Following, Bound::Following, "3", "4"), noKey);
    check(frameIs(unread, 4, 4, false, false) && lookup(prefix.frameNth(2, unread, 1), Status::NeedMore), "unread frame head cannot be treated as empty");
    const auto defaultFrame = frame("range", Bound::UnboundedPreceding, Bound::CurrentRow);
    const auto growing = prefix.frameBounds(2, defaultFrame, noKey);
    check(frameIs(growing, 0, 4, true, false), "unordered default RANGE whole peer is unfinished");
    check(lookup(prefix.frameNth(2, growing, 1), Status::Found, 0), "first value can stream without whole partition");
    check(lookup(prefix.frameNth(2, growing, 4), Status::Found, 3), "nth value can use exact read prefix");
    check(lookup(prefix.frameNth(2, growing, 5), Status::NeedMore), "nth beyond prefix is pending rather than NULL");
    check(lookup(prefix.frameNth(2, growing, 1, true), Status::NeedMore), "last value requires true peer tail");
    check(lookup(prefix.frameNth(2, growing, 1, false, Exclusion::Group), Status::NeedMore), "exclusion does not invent input completion");
    check(lookup(prefix.frameNth(2, growing, 2, false, Exclusion::Ties), Status::NeedMore), "ties exclusion retains actual source-demand boundary");
    check(lookup(prefix.frameNth(2, growing, 3, false, Exclusion::CurrentRow), Status::Found, 3), "current-row exclusion counts actual remaining prefix");
    prefix.buildIdentityPrefix(7, false);
    const auto expanded = prefix.frameBounds(2, defaultFrame, noKey);
    check(prefix.rowNumber(2) == 3 && prefix.sourceAt(3) == 3 && span(prefix.peers(2), 0, 7), "expanding prefix retains genuine source identity");
    check(lookup(prefix.frameNth(2, expanded, 1), Status::Found, 0), "first source remains stable after lookahead");
    check(lookup(prefix.frameNth(2, expanded, 5), Status::Found, 4), "lookahead resolves previously pending nth");
    prefix.buildIdentityPrefix(7, true);
    const auto finished = prefix.frameBounds(2, defaultFrame, noKey);
    check(finished.complete() && lookup(prefix.frameNth(2, finished, 1, true), Status::Found, 6), "real EOF resolves last source");
    check(lookup(prefix.frameNth(2, finished, 8), Status::Outside), "real EOF resolves absent nth");
    check(lookup(prefix.partitionOffset(2, 10), Status::Outside), "real EOF resolves absent lead target");
    Layout empty; empty.buildIdentityPrefix(0, false);
    check(empty.empty() && !empty.complete(), "empty incomplete prefix");
    empty.buildIdentityPrefix(0, true);
    check(empty.empty() && empty.complete(), "empty real EOF");

    // Already-sorted staged input: prior partition/peers finish independently
    // of the still-open final partition. No second sort or upstream eval.
    const auto sortedPart = [&](size_t a, size_t b) { return part(expected[a], expected[b]); };
    const auto sortedOrder = [&](size_t a, size_t b) { return order(expected[a], expected[b]); };
    Layout staged; staged.buildOrderedPrefix(6, false, sortedPart, sortedOrder);
    check(span(staged.partition(0), 0, 4) && staged.partitionComplete(0), "next partition proves preceding partition EOF");
    check(staged.peersComplete(3), "next partition also completes final preceding peer");
    check(span(staged.partition(5), 4, 6) && !staged.partitionComplete(5), "last staged partition can grow");
    check(staged.peersComplete(4) && !staged.peersComplete(5), "new peer completes earlier peer without whole partition");
    check(lookup(staged.partitionOffset(2, 9), Status::Outside), "previous partition cannot leak next partition rows");
    check(lookup(staged.partitionOffset(4, 2), Status::NeedMore), "open staged partition future target");
    check(lookup(staged.partitionOffset(4, -1), Status::Outside), "staged partition preceding head");
    check(frameIs(staged.frameBounds(4, defaultFrame, noKey), 4, 5), "peer lookahead completes default frame");
    check(frameIs(staged.frameBounds(5, defaultFrame, noKey), 4, 6, true, false), "last staged peer remains unfinished");
    const auto futureGroup = staged.frameBounds(4, frame("groups", Bound::Following, Bound::Following, "1", "1"), noKey);
    check(frameIs(futureGroup, 5, 6, true, false), "known following group start and unknown group end");
    check(lookup(staged.frameNth(4, futureGroup, 1), Status::Found, 5), "following group first can stream");
    check(lookup(staged.frameNth(4, futureGroup, 2), Status::NeedMore), "following group unread peer");
    check(lookup(staged.frameNth(4, futureGroup, 1, true), Status::NeedMore), "following group last requires peer EOF");
    const auto absentGroup = staged.frameBounds(4, frame("groups", Bound::Following, Bound::Following, "2", "2"), noKey);
    check(!absentGroup.headComplete && lookup(staged.frameNth(4, absentGroup, 1), Status::NeedMore), "unread next group is not outside partition");
    check(frameIs(staged.frameBounds(5, frame("groups", Bound::Preceding, Bound::Preceding, "1", "1"), noKey), 4, 5), "preceding group is complete inside unfinished partition");
    staged.buildOrderedPrefix(8, true, sortedPart, sortedOrder);
    check(staged.complete() && staged.sourceAt(7) == 7 && staged.partitionComplete(5), "staged input closes at genuine EOF");

    // RANGE must consume the exact typed NUMERIC datum, never rounded double.
    const std::vector<ExprValue> exact{ExprValue("numeric","9007199254740992.90"),
        ExprValue("numeric","9007199254740993.00"),ExprValue("numeric","9007199254740993.10"),
        ExprValue("numeric","9007199254740993.20")};
    Layout numeric; const auto equal = [](size_t, size_t) { return 0; };
    const auto numericOrder = [&](size_t a, size_t b) {
        const Numeric x(exact[a].value), y(exact[b].value); return x < y ? -1 : y < x ? 1 : 0;
    };
    numeric.buildOrderedPrefix(4, false, equal, numericOrder);
    size_t keyReads = 0;
    const auto typedKey = [&](size_t source) { ++keyReads; return exact.at(source); };
    check(frameIs(numeric.frameBounds(2, frame("range", Bound::Preceding, Bound::CurrentRow, "0.1"), typedKey), 1, 3), "exact typed RANGE decimal distance beyond double precision");
    check(keyReads > 0 && exact[2].typeName == "numeric" && exact[2].value == "9007199254740993.10", "typed accessor is actually demanded without mutating datum");
    check(frameIs(numeric.frameBounds(1, frame("range", Bound::CurrentRow, Bound::Following, "0", "0.1"), typedKey), 1, 3), "buffered stopping key proves offset RANGE tail");
    check(frameIs(numeric.frameBounds(3, frame("range", Bound::CurrentRow, Bound::Following, "0", "0.1"), typedKey), 3, 4, true, false), "RANGE reaching open partition end requests lookahead");

    // Keep the original 177 controls above intact. These exercise actual
    // one-row lookahead extensions, not repeated rebuilding of a full index.
    size_t partitionCalls = 0, orderCalls = 0;
    const auto countedPart = [&](size_t a, size_t b) { ++partitionCalls; return sortedPart(a, b); };
    const auto countedOrder = [&](size_t a, size_t b) { ++orderCalls; return sortedOrder(a, b); };
    Layout incremental; incremental.buildOrderedPrefix(0, false, countedPart, countedOrder);
    check(incremental.empty() && !incremental.complete() && !partitionCalls && !orderCalls, "empty staged prefix evaluates no keys");
    for (size_t n = 1; n <= expected.size(); ++n) {
        const auto previousPart = partitionCalls, previousOrder = orderCalls;
        incremental.appendOrderedPrefix(n, n == expected.size(), countedPart, countedOrder);
        check(partitionCalls - previousPart == (n == 1 ? 0 : 1), "append compares only one new adjacent partition boundary");
        const bool samePart = n > 1 && sortedPart(n - 2, n - 1) == 0;
        check(orderCalls - previousOrder == (samePart ? 1 : 0), "append compares ORDER only within actual partition");
        Layout rebuilt; rebuilt.buildOrderedPrefix(n, n == expected.size(), sortedPart, sortedOrder);
        bool equivalent = true;
        for (size_t p = 0; p < n; ++p) {
            const auto a = incremental.partition(p), b = rebuilt.partition(p);
            const auto x = incremental.peers(p), y = rebuilt.peers(p);
            equivalent = equivalent && span(a,b.first,b.last) && span(x,y.first,y.last) &&
                incremental.rank(p) == rebuilt.rank(p) && incremental.denseRank(p) == rebuilt.denseRank(p) &&
                incremental.rowNumber(p) == rebuilt.rowNumber(p) &&
                incremental.partitionComplete(p) == rebuilt.partitionComplete(p) &&
                incremental.peersComplete(p) == rebuilt.peersComplete(p);
        }
        check(equivalent, "all previously emitted rows retain correct partition and peer views after append");
        check(incremental.sourceAt(n - 1) == n - 1 && incremental.positionOf(n - 1) == n - 1, "staged append retains actual source occurrence identity");
    }
    const auto beforeFinalPart = partitionCalls, beforeFinalOrder = orderCalls;
    incremental.appendOrderedPrefix(8, true, countedPart, countedOrder);
    check(partitionCalls == beforeFinalPart && orderCalls == beforeFinalOrder && incremental.complete(), "duplicate real EOF evaluates no keys");
    check(state([&] { incremental.appendOrderedPrefix(9, true, countedPart, countedOrder); }, "XX000") &&
        incremental.size() == 8 && incremental.complete(), "append cannot reopen a completed source");
    Layout recover; recover.buildOrderedPrefix(4, false, sortedPart, sortedOrder);
    check(state([&] { recover.appendOrderedPrefix(3, false, sortedPart, sortedOrder); }, "XX000") &&
        recover.size() == 4 && !recover.complete(), "append cannot replace old typed input with a shorter prefix");
    check(state([&] { recover.appendOrderedPrefix(6, false, sortedPart, [&](size_t a,size_t b) {
        if (b == 5) throw DbError("22003", "primary new-peer comparison failure");
        return sortedOrder(a,b);
    }); }, "22003") && recover.size() == 4 && span(recover.partition(0),0,4) && !recover.partitionComplete(0),
        "throw after classifying a new partition leaves the old partial prefix unchanged");
    recover.appendOrderedPrefix(6, false, sortedPart, sortedOrder);
    check(recover.partitionComplete(0) && span(recover.partition(5),4,6) &&
        span(recover.peers(5),5,6) && !recover.peersComplete(5), "successful retry appends only real new rows after primary error");
    size_t completionComparisons = 0;
    const auto noCompare = [&](size_t,size_t) { ++completionComparisons; return 0; };
    recover.appendOrderedPrefix(6, true, noCompare, noCompare);
    check(!completionComparisons && recover.complete() && recover.partitionComplete(5) && recover.peersComplete(5), "EOF marker closes current intervals without rescanning typed keys");
    check(state([&] { prefix.appendOrderedPrefix(8, false, sortedPart, sortedOrder); }, "XX000") &&
        prefix.size() == 7 && prefix.complete(), "ordered append cannot silently reinterpret an identity stage");

    Layout large; size_t largePartCalls = 0, largeOrderCalls = 0;
    const auto onePart = [&](size_t,size_t) { ++largePartCalls; return 0; };
    const auto triples = [&](size_t a,size_t b) {
        ++largeOrderCalls; return a/3 < b/3 ? -1 : a/3 > b/3 ? 1 : 0;
    };
    for (size_t n = 1; n <= 1025; ++n) large.appendOrderedPrefix(n, false, onePart, triples);
    check(largePartCalls == 1024 && largeOrderCalls == 1024, "1025 on-demand extensions perform exactly linear key comparisons");
    check(span(large.partition(0),0,1025) && !large.partitionComplete(0) &&
        span(large.peers(0),0,3) && large.peersComplete(0), "old row intervals see the grown partition without rewriting old rows");
    check(large.rank(1024) == 1024 && large.denseRank(1024) == 342 &&
        span(large.peers(1024),1023,1025) && !large.peersComplete(1024), "final partial peer retains actual rank and unfinished tail");
    large.appendOrderedPrefix(1025, true, onePart, triples);
    check(largePartCalls == 1024 && largeOrderCalls == 1024 && large.peersComplete(1024) &&
        lookup(large.partitionOffset(1024,1),Status::Outside), "full EOF closes grown partition without another comparison");
    Layout allocationGuard; bool rejectedExtent = false;
    try { allocationGuard.appendOrderedPrefix(std::numeric_limits<size_t>::max(), false, onePart, triples); }
    catch (const std::length_error&) { rejectedExtent = true; }
    check(rejectedExtent && allocationGuard.empty() && allocationGuard.complete() &&
        largePartCalls == 1024 && largeOrderCalls == 1024, "unallocatable new extent is rejected before callbacks or visible mutation");
    Layout closedEmpty;
    closedEmpty.build(0, sortedPart, sortedOrder, [](auto first,auto last,auto less) { std::stable_sort(first,last,less); });
    check(state([&] { closedEmpty.appendOrderedPrefix(1,false,sortedPart,sortedOrder); }, "XX000") &&
        closedEmpty.empty() && closedEmpty.complete(), "empty fully materialized EOF cannot be mistaken for an uninitialized stage");

    constexpr size_t expectedChecks = 223;
    std::cout << "WINDOW_KERNEL_COMPLETE " << checks << " FAILURES " << failures << '\n';
    if (checks != expectedChecks) std::cerr << "WINDOW_KERNEL_COUNT expected " << expectedChecks << " actual " << checks << '\n';
    return checks == expectedChecks && failures == 0 ? 0 : 1;
}
