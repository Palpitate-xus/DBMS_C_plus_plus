#pragma once

#include "executor/window_frame.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <utility>
#include <vector>

namespace dbms::window_detail {

struct Span {
    size_t first = 0, last = 0; // half-open sorted positions
};

struct Lookup {
    enum class State { Found, Outside, NeedMore };
    State state = State::Outside;
    size_t source = 0;
    bool found() const { return state == State::Found; }
};

struct Frame {
    size_t first = 0, last = 0; // currently buffered portion of the frame
    bool headComplete = true, tailComplete = true;
    bool complete() const { return headComplete && tailComplete; }
};

enum class Exclusion { None, CurrentRow, Group, Ties };

// This kernel owns ordinal indexes only. Comparisons and key access borrow
// genuine typed rows from the execution owner; no datum or context is rendered,
// reconstructed, evaluated, or memoized here. All public positions are sorted
// positions; returned Lookup::source values identify original input rows.
class Layout {
    std::vector<size_t> order_, position_, partitionOf_, peerOf_, denseRank_;
    std::vector<Span> partitions_, groups_;
    bool complete_ = true;
    bool identity_ = false;
    bool orderedPrefix_ = false;
    bool initialized_ = false;
    size_t identitySize_ = 0;

    template<class Value>
    static void reserveGrowth(std::vector<Value>& values, size_t required) {
        if (required <= values.capacity()) return;
        const size_t maximum = values.max_size();
        if (required > maximum) throw std::length_error("window layout exceeds its index capacity");
        size_t capacity = std::max<size_t>(1, values.capacity());
        while (capacity < required) {
            if (capacity > maximum / 2) { capacity = required; break; }
            capacity *= 2;
        }
        values.reserve(capacity);
    }

    void requirePosition(size_t position) const {
        if (position >= size())
            throw DbError("XX000", "window position is outside its buffered input");
    }
    void requireFrame(size_t current, const Frame& frame) const {
        requirePosition(current);
        const auto span = partition(current);
        if (frame.first > frame.last || frame.first < span.first || frame.last > span.last)
            throw DbError("XX000", "window frame is outside its buffered partition");
    }
    template<class PartitionCompare, class OrderCompare>
    void index(PartitionCompare& partitionCompare, OrderCompare& orderCompare) {
        const size_t count = order_.size();
        position_.resize(count);
        partitionOf_.resize(count); peerOf_.resize(count); denseRank_.resize(count);
        for (size_t p = 0; p < count; ++p) position_[order_[p]] = p;
        for (size_t first = 0; first < count;) {
            size_t last = first + 1;
            while (last < count && partitionCompare(order_[first], order_[last]) == 0) ++last;
            const size_t partitionIndex = partitions_.size(); partitions_.push_back({first, last});
            size_t dense = 0;
            for (size_t group = first; group < last;) {
                size_t end = group + 1;
                while (end < last && orderCompare(order_[group], order_[end]) == 0) ++end;
                const size_t groupIndex = groups_.size(); groups_.push_back({group, end});
                ++dense;
                for (size_t p = group; p < end; ++p) {
                    partitionOf_[p] = partitionIndex; peerOf_[p] = groupIndex; denseRank_[p] = dense;
                }
                group = end;
            }
            first = last;
        }
    }

    bool groupBoundaryComplete(size_t current, WindowFrame::Bound kind,
                               const ExprValue& distance, bool end) const {
        using Bound = WindowFrame::Bound;
        if (kind == Bound::UnboundedPreceding) return true;
        if (kind == Bound::UnboundedFollowing) return partitionComplete(current);
        size_t group = current;
        if (WindowFrame::offsetBound(kind)) {
            const auto countValue = distance.asInt(); // bounds() validated it
            uint64_t count = static_cast<uint64_t>(countValue);
            const auto partitionSpan = partition(current);
            while (count) {
                if (kind == Bound::Preceding) {
                    if (peers(group).first == partitionSpan.first) return true;
                    group = peers(group).first - 1;
                } else {
                    if (peers(group).last == partitionSpan.last)
                        return partitionComplete(current);
                    group = peers(group).last;
                }
                --count;
            }
        }
        return !end || peersComplete(group);
    }

public:
    size_t size() const { return identity_ || orderedPrefix_ ? identitySize_ : order_.size(); }
    bool empty() const { return size() == 0; }
    bool complete() const { return complete_; }
    size_t sourceAt(size_t position) const { requirePosition(position); return identity_ || orderedPrefix_ ? position : order_[position]; }
    size_t positionOf(size_t source) const {
        if (source >= size())
            throw DbError("XX000", "window source row is outside its buffered input");
        return identity_ || orderedPrefix_ ? source : position_[source];
    }
    Span partition(size_t position) const {
        requirePosition(position); return identity_ ? Span{0, size()} : partitions_[partitionOf_[position]];
    }
    Span peers(size_t position) const {
        requirePosition(position); return identity_ ? Span{0, size()} : groups_[peerOf_[position]];
    }
    bool partitionComplete(size_t position) const {
        requirePosition(position); return complete_ || partition(position).last < size();
    }
    bool peersComplete(size_t position) const {
        requirePosition(position); return complete_ || peers(position).last < size();
    }
    size_t rank(size_t position) const {
        requirePosition(position); return peers(position).first - partition(position).first + 1;
    }
    size_t denseRank(size_t position) const { requirePosition(position); return identity_ ? 1 : denseRank_[position]; }
    size_t rowNumber(size_t position) const {
        requirePosition(position); return position - partition(position).first + 1;
    }

    // The caller supplies its existing interruptible stable sorter. The final
    // original ordinal tie-break is not part of partition/peer equality.
    // Build into a fresh object so throwing comparators/sorters leave *this intact.
    template<class PartitionCompare, class OrderCompare, class Sorter>
    void build(size_t count, PartitionCompare partitionCompare,
               OrderCompare orderCompare, Sorter sorter) {
        Layout next; next.initialized_ = true;
        next.order_.resize(count); std::iota(next.order_.begin(), next.order_.end(), size_t{0});
        sorter(next.order_.begin(), next.order_.end(), [&](size_t left, size_t right) {
            const int partitionOrder = partitionCompare(left, right);
            if (partitionOrder) return partitionOrder < 0;
            const int keyOrder = orderCompare(left, right);
            return keyOrder ? keyOrder < 0 : left < right;
        });
        next.index(partitionCompare, orderCompare);
        *this = std::move(next);
    }

    // The prefix is already sorted by this actual window's partition/order
    // keys. Only the final partition and final peer can still grow. The
    // comparison callbacks index this prefix's original typed input ordinals.
    template<class PartitionCompare, class OrderCompare>
    void buildOrderedPrefix(size_t count, bool complete,
                            PartitionCompare partitionCompare, OrderCompare orderCompare) {
        Layout next;
        next.appendOrderedPrefix(count, complete, partitionCompare, orderCompare);
        *this = std::move(next);
    }

    // Extend one fixed stage's already-sorted stream. The caller guarantees
    // unchanged comparisons and unchanged earlier typed rows. Each new row
    // makes one adjacent partition comparison and, within a partition, one
    // order comparison. Old row indexes are never rescanned or rewritten.
    // Interval tails and geometric vector growth make per-row lookahead
    // amortized linear in the total prefix, not quadratic in its row count.
    template<class PartitionCompare, class OrderCompare>
    void appendOrderedPrefix(size_t count, bool complete,
                             PartitionCompare partitionCompare, OrderCompare orderCompare) {
        if ((!orderedPrefix_ && initialized_) ||
            count < size() || (orderedPrefix_ && complete_ && (count > size() || !complete)))
            throw DbError("XX000", "ordered window prefix cannot replace or reopen its input");
        const size_t previous = size();
        if (count > partitionOf_.max_size() || count > peerOf_.max_size() || count > denseRank_.max_size())
            throw std::length_error("window layout exceeds its row index capacity");
        struct Boundary { bool partition, peer; };
        std::vector<Boundary> added;
        added.reserve(count - previous);
        size_t newPartitions = 0, newGroups = 0;
        // Run all potentially throwing callbacks before changing any visible
        // prefix or interval. Capacity changes during reserve are unobservable.
        for (size_t source = previous; source < count; ++source) {
            const bool partition = !source || partitionCompare(source - 1, source) != 0;
            const bool peer = partition || orderCompare(source - 1, source) != 0;
            added.push_back({partition, peer});
            newPartitions += partition; newGroups += peer;
        }
        reserveGrowth(partitionOf_, count); reserveGrowth(peerOf_, count); reserveGrowth(denseRank_, count);
        reserveGrowth(partitions_, partitions_.size() + newPartitions);
        reserveGrowth(groups_, groups_.size() + newGroups);
        size_t dense = previous ? denseRank_.back() : 0;
        for (size_t i = 0; i < added.size(); ++i) {
            const size_t source = previous + i;
            if (added[i].partition) { partitions_.push_back({source, source}); dense = 0; }
            if (added[i].peer) { groups_.push_back({source, source}); ++dense; }
            partitions_.back().last = source + 1; groups_.back().last = source + 1;
            partitionOf_.push_back(partitions_.size() - 1);
            peerOf_.push_back(groups_.size() - 1); denseRank_.push_back(dense);
        }
        orderedPrefix_ = true; initialized_ = true; identitySize_ = count; complete_ = complete;
    }
    void buildIdentityPrefix(size_t count, bool complete) {
        // Unordered streaming must not rebuild a vector for every emitted
        // row. Both this extension and all identity lookups are constant time.
        Layout next; next.identity_ = true; next.initialized_ = true;
        next.identitySize_ = count; next.complete_ = complete;
        *this = std::move(next);
    }

    Lookup partitionOffset(size_t current, int64_t distance) const {
        const auto span = partition(current);
        if (distance < 0) {
            // Avoid negating INT64_MIN and avoid unsigned/size_t wraparound.
            const uint64_t magnitude = static_cast<uint64_t>(-(distance + 1)) + 1;
            if (magnitude > current - span.first) return {Lookup::State::Outside, 0};
            return {Lookup::State::Found, sourceAt(current - static_cast<size_t>(magnitude))};
        }
        const uint64_t magnitude = static_cast<uint64_t>(distance);
        if (magnitude < span.last - current)
            return {Lookup::State::Found, sourceAt(current + static_cast<size_t>(magnitude))};
        return {partitionComplete(current) ? Lookup::State::Outside : Lookup::State::NeedMore, 0};
    }

    // key(sourceOrdinal) returns the owning typed datum, including metadata;
    // it is invoked only if the existing WindowFrame math needs a RANGE key.
    // The frame must already have its real/default bound kinds and initialized
    // offsets. Offset initialization/evaluation belongs to the execution owner.
    template<class Key>
    Frame frameBounds(size_t current, const WindowFrame& frame, Key key,
                      bool ascending = true, bool nullsFirst = false) const {
        using Bound = WindowFrame::Bound;
        const auto span = partition(current);
        const auto bounds = frame.bounds(current, span.first, span.last,
            [&](size_t p) { const auto group = peers(p); return std::make_pair(group.first, group.last); },
            [&](size_t p) { return key(sourceAt(p)); }, ascending, nullsFirst);
        Frame result{bounds.first, bounds.second, true, true};
        if (partitionComplete(current)) return result;
        const auto known = [&](Bound kind, const ExprValue& distance, bool end, size_t boundary) {
            if (kind == Bound::UnboundedPreceding) return true;
            if (kind == Bound::UnboundedFollowing) return false;
            if (frame.mode == "rows") {
                if (kind != Bound::Following) return true;
                const auto count = static_cast<uint64_t>(distance.asInt());
                const size_t available = span.last - current - (end ? 1 : 0);
                return count <= available;
            }
            if (frame.mode == "groups") return groupBoundaryComplete(current, kind, distance, end);
            if (kind == Bound::CurrentRow) return !end || peersComplete(current);
            // For an offset RANGE scan, a buffered stopping tuple proves the
            // boundary. Reaching the uncompleted partition end does not.
            return boundary < span.last;
        };
        result.headComplete = known(frame.start, frame.startOffset, false, bounds.first);
        result.tailComplete = known(frame.end, frame.endOffset, true, bounds.second);
        return result;
    }

    bool frameContains(size_t current, const Frame& frame, size_t member,
                       Exclusion exclusion = Exclusion::None) const {
        requireFrame(current, frame);
        if (!frame.headComplete || member < frame.first || member >= frame.last) return false;
        if (exclusion == Exclusion::CurrentRow) return member != current;
        const auto group = peers(current);
        const bool peer = member >= group.first && member < group.last;
        if (exclusion == Exclusion::Group) return !peer;
        if (exclusion == Exclusion::Ties) return !peer || member == current;
        return true;
    }

    Lookup frameNth(size_t current, const Frame& frame, uint64_t number,
                    bool fromEnd = false, Exclusion exclusion = Exclusion::None) const {
        requireFrame(current, frame);
        if (!number) throw DbError("XX000", "window frame ordinal must be positive");
        if (!frame.headComplete || (fromEnd && !frame.tailComplete)) return {Lookup::State::NeedMore, 0};
        uint64_t seen = 0;
        if (fromEnd) {
            for (size_t p = frame.last; p > frame.first;) {
                --p;
                if (frameContains(current, frame, p, exclusion) && ++seen == number)
                    return {Lookup::State::Found, sourceAt(p)};
            }
        } else {
            for (size_t p = frame.first; p < frame.last; ++p)
                if (frameContains(current, frame, p, exclusion) && ++seen == number)
                    return {Lookup::State::Found, sourceAt(p)};
        }
        // Do not infer EOF from exclusions or from a buffered prefix. The
        // actual host decides when PostgreSQL's source demand requires more.
        return {frame.tailComplete ? Lookup::State::Outside : Lookup::State::NeedMore, 0};
    }
};

} // namespace dbms::window_detail
