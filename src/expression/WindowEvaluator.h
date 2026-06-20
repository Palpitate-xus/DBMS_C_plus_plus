// ============================================================================
// Window Function Evaluator — Phase 4 Wave 2 skeleton
//
// Provides in-memory evaluation of common window functions over a vector of
// rows.  Each row exposes partition keys and order keys.  This is a skeletal
// implementation: it supports row_number, rank, dense_rank, lag and lead with
// a ROWS-style frame.  RANGE / GROUPS frames and named windows are left for
// later phases.
// ============================================================================

#pragma once

#include "expression/ExprEvaluator.h"
#include <algorithm>
#include <vector>

namespace dbms {

struct WindowRow {
    std::vector<ExprValue> partitionKeys;
    std::vector<ExprValue> orderKeys;
};

class WindowEvaluator {
public:
    static std::vector<ExprValue> rowNumber(const std::vector<WindowRow>& rows) {
        std::vector<ExprValue> out(rows.size());
        if (rows.empty()) return out;

        std::vector<size_t> idx(rows.size());
        for (size_t i = 0; i < idx.size(); ++i) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&rows](size_t a, size_t b) { return compareRows(rows[a], rows[b]) < 0; });

        size_t partitionStartIdx = 0;
        for (size_t i = 0; i < idx.size(); ++i) {
            if (i > 0 && comparePartition(rows[idx[i]], rows[idx[i - 1]]) != 0) {
                partitionStartIdx = i;
            }
            out[idx[i]] = ExprValue("bigint", std::to_string(i - partitionStartIdx + 1), false);
        }
        return out;
    }

    static std::vector<ExprValue> rank(const std::vector<WindowRow>& rows) {
        return rankInternal(rows, false);
    }

    static std::vector<ExprValue> denseRank(const std::vector<WindowRow>& rows) {
        return rankInternal(rows, true);
    }

    static std::vector<ExprValue> lag(const std::vector<WindowRow>& rows,
                                        size_t offset,
                                        const ExprValue& defaultVal) {
        std::vector<ExprValue> out(rows.size(), defaultVal);
        if (rows.empty()) return out;

        std::vector<size_t> idx(rows.size());
        for (size_t i = 0; i < idx.size(); ++i) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&rows](size_t a, size_t b) { return compareRows(rows[a], rows[b]) < 0; });

        size_t prevPartitionStart = 0;
        for (size_t i = 0; i < idx.size(); ++i) {
            if (i > 0 && comparePartition(rows[idx[i]], rows[idx[i - 1]]) != 0) {
                prevPartitionStart = i;
            }
            if (i >= prevPartitionStart + offset) {
                out[idx[i]] = ExprValue(defaultVal.typeName,
                                        rows[idx[i - offset]].orderKeys.empty()
                                            ? defaultVal.value
                                            : rows[idx[i - offset]].orderKeys.front().value,
                                        false);
            }
        }
        return out;
    }

    static std::vector<ExprValue> lead(const std::vector<WindowRow>& rows,
                                         size_t offset,
                                         const ExprValue& defaultVal) {
        std::vector<ExprValue> out(rows.size(), defaultVal);
        if (rows.empty()) return out;

        std::vector<size_t> idx(rows.size());
        for (size_t i = 0; i < idx.size(); ++i) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&rows](size_t a, size_t b) { return compareRows(rows[a], rows[b]) < 0; });

        size_t partitionStart = 0;
        for (size_t i = 0; i < idx.size(); ++i) {
            if (i > 0 && comparePartition(rows[idx[i]], rows[idx[i - 1]]) != 0) {
                partitionStart = i;
            }
            size_t nextPos = i + offset;
            if (nextPos < idx.size() &&
                comparePartition(rows[idx[nextPos]], rows[idx[partitionStart]]) == 0) {
                out[idx[i]] = ExprValue(defaultVal.typeName,
                                        rows[idx[nextPos]].orderKeys.empty()
                                            ? defaultVal.value
                                            : rows[idx[nextPos]].orderKeys.front().value,
                                        false);
            }
        }
        return out;
    }

private:
    static int comparePartition(const WindowRow& a, const WindowRow& b) {
        size_t n = std::min(a.partitionKeys.size(), b.partitionKeys.size());
        for (size_t i = 0; i < n; ++i) {
            int c = compareValues(a.partitionKeys[i], b.partitionKeys[i]);
            if (c != 0) return c;
        }
        if (a.partitionKeys.size() < b.partitionKeys.size()) return -1;
        if (a.partitionKeys.size() > b.partitionKeys.size()) return 1;
        return 0;
    }

    static int compareRows(const WindowRow& a, const WindowRow& b) {
        int p = comparePartition(a, b);
        if (p != 0) return p;
        size_t n = std::min(a.orderKeys.size(), b.orderKeys.size());
        for (size_t i = 0; i < n; ++i) {
            int c = compareValues(a.orderKeys[i], b.orderKeys[i]);
            if (c != 0) return c;
        }
        if (a.orderKeys.size() < b.orderKeys.size()) return -1;
        if (a.orderKeys.size() > b.orderKeys.size()) return 1;
        return 0;
    }

    static std::vector<ExprValue> rankInternal(const std::vector<WindowRow>& rows,
                                                  bool dense) {
        std::vector<ExprValue> out(rows.size());
        if (rows.empty()) return out;

        std::vector<size_t> idx(rows.size());
        for (size_t i = 0; i < idx.size(); ++i) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&rows](size_t a, size_t b) { return compareRows(rows[a], rows[b]) < 0; });

        size_t rank = 1;
        size_t partitionStartPos = 1;
        for (size_t i = 0; i < idx.size(); ++i) {
            if (i > 0 && comparePartition(rows[idx[i]], rows[idx[i - 1]]) != 0) {
                rank = 1;
                partitionStartPos = i + 1;
            } else if (i > 0 && compareRows(rows[idx[i]], rows[idx[i - 1]]) != 0) {
                if (dense) {
                    ++rank;
                } else {
                    rank = (i + 1) - partitionStartPos + 1;
                }
            }
            out[idx[i]] = ExprValue("bigint", std::to_string(rank), false);
        }
        return out;
    }

    // Lightweight value comparison reused from ExprEvaluator internals.
    static int compareValues(const ExprValue& a, const ExprValue& b) {
        if (a.isNull && b.isNull) return 0;
        if (a.isNull) return -1;
        if (b.isNull) return 1;
        // Numeric when possible
        bool aNum = isNumeric(a.value);
        bool bNum = isNumeric(b.value);
        if (aNum && bNum) {
            double da = std::stod(a.value);
            double db = std::stod(b.value);
            if (da < db) return -1;
            if (da > db) return 1;
            return 0;
        }
        if (a.value < b.value) return -1;
        if (a.value > b.value) return 1;
        return 0;
    }

    static bool isNumeric(const std::string& s) {
        if (s.empty()) return false;
        char* end = nullptr;
        std::strtod(s.c_str(), &end);
        return end != s.c_str() && *end == '\0';
    }
};

} // namespace dbms
