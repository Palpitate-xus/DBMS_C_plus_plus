#pragma once

#include "access/BPTree.h"
#include "interfaces/table_schema.h"

namespace dbms::primary_key_index {

inline std::optional<size_t> singleKeyColumn(const TableSchema& table) {
    size_t column = table.len();
    if (!table.pkColIndices.empty()) {
        if (table.pkColIndices.size() != 1) return {};
        column = table.pkColIndices.front();
    } else {
        for (size_t i = 0; i < table.len(); ++i) {
            if (!table.cols[i].isPrimaryKey) continue;
            if (column != table.len()) return {};
            column = i;
        }
    }
    if (column >= table.len()) return {};
    return column;
}

inline bool isCompoundKey(const TableSchema& table) {
    if (table.partitionType != TableSchema::PartitionType::None) return false;
    if (!table.pkColIndices.empty()) return table.pkColIndices.size() > 1;
    size_t count = 0;
    for (size_t i = 0; i < table.len(); ++i) {
        if (table.cols[i].isPrimaryKey && ++count == 2) return true;
    }
    return false;
}

inline std::optional<size_t> textKeyColumn(const TableSchema& table) {
    if (table.partitionType != TableSchema::PartitionType::None) return {};
    const auto column = singleKeyColumn(table);
    if (!column) return {};
    const auto& key = table.cols[*column];
    const auto& type = key.dataType;
    const bool text = !key.isArray && key.enumValues.empty() &&
        (type == "text" || type == "varchar" || type == "character varying" ||
         type == "char" || type == "character" || type == "bpchar");
    return text ? column : std::nullopt;
}

inline bool isSingleTextKey(const TableSchema& table) {
    return textKeyColumn(table).has_value();
}

inline bool sharesPrefix(const TableSchema& table, const std::string& key) {
    // Compound keys can also share a short physical key when a column
    // contains the separator used by the original disk encoding.
    return isCompoundKey(table) || (key.size() >= BP_KEY_LEN && isSingleTextKey(table));
}

inline bool lookupNeedsHeap(const TableSchema& table, const BPTree& index,
                            const std::string& key) {
    return sharesPrefix(table, key) && !index.hasCompletePrimaryPrefixes();
}

inline bool insert(const TableSchema& table, BPTree& index,
                   const std::string& key, int64_t rid) {
    return sharesPrefix(table, key) ? index.insertMulti(key, rid) : index.insert(key, rid);
}

inline bool remove(const TableSchema& table, BPTree& index,
                   const std::string& key, int64_t rid, bool undo = false) {
    if (sharesPrefix(table, key)) {
        std::vector<int64_t> candidates;
        if (!index.searchMultiChecked(key, candidates)) return false;
        if (std::find(candidates.begin(), candidates.end(), rid) == candidates.end()) {
            // A legacy index may have omitted a distinct row at this prefix.
            return undo || !index.hasCompletePrimaryPrefixes();
        }
        return index.removeMulti(key, rid);
    }
    int64_t indexedRid = -1;
    const auto found = index.searchChecked(key, indexedRid);
    if (found == BPTree::SearchResult::Error) return false;
    if (found == BPTree::SearchResult::NotFound || indexedRid != rid) return undo;
    return index.remove(key);
}

inline bool restore(const TableSchema& table, BPTree& index,
                    const std::string& key, int64_t rid) {
    if (sharesPrefix(table, key)) {
        std::vector<int64_t> candidates;
        return index.searchMultiChecked(key, candidates) &&
            (std::find(candidates.begin(), candidates.end(), rid) != candidates.end() ||
             index.insertMulti(key, rid));
    }
    int64_t indexedRid = -1;
    const auto found = index.searchChecked(key, indexedRid);
    if (found == BPTree::SearchResult::Error) return false;
    if (found == BPTree::SearchResult::Found) return indexedRid == rid;
    return index.insert(key, rid);
}

} // namespace dbms::primary_key_index
