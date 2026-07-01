#pragma once

#include "interfaces/index_am.h"
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace dbms {

// Simplified GIN (Generalized Inverted Index) for arrays, full-text, JSONB.
// Stores term -> set of RowId mappings. Supports @@ (contains) and ?| (any key).
class GinIndex {
public:
    explicit GinIndex(const std::filesystem::path& indexFile);
    ~GinIndex();

    bool open();
    void close();

    // Extract terms from a value and index them for the given rowId.
    // For arrays: each element is a term.
    // For text: split on whitespace + punctuation.
    void insert(const std::string& value, RowId rowId);
    void remove(const std::string& value, RowId rowId);

    // Find rows containing ALL terms in query (AND semantics).
    std::vector<RowId> searchContains(const std::string& query) const;

    // Find rows matching an array literal @> '{"a","b"}'
    std::vector<RowId> searchArrayContains(const std::vector<std::string>& elements) const;

    // Find rows containing ANY of the given keys (?| operator)
    std::vector<RowId> searchAnyKey(const std::vector<std::string>& keys) const;

    // JSONB containment: @> operator
    std::vector<RowId> searchJsonContains(const std::string& jsonQuery) const;

    void rebuild(const std::vector<std::pair<std::string, RowId>>& allEntries);

    const std::filesystem::path& path() const { return indexFile_; }

private:
    std::vector<std::string> extractTerms(const std::string& value) const;
    void persist() const;
    void load();

    std::filesystem::path indexFile_;
    // term -> sorted set of RowIds
    std::map<std::string, std::set<RowId>> postings_;
    bool dirty_ = false;
};

} // namespace dbms
