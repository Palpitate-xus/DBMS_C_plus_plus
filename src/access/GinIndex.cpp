#include "GinIndex.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>

namespace dbms {

GinIndex::GinIndex(const std::filesystem::path& indexFile) : indexFile_(indexFile) {}

GinIndex::~GinIndex() { close(); }

bool GinIndex::open() {
    if (std::filesystem::exists(indexFile_)) {
        load();
    }
    return true;
}

void GinIndex::close() {
    if (dirty_) persist();
}

std::vector<std::string> GinIndex::extractTerms(const std::string& value) const {
    std::vector<std::string> terms;
    std::string cur;
    for (char c : value) {
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
            cur += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        } else if (!cur.empty()) {
            // Deduplicate within same value
            if (terms.empty() || terms.back() != cur)
                terms.push_back(cur);
            cur.clear();
        }
    }
    if (!cur.empty() && (terms.empty() || terms.back() != cur))
        terms.push_back(cur);
    return terms;
}

void GinIndex::insert(const std::string& value, RowId rowId) {
    auto terms = extractTerms(value);
    for (const auto& t : terms) {
        postings_[t].insert(static_cast<RowId>(rowId));
    }
    if (!terms.empty()) dirty_ = true;
}

void GinIndex::remove(const std::string& value, RowId rowId) {
    auto terms = extractTerms(value);
    for (const auto& t : terms) {
        auto it = postings_.find(t);
        if (it != postings_.end()) {
            it->second.erase(static_cast<RowId>(rowId));
            if (it->second.empty()) postings_.erase(it);
        }
    }
    if (!terms.empty()) dirty_ = true;
}

std::vector<RowId> GinIndex::searchContains(const std::string& query) const {
    auto terms = extractTerms(query);
    if (terms.empty()) return {};
    // AND semantics: intersect posting lists
    std::set<RowId> result;
    auto it0 = postings_.find(terms[0]);
    if (it0 == postings_.end()) return {};
    result = it0->second;
    for (size_t i = 1; i < terms.size(); ++i) {
        auto it = postings_.find(terms[i]);
        if (it == postings_.end()) return {};
        std::set<RowId> merged;
        std::set_intersection(result.begin(), result.end(),
                              it->second.begin(), it->second.end(),
                              std::inserter(merged, merged.begin()));
        result = std::move(merged);
        if (result.empty()) break;
    }
    return std::vector<RowId>(result.begin(), result.end());
}

std::vector<RowId> GinIndex::searchArrayContains(const std::vector<std::string>& elements) const {
    if (elements.empty()) return {};
    std::set<RowId> result;
    auto it0 = postings_.find(elements[0]);
    if (it0 == postings_.end()) return {};
    result = it0->second;
    for (size_t i = 1; i < elements.size(); ++i) {
        auto it = postings_.find(elements[i]);
        if (it == postings_.end()) return {};
        std::set<RowId> merged;
        std::set_intersection(result.begin(), result.end(),
                              it->second.begin(), it->second.end(),
                              std::inserter(merged, merged.begin()));
        result = std::move(merged);
        if (result.empty()) break;
    }
    return std::vector<RowId>(result.begin(), result.end());
}

std::vector<RowId> GinIndex::searchAnyKey(const std::vector<std::string>& keys) const {
    std::set<RowId> result;
    for (const auto& k : keys) {
        auto it = postings_.find(k);
        if (it != postings_.end())
            result.insert(it->second.begin(), it->second.end());
    }
    return std::vector<RowId>(result.begin(), result.end());
}

std::vector<RowId> GinIndex::searchJsonContains(const std::string& jsonQuery) const {
    // Simplified: extract all terms from the JSON query and do AND search.
    return searchContains(jsonQuery);
}

void GinIndex::rebuild(const std::vector<std::pair<std::string, RowId>>& allEntries) {
    postings_.clear();
    for (const auto& [val, rid] : allEntries) {
        auto terms = extractTerms(val);
        for (const auto& t : terms) {
            postings_[t].insert(static_cast<RowId>(rid));
        }
    }
    dirty_ = true;
}

void GinIndex::persist() const {
    std::ofstream ofs(indexFile_);
    if (!ofs) return;
    for (const auto& [term, rids] : postings_) {
        ofs << term;
        for (auto rid : rids) ofs << " " << rid;
        ofs << "\n";
    }
}

void GinIndex::load() {
    std::ifstream ifs(indexFile_);
    if (!ifs) return;
    std::string line;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::string term;
        iss >> term;
        if (term.empty()) continue;
        RowId rid;
        while (iss >> rid) {
            postings_[term].insert(rid);
        }
    }
}

} // namespace dbms
