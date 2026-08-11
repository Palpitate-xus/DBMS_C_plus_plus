#include "HashIndexAM.h"

namespace dbms {

HashIndexAM::HashIndexAM(const std::string& name,
                         const std::filesystem::path& indexFile)
    : name_(name), filePath_(indexFile), index_(std::make_unique<HashIndex>(indexFile))
{
    index_->open();
}

HashIndexAM::~HashIndexAM() {
    close();
}

bool HashIndexAM::insert(const std::string& key, RowId rowId) {
    if (!index_ || !index_->isOpen()) return false;
    return index_->insert(key, rowId);
}

bool HashIndexAM::remove(const std::string& key, RowId rowId) {
    if (!index_ || !index_->isOpen()) return false;
    auto rids = index_->search(key);
    bool found = false;
    for (auto rid : rids) {
        if (rid == rowId) {
            found = true;
            break;
        }
    }
    if (!found) return false;
    return index_->remove(key, rowId);
}

std::vector<RowId> HashIndexAM::search(const std::string& key) const {
    if (!index_ || !index_->isOpen()) return {};
    return index_->search(key);
}

bool HashIndexAM::rebuild() {
    if (!index_) return false;
    index_->close();
    index_ = std::make_unique<HashIndex>(filePath_);
    return index_->open();
}

void HashIndexAM::close() {
    if (index_) {
        index_->close();
        index_.reset();
    }
}

std::string HashIndexAM::name() const {
    return name_;
}

} // namespace dbms
