#include "BPTreeIndexAM.h"

namespace dbms {

BPTreeIndexAM::BPTreeIndexAM(const std::string& name,
                             const std::filesystem::path& indexFile)
    : name_(name), filePath_(indexFile), tree_(std::make_unique<BPTree>(indexFile))
{
    tree_->open();
}

BPTreeIndexAM::~BPTreeIndexAM() {
    close();
}

bool BPTreeIndexAM::insert(const std::string& key, RowId rowId) {
    if (!tree_ || !tree_->isOpen()) return false;
    // BPTree 的 insert 在 key 已存在时返回 false；secondary index 用 insertMulti
    return tree_->insertMulti(key, rowId);
}

bool BPTreeIndexAM::remove(const std::string& key, RowId rowId) {
    if (!tree_ || !tree_->isOpen()) return false;
    return tree_->removeMulti(key, rowId);
}

std::vector<RowId> BPTreeIndexAM::search(const std::string& key) const {
    if (!tree_ || !tree_->isOpen()) return {};
    return tree_->searchMulti(key);
}

std::vector<RowId> BPTreeIndexAM::searchRange(const std::string& lowKey,
                                               const std::string& highKey) const {
    if (!tree_ || !tree_->isOpen()) return {};
    return tree_->rangeScan(lowKey, highKey);
}

bool BPTreeIndexAM::rebuild() {
    if (!tree_) return false;
    tree_->close();
    tree_ = std::make_unique<BPTree>(filePath_);
    return tree_->open();
}

void BPTreeIndexAM::close() {
    if (tree_) {
        tree_->close();
        tree_.reset();
    }
}

std::string BPTreeIndexAM::name() const {
    return name_;
}

} // namespace dbms
