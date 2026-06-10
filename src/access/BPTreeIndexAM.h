#pragma once

#include "index_am.h"
#include "BPTree.h"
#include <memory>

namespace dbms {

// ============================================================================
// B+ Tree 索引的 IIndexAM 适配器
// Phase 0：将现有 BPTree 包装为可插拔的索引访问方法
// ============================================================================

class BPTreeIndexAM : public IIndexAM {
public:
    BPTreeIndexAM(const std::string& name,
                  const std::filesystem::path& indexFile);
    ~BPTreeIndexAM() override;

    // IIndexAM 接口实现
    bool insert(const std::string& key, RowId rowId) override;
    bool remove(const std::string& key, RowId rowId) override;
    std::vector<RowId> search(const std::string& key) const override;
    std::vector<RowId> searchRange(const std::string& lowKey,
                                      const std::string& highKey) const override;
    bool rebuild() override;
    void close() override;
    std::string name() const override;
    const char* amName() const override { return "btree"; }

    // 底层 BPTree 访问（供迁移期内部使用）
    BPTree* underlying() const { return tree_.get(); }

private:
    std::string name_;
    std::filesystem::path filePath_;
    std::unique_ptr<BPTree> tree_;
};

} // namespace dbms
