#pragma once

#include "index_am.h"
#include "HashIndex.h"
#include <memory>

namespace dbms {

// ============================================================================
// Hash 索引的 IIndexAM 适配器
// Phase 0：将现有 HashIndex 包装为可插拔的索引访问方法
// ============================================================================

class HashIndexAM : public IIndexAM {
public:
    HashIndexAM(const std::string& name,
                const std::filesystem::path& indexFile);
    ~HashIndexAM() override;

    // IIndexAM 接口实现
    bool insert(const std::string& key, RowId rowId) override;
    bool remove(const std::string& key, RowId rowId) override;
    std::vector<RowId> search(const std::string& key) const override;
    bool rebuild() override;
    void close() override;
    std::string name() const override;
    const char* amName() const override { return "hash"; }

    // 底层 HashIndex 访问（供迁移期内部使用）
    HashIndex* underlying() const { return index_.get(); }

private:
    std::string name_;
    std::filesystem::path filePath_;
    std::unique_ptr<HashIndex> index_;
};

} // namespace dbms
