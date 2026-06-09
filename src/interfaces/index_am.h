#pragma once

#include "dbms_defs.h"
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// 索引访问方法接口 (Index Access Method)
// Phase 6 逐步实现：BTree/Hash/GIN/GiST/BRIN/SP-GiST 均实现此接口
// ============================================================================

class IIndexAM {
public:
    virtual ~IIndexAM() = default;

    // 插入索引项
    virtual bool insert(const std::string& key, RowId rowId) = 0;

    // 删除索引项
    virtual bool remove(const std::string& key, RowId rowId) = 0;

    // 精确查找：返回所有匹配的 RowId
    virtual std::vector<RowId> search(const std::string& key) const = 0;

    // 范围查找：[low, high)，用于 BTree / GiST
    virtual std::vector<RowId> searchRange(const std::string& lowKey,
                                            const std::string& highKey) const {
        (void)lowKey; (void)highKey;
        return {}; // 默认不支持，由子类覆盖
    }

    // 前缀查找：用于 SP-GiST / BTree prefix
    virtual std::vector<RowId> searchPrefix(const std::string& prefix) const {
        (void)prefix;
        return {};
    }

    // 重建索引
    virtual bool rebuild() = 0;

    // 关闭并释放资源
    virtual void close() = 0;

    // 索引名称
    virtual std::string name() const = 0;

    // 索引类型标识
    virtual const char* amName() const = 0;
};

// ============================================================================
// 索引扫描描述符
// ============================================================================

struct IndexScanKey {
    std::string column;
    std::string op;       // =, <, >, <=, >=, <>, LIKE, etc.
    std::string value;
};

class IIndexScan {
public:
    virtual ~IIndexScan() = default;

    virtual void beginScan(const std::vector<IndexScanKey>& keys) = 0;
    virtual bool getNext(ItemPointer* tupleId) = 0;
    virtual void endScan() = 0;
};

} // namespace dbms
