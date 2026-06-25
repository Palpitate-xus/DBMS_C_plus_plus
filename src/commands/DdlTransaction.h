// ============================================================================
// DDL Transaction Skeleton — Phase 4 Wave 0.4
//
// 为 DDL 操作提供事务化包装：
//   - 进入/复用 StorageEngine 事务上下文
//   - 记录本次 DDL 创建/删除/修改的对象
//   - 失败时按记录回滚（目前支持撤销 CREATE 类操作）
//   - 为每个 DDL 变更写 CATALOG WAL 记录，为 Wave 5 完整 DDL 事务化做准备
//
// 当前仍是骨架实现：DROP 的回滚只做记录，无法真正恢复被删除对象；
// 完整 redo/undo 与 CatalogManager 集成留给 Phase 4 Wave 5。
// ============================================================================

#pragma once

#include "commands/TableManage.h"
#include "Session.h"
#include "storage/WAL.h"
#include <string>
#include <vector>

namespace dbms {

enum class DdlObjectKind {
    Database,
    Schema,
    Table,
    Index,
    Sequence,
    Domain,
    Type,
    View,
    MaterializedView,
    Function,
    Procedure,
    Trigger,
    Policy
};

class DdlTransaction {
public:
    explicit DdlTransaction(Session& session);
    ~DdlTransaction();

    // 尝试进入事务：复用已有事务，或新起一个事务。返回是否成功。
    bool begin();

    // 记录 DDL 变更（按操作类型）。同时写一条 CATALOG WAL 记录。
    void recordCreate(DdlObjectKind kind, const std::string& name,
                      const std::string& extra = {});
    void recordDrop(DdlObjectKind kind, const std::string& name,
                    const std::string& extra = {});
    void recordUpdate(DdlObjectKind kind, const std::string& name,
                      const std::string& extra = {});

    // 提交：标记成功并清空操作日志。若本类开启了事务，则同时提交 StorageEngine。
    void commit();

    // 回滚：撤销已记录的 CREATE 操作，并回滚 StorageEngine 事务（如果是本类开启的）。
    void rollback();

    bool isActive() const { return active_; }
    bool isCommitted() const { return committed_; }
    size_t changeCount() const { return ops_.size(); }

    struct RecordedOp {
        enum class Op { Create, Drop, Update } op;
        DdlObjectKind kind;
        std::string name;
        std::string extra;
    };

private:
    Session& session_;
    StorageEngine& engine_;
    bool active_ = false;
    bool startedByUs_ = false;
    bool committed_ = false;
    std::vector<RecordedOp> ops_;

    std::string kindString(DdlObjectKind kind) const;
    bool undoCreate(const RecordedOp& op);
    void writeWal(const RecordedOp& op);
};

} // namespace dbms
