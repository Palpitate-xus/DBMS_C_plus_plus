// ============================================================================
// DDL Transaction Skeleton — Phase 4 Wave 0.4
//
// 为 DDL 操作提供事务化包装：
//   - 进入/复用 StorageEngine 事务上下文
//   - 记录本次 DDL 创建/删除/修改的对象
//   - 失败时撤销 CREATE 记录，并恢复本类启动事务的当前格式数据库快照
//   - 为每个 DDL 变更写 CATALOG WAL 记录，为 Wave 5 完整 DDL 事务化做准备
//
// 当前仍是渐进式实现：完整跨对象依赖 undo、并发 DDL 锁和全部
// PostgreSQL 隐式提交边界仍待补齐；当前格式快照用于保护已接入的
// statement-level DDL 失败路径，CREATE undo 会挂接到外层事务及 SAVEPOINT。
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
    Policy,
    Collation
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

    // 提交：返回引擎最终状态。提交失败时保留失败语义，并恢复本类
    // 开启事务的物理快照，避免调用方把失败误报为成功。
    bool commit();

    // 回滚：撤销已记录的 CREATE 操作，回滚 StorageEngine 事务，并在本类
    // 开启事务时恢复当前格式数据库快照。
    void rollback();

    // Opt in to physical snapshot restoration for DDL paths whose storage
    // primitives rewrite files outside the row-level undo log.
    bool enableSnapshotRollback();
    void markSnapshotDirty();

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
    bool snapshotRollbackEnabled_ = false;
    bool snapshotDirty_ = false;
    bool snapshotCreatedByThis_ = false;
    std::vector<RecordedOp> ops_;

    std::string kindString(DdlObjectKind kind) const;
    bool undoCreate(const RecordedOp& op);
    static bool undoCreate(StorageEngine& engine, const std::string& db,
                           const RecordedOp& op);
    void writeWal(const RecordedOp& op);
};

} // namespace dbms
