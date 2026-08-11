// ============================================================================
// DDL Transaction Skeleton — Phase 4 Wave 0.4
// ============================================================================

#include "commands/DdlTransaction.h"
#include <algorithm>
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace dbms {

namespace {

uint8_t walInfoForOp(DdlTransaction::RecordedOp::Op op) {
    switch (op) {
        case DdlTransaction::RecordedOp::Op::Create: return XLOG_CATALOG_CREATE;
        case DdlTransaction::RecordedOp::Op::Drop:   return XLOG_CATALOG_DROP;
        case DdlTransaction::RecordedOp::Op::Update: return XLOG_CATALOG_UPDATE;
    }
    return XLOG_CATALOG_UPDATE;
}

} // anonymous namespace

DdlTransaction::DdlTransaction(Session& session)
    : session_(session), engine_(g_engine) {}

DdlTransaction::~DdlTransaction() {
    if (active_ && !committed_) rollback();
}

void DdlTransaction::enableSnapshotRollback() {
    snapshotRollbackEnabled_ = true;
    if (active_ && startedByUs_) {
        engine_.preserveTransactionBackupOnRollback(true);
        engine_.createTransactionBackup();
    }
}

bool DdlTransaction::begin() {
    if (engine_.inTransaction()) {
        active_ = true;
        startedByUs_ = false;
        return true;
    }
    if (session_.currentDB.empty()) {
        // CREATE/DROP DATABASE 等跨库命令在没有 currentDB 时也能执行，
        // 这里先激活事务状态，实际操作自行使用合适的 dbname。
        active_ = true;
        startedByUs_ = false;
        return true;
    }
    DBStatus st = engine_.beginTransaction(session_.currentDB, snapshotRollbackEnabled_);
    active_ = (st == DBStatus::OK);
    startedByUs_ = active_;
    if (startedByUs_) {
        engine_.preserveTransactionBackupOnRollback(snapshotRollbackEnabled_);
        if (snapshotRollbackEnabled_ && !engine_.createTransactionBackup()) {
            engine_.rollbackTransaction();
            active_ = false;
            startedByUs_ = false;
        }
    }
    return active_;
}

void DdlTransaction::recordCreate(DdlObjectKind kind, const std::string& name,
                                  const std::string& extra) {
    RecordedOp op{RecordedOp::Op::Create, kind, name, extra};
    ops_.push_back(op);
    writeWal(op);
}

void DdlTransaction::recordDrop(DdlObjectKind kind, const std::string& name,
                                const std::string& extra) {
    RecordedOp op{RecordedOp::Op::Drop, kind, name, extra};
    ops_.push_back(op);
    writeWal(op);
}

void DdlTransaction::recordUpdate(DdlObjectKind kind, const std::string& name,
                                  const std::string& extra) {
    RecordedOp op{RecordedOp::Op::Update, kind, name, extra};
    ops_.push_back(op);
    writeWal(op);
}

bool DdlTransaction::commit() {
    if (!active_) return committed_;

    if (startedByUs_ && engine_.inTransaction()) {
        const DBStatus status = engine_.commitTransaction();
        if (status != DBStatus::OK) {
            std::cerr << "DDL transaction commit failed (SQLSTATE "
                      << sqlstateForDBStatus(status) << ")" << std::endl;
            // StorageEngine rolls back row-level changes for deferred-check
            // and SSI failures.  The wrapper still owns physical DDL changes
            // and must restore its statement snapshot before reporting the
            // failure to the executor.
            rollback();
            return false;
        }
    }

    if (startedByUs_) {
        engine_.discardTransactionBackup(session_.currentDB);
    }
    ops_.clear();
    committed_ = true;
    active_ = false;
    return true;
}

void DdlTransaction::rollback() {
    if (!active_) return;
    // 按后进先出顺序撤销 CREATE 操作。
    for (auto it = ops_.rbegin(); it != ops_.rend(); ++it) {
        if (it->op == RecordedOp::Op::Create) {
            if (!undoCreate(*it)) {
                std::cerr << "DDL rollback warning: failed to undo creation of "
                          << kindString(it->kind) << " " << it->name << std::endl;
            }
        }
    }
    if (startedByUs_ && engine_.inTransaction()) {
        engine_.rollbackTransaction();
    }
    if (startedByUs_) {
        if (snapshotRollbackEnabled_ && snapshotDirty_ && !session_.currentDB.empty()) {
            if (!engine_.restoreTransactionBackup(session_.currentDB)) {
                std::cerr << "DDL rollback warning: failed to restore database snapshot for "
                          << session_.currentDB << std::endl;
            }
        } else {
            engine_.discardTransactionBackup(session_.currentDB);
        }
    }
    ops_.clear();
    committed_ = false;
    active_ = false;
}

std::string DdlTransaction::kindString(DdlObjectKind kind) const {
    switch (kind) {
        case DdlObjectKind::Database:   return "database";
        case DdlObjectKind::Schema:     return "schema";
        case DdlObjectKind::Table:      return "table";
        case DdlObjectKind::Index:      return "index";
        case DdlObjectKind::Sequence:   return "sequence";
        case DdlObjectKind::Domain:     return "domain";
        case DdlObjectKind::Type:       return "type";
        case DdlObjectKind::View:       return "view";
        case DdlObjectKind::MaterializedView: return "materialized_view";
        case DdlObjectKind::Function:   return "function";
        case DdlObjectKind::Procedure:  return "procedure";
        case DdlObjectKind::Trigger:    return "trigger";
        case DdlObjectKind::Policy:     return "policy";
        case DdlObjectKind::Collation:  return "collation";
    }
    return "object";
}

bool DdlTransaction::undoCreate(const RecordedOp& op) {
    const std::string& db = session_.currentDB.empty() ? op.extra : session_.currentDB;
    if (db.empty() && op.kind != DdlObjectKind::Database) return false;

    switch (op.kind) {
        case DdlObjectKind::Database:
            engine_.dropDatabase(op.name);
            break;
        case DdlObjectKind::Schema:
            engine_.dropSchema(db, op.name, true);
            break;
        case DdlObjectKind::Table:
            engine_.dropTable(db, op.name);
            break;
        case DdlObjectKind::Index:
            if (op.extra.empty()) return false;
            return engine_.dropIndex(db, op.extra, op.name) == DBStatus::OK;
        case DdlObjectKind::Sequence:
            engine_.dropSequence(db, op.name);
            break;
        case DdlObjectKind::Domain:
            engine_.dropDomain(db, op.name);
            break;
        case DdlObjectKind::Type:
            engine_.dropCompositeType(db, op.name);
            break;
        case DdlObjectKind::View:
            return engine_.dropView(db, op.name) == DBStatus::OK;
        case DdlObjectKind::MaterializedView:
            return engine_.dropMaterializedView(db, op.name) == DBStatus::OK;
        case DdlObjectKind::Function: {
            const bool hadUdf = engine_.udfExists(db, op.name);
            const bool hadTvf = engine_.tvfExists(db, op.name);
            if (!hadUdf && !hadTvf) return false;
            bool removed = false;
            if (hadUdf) removed = engine_.dropUDF(db, op.name) == DBStatus::OK || removed;
            if (hadTvf) removed = engine_.dropTVF(db, op.name) == DBStatus::OK || removed;
            return removed;
        }
        case DdlObjectKind::Procedure:
            return engine_.dropProcedure(db, op.name) == DBStatus::OK;
        case DdlObjectKind::Trigger:
            return engine_.dropTrigger(db, op.name) == DBStatus::OK;
        case DdlObjectKind::Policy:
            if (op.extra.empty()) return false;
            return engine_.dropPolicy(db, op.extra, op.name) == DBStatus::OK;
        case DdlObjectKind::Collation:
            return engine_.dropCollation(db, op.name) == DBStatus::OK;
        default:
            return false;
    }
    return true;
}

void DdlTransaction::writeWal(const RecordedOp& op) {
    const std::string& db = session_.currentDB.empty() ? op.extra : session_.currentDB;
    if (db.empty()) return;
    engine_.walCatalogChange(db, walInfoForOp(op.op), kindString(op.kind), op.name);
}

} // namespace dbms
