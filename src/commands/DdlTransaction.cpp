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

bool DdlTransaction::enableSnapshotRollback() {
    snapshotRollbackEnabled_ = true;
    if (!active_) return true;
    if (!engine_.inTransaction()) return false;

    if (active_) {
        const bool hadBackup = engine_.hasTransactionBackup();
        engine_.preserveTransactionBackupOnRollback(true);
        engine_.restoreTransactionBackupBeforeRowUndo(true);
        const bool created = engine_.createTransactionBackup();
        if (created && !hadBackup) snapshotCreatedByThis_ = true;
        return created;
    }
    return true;
}

void DdlTransaction::markSnapshotDirty() {
    snapshotDirty_ = true;
    engine_.markTransactionBackupDirty();
}

bool DdlTransaction::begin() {
    if (engine_.inTransaction()) {
        if (snapshotRollbackEnabled_ && engine_.transactionBackupDirty()) {
            // A full snapshot already contains an earlier physical DDL
            // mutation. A second snapshot-scoped statement would need a
            // nested image to preserve statement atomicity; reject it until
            // object-level/nested DDL savepoints are implemented.
            return false;
        }
        active_ = true;
        startedByUs_ = false;
        if (snapshotRollbackEnabled_ && !enableSnapshotRollback()) {
            active_ = false;
            return false;
        }
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
        if (snapshotRollbackEnabled_) {
            snapshotCreatedByThis_ = true;
            engine_.restoreTransactionBackupBeforeRowUndo(true);
        }
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
    } else if (engine_.inTransaction()) {
        // A DDL wrapper is statement-scoped, while the engine transaction may
        // span many statements. Preserve CREATE undo actions in that outer
        // transaction so COMMIT/ROLLBACK and SAVEPOINT see the complete DDL
        // history instead of only the last statement.
        const std::string db = session_.currentDB;
        for (const auto& op : ops_) {
            if (op.op != RecordedOp::Op::Create) continue;
            const std::string undoDb = db.empty() ? op.extra : db;
            engine_.registerDdlUndo([engine = &engine_, undoDb, op]() {
                return DdlTransaction::undoCreate(*engine, undoDb, op);
            });
        }
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
        const DBStatus status = engine_.rollbackTransaction();
        if (status != DBStatus::OK) {
            std::cerr << "DDL rollback warning (SQLSTATE "
                      << sqlstateForDBStatus(status) << ")" << std::endl;
        }
    } else if (!startedByUs_ && engine_.inTransaction() &&
               snapshotRollbackEnabled_ && snapshotDirty_ &&
               !session_.currentDB.empty()) {
        if (!engine_.restoreTransactionBackup(session_.currentDB)) {
            std::cerr << "DDL rollback warning: failed to restore database snapshot for "
                      << session_.currentDB << std::endl;
        }
    } else if (!startedByUs_ && engine_.inTransaction() &&
               snapshotRollbackEnabled_ && !snapshotDirty_) {
        if (snapshotCreatedByThis_) {
            engine_.discardTransactionBackup(session_.currentDB);
        }
    } else if (startedByUs_) {
        engine_.discardTransactionBackup(session_.currentDB);
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
    const std::string db = session_.currentDB.empty() ? op.extra : session_.currentDB;
    return undoCreate(engine_, db, op);
}

bool DdlTransaction::undoCreate(StorageEngine& engine, const std::string& db,
                                const RecordedOp& op) {
    if (db.empty() && op.kind != DdlObjectKind::Database) return false;

    switch (op.kind) {
        case DdlObjectKind::Database:
            engine.dropDatabase(op.name);
            break;
        case DdlObjectKind::Schema:
            engine.dropSchema(db, op.name, true);
            break;
        case DdlObjectKind::Table:
            engine.dropTable(db, op.name);
            break;
        case DdlObjectKind::Index:
            if (op.extra.empty()) return false;
            return engine.dropIndex(db, op.extra, op.name) == DBStatus::OK;
        case DdlObjectKind::Sequence:
            engine.dropSequence(db, op.name);
            break;
        case DdlObjectKind::Domain:
            engine.dropDomain(db, op.name);
            break;
        case DdlObjectKind::Type:
            engine.dropCompositeType(db, op.name);
            break;
        case DdlObjectKind::View:
            return engine.dropView(db, op.name) == DBStatus::OK;
        case DdlObjectKind::MaterializedView:
            return engine.dropMaterializedView(db, op.name) == DBStatus::OK;
        case DdlObjectKind::Function: {
            const bool hadUdf = engine.udfExists(db, op.name);
            const bool hadTvf = engine.tvfExists(db, op.name);
            if (!hadUdf && !hadTvf) return false;
            bool removed = false;
            if (hadUdf) removed = engine.dropUDF(db, op.name) == DBStatus::OK || removed;
            if (hadTvf) removed = engine.dropTVF(db, op.name) == DBStatus::OK || removed;
            return removed;
        }
        case DdlObjectKind::Procedure:
            return engine.dropProcedure(db, op.name) == DBStatus::OK;
        case DdlObjectKind::Trigger:
            return engine.dropTrigger(db, op.name) == DBStatus::OK;
        case DdlObjectKind::Policy:
            if (op.extra.empty()) return false;
            return engine.dropPolicy(db, op.extra, op.name) == DBStatus::OK;
        case DdlObjectKind::Collation:
            return engine.dropCollation(db, op.name) == DBStatus::OK;
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
