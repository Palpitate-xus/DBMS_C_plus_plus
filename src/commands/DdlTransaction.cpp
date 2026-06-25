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
    DBStatus st = engine_.beginTransaction(session_.currentDB);
    active_ = (st == DBStatus::OK);
    startedByUs_ = active_;
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

void DdlTransaction::commit() {
    if (!active_) return;
    if (startedByUs_ && engine_.inTransaction()) {
        engine_.commitTransaction();
    }
    ops_.clear();
    committed_ = true;
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
            engine_.dropIndex(db, op.name);
            break;
        case DdlObjectKind::Sequence:
            engine_.dropSequence(db, op.name);
            break;
        case DdlObjectKind::Domain:
            engine_.dropDomain(db, op.name);
            break;
        case DdlObjectKind::Type:
            engine_.dropCompositeType(db, op.name);
            break;
        default:
            // View / Function / Procedure 暂不支持自动撤销
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
