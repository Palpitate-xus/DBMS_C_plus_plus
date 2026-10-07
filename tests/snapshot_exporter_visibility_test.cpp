#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/CommitLog.h"
#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

namespace {
using namespace dbms;
using Rows = std::vector<std::vector<std::string>>;
using Nulls = std::vector<std::vector<bool>>;

void expectRows(StorageEngine& owner, const std::string& db,
                const Rows& expected, const Nulls& nulls) {
    auto prepared = owner.prepareBoundQuery(db, "SELECT id,val FROM t ORDER BY id");
    auto plan = QueryPlanner::buildPreparedSelectPlan(&owner, db, "t", std::move(prepared));
    auto result = QueryPlanner::executePlanChecked(std::move(plan)); result.throwIfFailed();
    if (result.structuredRows != expected || result.structuredNulls != nulls) {
        for (size_t row = 0; row < result.structuredRows.size(); ++row) {
            std::cerr << "SNAPSHOT_EXPORTER_ACTUAL";
            for (size_t column = 0; column < result.structuredRows[row].size(); ++column)
                std::cerr << ' ' << result.structuredRows[row][column] << "/null=" << result.structuredNulls[row][column];
            std::cerr << '\n';
        }
    }
    assert(result.structuredRowsAvailable && result.structuredRows == expected && result.structuredNulls == nulls);
}

void readCommand(StorageEngine& owner, const std::string& db, const Rows& rows, const Nulls& nulls) {
    assert(owner.beginSqlCommand()); expectRows(owner, db, rows, nulls); assert(owner.finishSqlCommand());
}

Snapshot decode(const std::string& bytes, uint64_t exporter, uint64_t foreign, uint32_t cid) {
    const auto decoded = Snapshot::importFromBytes(bytes); assert(decoded);
    const auto& snap = *decoded;
    std::cout << "SNAPSHOT_EXPORTER_TRANSFER xid=" << exporter << " xmin=" << snap.xmin
              << " xmax=" << snap.xmax << " cid=" << snap.curCid << " active=";
    for (auto xid : snap.activeXids) std::cout << xid << ',';
    std::cout << std::endl;
    assert(snap.version == 2 && snap.curCid == cid && snap.xmin <= exporter);
    assert(exporter < snap.xmax && std::binary_search(snap.activeXids.begin(), snap.activeXids.end(), exporter));
    assert(std::binary_search(snap.activeXids.begin(), snap.activeXids.end(), foreign));
    assert(std::is_sorted(snap.activeXids.begin(), snap.activeXids.end()));
    assert(std::adjacent_find(snap.activeXids.begin(), snap.activeXids.end()) == snap.activeXids.end());
    assert(std::is_sorted(snap.subxip.begin(), snap.subxip.end()));
    for (auto xid : snap.subxip) assert(!std::binary_search(snap.activeXids.begin(), snap.activeXids.end(), xid));
    assert(snap.exportToBytes() == bytes);
    return snap;
}

void scenario(bool commit) {
    // Construct every owner before any transaction: recovery must not inspect
    // the scenario while a live exporter/importer owns its active XID.
    StorageEngine writer, foreign, first, second, reimported, fresh;
    const auto db = testDbPath(commit ? "snapshot_exporter_commit" : "snapshot_exporter_rollback");
    assert(writer.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table; table.tablename = "t";
    table.append(makeIntColumn("id", false, 2, true));
    table.append(makeVarCharColumn("val", true, 30, false));
    assert(writer.createTable(db, table) == DBStatus::OK);
    assert(writer.insert(db, "t", {{"id", "1"}, {"val", "old"}}) == DBStatus::OK);
    assert(writer.insert(db, "t", {{"id", "2"}, {"val", "deleted"}}) == DBStatus::OK);
    assert(writer.insertRow(db, "t", StorageEngine::SqlRow{{"id", "3"}, {"val", std::nullopt}}) == DBStatus::OK);
    const Rows old = {{"1","old"}, {"2","deleted"}, {"3",""}};
    const Nulls oldNulls = {{false,false}, {false,false}, {false,true}};
    assert(foreign.beginTransaction(db) == DBStatus::OK);
    assert(foreign.insert(db, "t", {{"id", "20"}, {"val", "foreign"}}) == DBStatus::OK);
    const auto foreignXid = foreign.currentTxnId();
    assert(writer.beginTransaction(db) == DBStatus::OK);
    assert(writer.beginSqlCommand());
    assert(writer.update(db, "t", {{"val", "new"}}, {"=id 1"}) == DBStatus::OK);
    assert(writer.remove(db, "t", {"=id 2"}) == DBStatus::OK);
    assert(writer.insertRow(db, "t", StorageEngine::SqlRow{{"id", "4"}, {"val", std::nullopt}}) == DBStatus::OK);
    assert(writer.finishSqlCommand());
    assert(writer.savepoint("released") == DBStatus::OK);
    assert(writer.beginSqlCommand());
    assert(writer.insert(db, "t", {{"id", "5"}, {"val", "released"}}) == DBStatus::OK);
    assert(writer.finishSqlCommand());
    assert(writer.releaseSavepoint("released") == DBStatus::OK);
    assert(writer.savepoint("undone") == DBStatus::OK);
    assert(writer.beginSqlCommand());
    assert(writer.insert(db, "t", {{"id", "6"}, {"val", "undone"}}) == DBStatus::OK);
    assert(writer.finishSqlCommand());
    assert(writer.rollbackToSavepoint("undone") == DBStatus::OK);
    assert(writer.releaseSavepoint("undone") == DBStatus::OK);
    const Rows own = {{"1","new"}, {"3",""}, {"4",""}, {"5","released"}};
    const Nulls ownNulls = {{false,false}, {false,true}, {false,true}, {false,false}};
    readCommand(writer, db, own, ownNulls);
    const auto exporterXid = writer.currentTxnId();
    const auto live = *writer.getCurrentReadView();
    const auto beforeCid = writer.currentCommandId();
    const auto bytes = writer.exportSnapshot();
    const auto transfer = decode(bytes, exporterXid, foreignXid, beforeCid);
    assert(writer.currentCommandId() == beforeCid);
    assert(transfer.database == db);
    // Export augments only the transfer copy. The live own-write/CID view is
    // not made foreign/in-progress and retains all original state.
    const auto* afterExport = writer.getCurrentReadView();
    assert(afterExport->creatorTxnId == exporterXid && !afterExport->activeTxnIds.count(exporterXid));
    assert(afterExport->activeTxnIds == live.activeTxnIds && afterExport->subTxnIds == live.subTxnIds &&
           afterExport->upLimitId == live.upLimitId && afterExport->lowLimitId == live.lowLimitId &&
           afterExport->currentCommandId == live.currentCommandId &&
           afterExport->commandIdVisibility == live.commandIdVisibility && afterExport->comboCommandIds == live.comboCommandIds);
    readCommand(writer, db, own, ownNulls);
    for (auto* reader : {&first, &second}) {
        assert(reader->beginTransaction(db) == DBStatus::OK);
        assert(reader->currentCommandId() == 0 && reader->importSnapshot(bytes));
        assert(!reader->importSnapshot(bytes));
        readCommand(*reader, db, old, oldNulls);
    }
    assert(foreign.commitTransaction() == DBStatus::OK);
    assert(foreign.getPageAllocator(db, "t")->flush());
    for (auto* reader : {&first, &second}) readCommand(*reader, db, old, oldNulls);
    readCommand(writer, db, own, ownNulls);
    assert((commit ? writer.commitTransaction() : writer.rollbackTransaction()) == DBStatus::OK);
    assert(writer.getPageAllocator(db, "t")->flush());
    assert(first.getCurrentReadView()->commitLog->getStatus(exporterXid) ==
           (commit ? CommitLog::Status::Committed : CommitLog::Status::Aborted));
    for (auto* reader : {&first, &second}) readCommand(*reader, db, old, oldNulls);
    // Re-exporting an imported snapshot uses its retained xmax. The exporting
    // importer was allocated beyond that horizon and is already excluded;
    // do not invent an over-horizon active XID or change the original fields.
    const auto forwardedBytes = first.exportSnapshot();
    const auto forwarded = Snapshot::importFromBytes(forwardedBytes); assert(forwarded);
    assert(first.currentTxnId() >= transfer.xmax);
    assert(forwarded->xmin == transfer.xmin && forwarded->xmax == transfer.xmax &&
           forwarded->activeXids == transfer.activeXids && forwarded->subxip == transfer.subxip);
    assert(reimported.beginTransaction(db) == DBStatus::OK && reimported.importSnapshot(forwardedBytes));
    readCommand(reimported, db, old, oldNulls);
    assert(first.beginSqlCommand());
    assert(first.insert(db, "t", {{"id", "7"}, {"val", "importer own"}}) == DBStatus::OK);
    // Imported curCid is serialization metadata, not the importer's command
    // counter. Its own current-command INSERT stays outside this command.
    expectRows(first, db, old, oldNulls);
    assert(first.finishSqlCommand());
    auto importerOwn = old; importerOwn.push_back({"7","importer own"});
    auto importerNulls = oldNulls; importerNulls.push_back({false,false});
    readCommand(first, db, importerOwn, importerNulls);
    readCommand(second, db, old, oldNulls);
    readCommand(reimported, db, old, oldNulls);
    assert(first.rollbackTransaction() == DBStatus::OK);
    assert(second.rollbackTransaction() == DBStatus::OK);
    assert(reimported.rollbackTransaction() == DBStatus::OK);
    auto committed = commit ? own : old;
    auto committedNulls = commit ? ownNulls : oldNulls;
    committed.push_back({"20","foreign"}); committedNulls.push_back({false,false});
    assert(fresh.beginTransaction(db) == DBStatus::OK);
    readCommand(fresh, db, committed, committedNulls);
    assert(fresh.commitTransaction() == DBStatus::OK);
    std::cout << "[SNAPSHOT EXPORTER " << (commit ? "COMMIT" : "ROLLBACK")
              << "] original/imported/own/foreign/NULL/savepoint/CID/copy/horizon passed\n";
}
} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    scenario(true);
    scenario(false);
    std::cout << "[SNAPSHOT EXPORTER VISIBILITY] passed\n";
}
