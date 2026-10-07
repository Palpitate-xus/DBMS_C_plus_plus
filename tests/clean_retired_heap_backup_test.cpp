#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/BufferPool.h"

#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;
namespace fs = std::filesystem;

static TableSchema schema() {
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 4, true));
    return table;
}

static void cleanRestoredGeneration() {
    const std::string db = "__t_clean_retired_backup";
    const std::string seed = db + "_seed";
    const std::string copy = db + "_copy";
    StorageEngine producer, observer;
    assert(producer.createDatabase(db) == DBStatus::OK);
    assert(producer.createTable(db, schema()) == DBStatus::OK);
    assert(producer.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(producer.physicalBackup(db, seed));
    assert(observer.query(db, "items", {}, {"id"}) == std::vector<std::string>{"1 "});
    PageAllocator* old = observer.getPageAllocator(db, "items");
    assert(old && old->isOpen() && old->flush());
    assert(producer.insert(db, "items", {{"id", "2"}}) == DBStatus::OK);
    assert(producer.physicalRestore(db, seed));
    assert(old->isOpen() && !old->bufferPool()->refersToCurrentFiles());
    for (const auto& frame : old->bufferPool()->getFrameInfo()) {
        assert(!frame.dirty && frame.pinCount == 0);
    }
    assert(!old->flush()); // The low-level stale-owner contract stays strict.
    assert(observer.physicalBackup(db, copy));
    assert(producer.physicalRestore(db, copy));
    assert(observer.query(db, "items", {}, {"id"}) == std::vector<std::string>{"1 "});
}

static void dirtyClosedAndMissingStillFail() {
    const std::string db = "__t_retired_backup_faults";
    const std::string copy = db + "_copy";
    StorageEngine engine;
    assert(engine.createDatabase(db) == DBStatus::OK);
    assert(engine.createTable(db, schema()) == DBStatus::OK);
    assert(engine.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    PageAllocator* owner = engine.getPageAllocator(db, "items");
    assert(owner && owner->flush());
    const fs::path path = fs::path(db) / "items.dt";
    const fs::path saved = fs::path(db) / "items.dt.saved";
    char* page = owner->fetchPage(1);
    assert(page);
    owner->markDirty(1);
    owner->unpinPage(1);
    fs::rename(path, saved);
    assert(fs::copy_file(saved, path));
    assert(!owner->bufferPool()->refersToCurrentFiles());
    assert(!engine.physicalBackup(db, copy)); // Never discard dirty old frames.
    assert(fs::remove(path));
    fs::rename(saved, path);
    assert(owner->flush());

    page = owner->fetchPage(1);
    assert(page);
    fs::rename(path, saved);
    assert(fs::copy_file(saved, path));
    assert(!engine.physicalBackup(db, copy)); // An active clean pin is not idle.
    owner->unpinPage(1);
    assert(fs::remove(path));
    fs::rename(saved, path);

    page = owner->fetchPage(1);
    assert(page);
    owner->bufferPool()->invalidatePage(1);
    for (const auto& frame : owner->bufferPool()->getFrameInfo())
        assert(frame.pageId != 1); // The diagnostic list omits orphan pins.
    fs::rename(path, saved);
    assert(fs::copy_file(saved, path));
    const bool orphanBackup = engine.physicalBackup(db, copy);
    std::cerr << "[RETIRED ORPHAN PIN] backup=" << orphanBackup << '\n';
    assert(!orphanBackup); // The retained reader still owns this old frame.
    assert(fs::remove(path));
    fs::rename(saved, path);
    owner->unpinPage(1);

    owner->close();
    assert(!owner->isOpen());
    assert(!engine.physicalBackup(db, copy)); // No implicit reopen of a fault.
    assert(owner->open());
    fs::rename(path, saved);
    assert(!engine.physicalBackup(db, copy)); // Missing live main is not retired.
    assert(!fs::exists(path));
    fs::rename(saved, path);
    assert(engine.physicalBackup(db, copy));
    assert(engine.query(db, "items", {}, {"id"}) == std::vector<std::string>{"1 "});
}

int main() {
    cleanRestoredGeneration();
    dirtyClosedAndMissingStillFail();
    std::cout << "[CLEAN RETIRED HEAP] current-file backup succeeds; dirty/pinned/closed/missing remain failures\n";
}
