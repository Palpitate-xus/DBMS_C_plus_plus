#include "commands/TableManage.h"
#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include "storage/PageWrapper.h"
#include "storage/PageAllocator.h"
#include <cassert>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string database = "__t_derived_map_engine_coherence";

static uint8_t actualFreeSpace(StorageEngine& engine, const std::string& table = "items") {
    auto* allocator = engine.getPageAllocator(database, table);
    assert(allocator && allocator->numPages() > 1);
    auto* buffer = allocator->fetchPage(1);
    assert(buffer);
    const auto schema = engine.getTableSchema(database, table);
    PageWrapper page(buffer, allocator->pageSize(), schema.formatVersion);
    const auto result = static_cast<uint8_t>(page.freeSpace() * 100 / allocator->pageSize());
    allocator->unpinPage(1);
    return result;
}

static void committedPage() {
    StorageEngine writer, reader;
    writer.setBackgroundIntervals(1000000, 1000000);
    reader.setBackgroundIntervals(1000000, 1000000);
    if (!writer.databaseExists(database))
        assert(writer.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "committed";
    table.append(makeIntColumn("id", false, 2, false));
    table.append(makeStringColumn("payload", false, 512));
    assert(writer.createTable(database, table) == DBStatus::OK);
    assert(writer.beginTransaction(database) == DBStatus::OK);
    assert(writer.insert(database, "committed", {{"id", "1"}, {"payload", std::string(512, 'x')}}) == DBStatus::OK);
    assert(writer.commitTransaction() == DBStatus::OK);
    assert(writer.getFSM(database, "committed")->flushChecked());
    assert(writer.getVM(database, "committed")->flushChecked());
    auto* fsm = reader.getFSM(database, "committed");
    auto* vm = reader.getVM(database, "committed");
    const auto before = actualFreeSpace(writer, "committed");
    assert(fsm->getFreePercent(1) == before);
    // This page genuinely exists and contains only an already committed tuple.
    vm->setAllVisible(1, true);
    assert(vm->flushChecked());
    assert(writer.beginTransaction(database) == DBStatus::OK);
    assert(writer.insert(database, "committed", {{"id", "2"}, {"payload", std::string(512, 'y')}}) == DBStatus::OK);
    const auto after = actualFreeSpace(writer, "committed");
    assert(before != after);
    std::cout << "committed_page_before=" << static_cast<unsigned>(before)
              << " actual_after=" << static_cast<unsigned>(after)
              << " cached_after=" << static_cast<unsigned>(fsm->getFreePercent(1))
              << " all_visible=" << vm->isAllVisible(1) << std::endl;
    assert(fsm->getFreePercent(1) == after);
    assert(!vm->isAllVisible(1));
    assert(writer.rollbackTransaction() == DBStatus::OK);
    assert(!vm->isAllVisible(1));
    assert(fsm->getFreePercent(1) == actualFreeSpace(writer, "committed"));
    assert(writer.query(database, "committed", {}, {"id"}).size() == 1);
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "committed-page") { committedPage(); return 0; }
    if (argc == 2 && std::string(argv[1]) == "verify") {
        StorageEngine cold;
        const auto rows = cold.query(database, "items", {}, {"id"});
        assert(rows.size() == 3);
        assert(cold.getFSM(database, "items")->getFreePercent(1) == actualFreeSpace(cold));
        assert(!cold.getVM(database, "items")->isAllVisible(1));
        return 0;
    }
    {
        StorageEngine writer, reader;
        writer.setBackgroundIntervals(1000000, 1000000);
        reader.setBackgroundIntervals(1000000, 1000000);
        assert(writer.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.append(makeIntColumn("id", false, 2, true));
        table.pkColIndices.push_back(0);
        assert(writer.createTable(database, table) == DBStatus::OK);
        auto* readerFSM = reader.getFSM(database, "items");
        auto* readerVM = reader.getVM(database, "items");
        assert(readerFSM && readerVM);
        // Warm reader's independent map before any heap page exists.
        assert(readerFSM->getFreePercent(1) == 255);
        readerVM->setAllVisible(1, true);
        assert(readerVM->flushChecked());
        assert(writer.beginTransaction(database) == DBStatus::OK);
        assert(writer.insert(database, "items", {{"id", "1"}}) == DBStatus::OK);
        const auto actual = actualFreeSpace(writer);
        std::cout << "actual_heap_free=" << static_cast<unsigned>(actual)
                  << " reader_fsm=" << static_cast<unsigned>(readerFSM->getFreePercent(1))
                  << " reader_vm=" << readerVM->isAllVisible(1) << std::endl;
        if (argc == 2 && std::string(argv[1]) == "vm-first")
            assert(!readerVM->isAllVisible(1));
        assert(readerFSM->getFreePercent(1) == actual);
        assert(!readerVM->isAllVisible(1)); // DML's pending invalidation, not a name check.
        assert(writer.commitTransaction() == DBStatus::OK);
        assert(reader.beginTransaction(database) == DBStatus::OK);
        assert(reader.insert(database, "items", {{"id", "2"}}) == DBStatus::OK);
        assert(reader.commitTransaction() == DBStatus::OK);
        assert(writer.getFSM(database, "items")->getFreePercent(1) == actualFreeSpace(reader));
        // Destruction of a third owner cannot erase another owner's pending map.
        {
            StorageEngine observer;
            observer.setBackgroundIntervals(1000000, 1000000);
            assert(observer.getFSM(database, "items")->getFreePercent(1) == actualFreeSpace(reader));
            assert(!observer.getVM(database, "items")->isAllVisible(1));
        }
        assert(writer.beginTransaction(database) == DBStatus::OK);
        assert(writer.insert(database, "items", {{"id", "3"}}) == DBStatus::OK);
        assert(writer.commitTransaction() == DBStatus::OK);
        assert(writer.getFSM(database, "items")->flushChecked());
        assert(readerVM->flushChecked());
        assert(writer.query(database, "items", {}, {"id"}).size() == 3);
    }
    committedPage();
    const auto child = ::fork();
    assert(child >= 0);
    if (child == 0) { ::execl(argv[0], argv[0], "verify", nullptr); _exit(127); }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    std::cout << "derived maps match real heap pages across engines and fresh exec\n";
}
