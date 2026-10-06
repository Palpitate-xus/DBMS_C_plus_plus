#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "sequence_alter_position", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    SequenceInfo bounded;
    bounded.minValue = 1; bounded.hasMinValue = true;
    bounded.maxValue = 10; bounded.hasMaxValue = true;
    bounded.cycle = true; bounded.cycleSpecified = true;
    assert(g_engine.createSequence(db, "direction", bounded) == DBStatus::OK);
    assert(g_engine.nextval(db, "direction") == 1);
    assert(g_engine.nextval(db, "direction") == 2);
    SequenceInfo change;
    change.increment = -1; change.incrementSpecified = true;
    assert(g_engine.alterSequence(db, "direction", change) == DBStatus::OK);
    const auto descending = g_engine.nextval(db, "direction");
    std::cout << "ALTER DIRECTION AFTER LAST 2 = " << descending << " expected 1" << std::endl;
    assert(descending == 1);
    assert(g_engine.nextval(db, "direction") == 10);

    SequenceInfo cached;
    cached.cache = 5; cached.cacheSpecified = true;
    assert(g_engine.createSequence(db, "cached", cached) == DBStatus::OK);
    assert(g_engine.nextval(db, "cached") == 1);
    change.increment = 2;
    assert(g_engine.alterSequence(db, "cached", change) == DBStatus::OK);
    assert(g_engine.nextval(db, "cached") == 7); // invalidate caller cache, start after disk high-water 5

    SequenceInfo uncalled;
    uncalled.start = 5; uncalled.startSpecified = true;
    assert(g_engine.createSequence(db, "uncalled", uncalled) == DBStatus::OK);
    change.increment = 3;
    assert(g_engine.alterSequence(db, "uncalled", change) == DBStatus::OK);
    assert(g_engine.nextval(db, "uncalled") == 5);
    assert(g_engine.setval(db, "uncalled", 42, false) == 42);
    change.increment = -1;
    assert(g_engine.alterSequence(db, "uncalled", change) == DBStatus::OK);
    assert(g_engine.nextval(db, "uncalled") == 42);
    assert(g_engine.nextval(db, "uncalled") == 41);

    SequenceInfo atMinimum;
    atMinimum.start = 10; atMinimum.startSpecified = true;
    atMinimum.increment = 2; atMinimum.incrementSpecified = true;
    atMinimum.minValue = 10; atMinimum.hasMinValue = true;
    atMinimum.maxValue = 30; atMinimum.hasMaxValue = true;
    assert(g_engine.createSequence(db, "at_minimum", atMinimum) == DBStatus::OK);
    assert(g_engine.nextval(db, "at_minimum") == 10);
    change.increment = -2;
    assert(g_engine.alterSequence(db, "at_minimum", change) == DBStatus::OK);
    bool exhausted = false;
    try { (void)g_engine.nextval(db, "at_minimum"); }
    catch (const DbError& error) { exhausted = error.sqlState() == "2200H"; }
    assert(exhausted);
    // Direct native ALTER has no SQL owner to perform commit-time pruning.
    // All four live declarations, and only those, retain a runtime fork.
    assert(std::distance(std::filesystem::directory_iterator(db + "/.sequence_runtime"),
                         std::filesystem::directory_iterator{}) == 4);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[SEQUENCE ALTER POSITION] passed\n";
}
