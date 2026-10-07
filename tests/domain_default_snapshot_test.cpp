#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <fstream>
#include <iostream>

int main() {
    using namespace dbms;
    StorageEngine engine;
    const auto db=testDbPath("domain_default_snapshot");
    assert(engine.createDatabase(db)==DBStatus::OK);
    StorageEngine::DomainInfo base; base.name="base_default";base.baseType="int";
    base.hasDefault=true;base.defaultValue="1";
    assert(engine.createDomain(db,base)==DBStatus::OK);
    StorageEngine::DomainInfo child;child.name="child_default";child.baseType="base_default";
    assert(engine.createDomain(db,child)==DBStatus::OK);
    assert(engine.getDomain(db,"child_default").defaultValue=="1");
    auto change=engine.getDomain(db,"base_default");change.defaultValue="2";
    assert(engine.alterDomain(db,"base_default",change)==DBStatus::OK);
    assert(engine.resolveDomainAncestry(db,"child_default").defaultValue=="1");
    auto dropped=engine.getDomain(db,"child_default");dropped.defaultValue.clear();dropped.hasDefault=false;
    assert(engine.alterDomain(db,"child_default",dropped)==DBStatus::OK);
    assert(!engine.resolveDomainAncestry(db,"child_default").hasDefault);
    StorageEngine::DomainInfo nullChild;nullChild.name="null_default";nullChild.baseType="base_default";
    nullChild.hasDefault=true;nullChild.defaultValue="NULL";
    assert(engine.createDomain(db,nullChild)==DBStatus::OK);
    StorageEngine::DomainInfo deeper;deeper.name="deep_null_default";deeper.baseType="null_default";
    assert(engine.createDomain(db,deeper)==DBStatus::OK);
    assert(engine.resolveDomainAncestry(db,"deep_null_default").hasDefault);
    assert(engine.resolveDomainAncestry(db,"deep_null_default").defaultValue=="NULL");
    nullChild=engine.getDomain(db,"null_default");nullChild.defaultValue="3";
    assert(engine.alterDomain(db,"null_default",nullChild)==DBStatus::OK);
    assert(engine.resolveDomainAncestry(db,"deep_null_default").defaultValue=="NULL");
    StorageEngine reader;
    assert(reader.getDomain(db,"child_default").defaultResolved);
    assert(!reader.resolveDomainAncestry(db,"child_default").hasDefault);
    assert(reader.resolveDomainAncestry(db,"deep_null_default").defaultValue=="NULL");
    // A legacy five-field record is kept as legacy, not fabricated into a
    // creation snapshot using today's parent value.
    { std::ofstream legacy(engine.dbPath(db)/".domains",std::ios::app);
      legacy << "legacy_child|base_default|||\n";assert(legacy); }
    assert(!reader.getDomain(db,"legacy_child").defaultResolved);
    assert(reader.resolveDomainAncestry(db,"legacy_child").defaultValue=="2");
    assert(engine.dropDatabase(db)==DBStatus::OK);
    std::cout << "[DOMAIN DEFAULT SNAPSHOT] passed\n";
}
