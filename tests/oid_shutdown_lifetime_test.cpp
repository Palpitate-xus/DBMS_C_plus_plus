// test_sources: src/catalog/oid.cpp
#include "catalog/oid.h"

#include <cassert>
#include <iostream>
#include <memory>

struct CatalogShutdown {
    std::unique_ptr<dbms::OidGenerator> generator;
    ~CatalogShutdown() {
        // Real global StorageEngine catalogs can publish their counters
        // during process teardown, after main has finished.
        if (generator) assert(generator->persist());
    }
};

CatalogShutdown catalog;

int main() {
    catalog.generator = std::make_unique<dbms::OidGenerator>("oid_shutdown_counter");
    assert(catalog.generator->allocate() >= dbms::OidGenerator::kFirstUserOid);
    std::cout << "[OID SHUTDOWN LIFETIME] main completed" << std::endl;
}
