#pragma once

#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace dbms_test {
// Read the current catalog at each assertion, while retaining every model
// from which this fixture has borrowed rows/OIDs. A replacing DDL must not
// turn those old row pointers into dangling references or the current image.
class CatalogGenerationView {
public:
    CatalogGenerationView(dbms::StorageEngine& engine,std::string database)
        : engine_(engine),database_(std::move(database)) {}

    dbms::CatalogManager& operator()() {
        // get() establishes the cache on a genuinely cold first access;
        // acquire() owns the latest generation, including a peer refresh
        // between those two lookups. Return that lease, never the raw get.
        (void)engine_.catalogService().get(database_);
        auto current=engine_.catalogService().acquire(database_);
        generations_.push_back(current);
        return *current;
    }

private:
    dbms::StorageEngine& engine_;
    std::string database_;
    std::vector<std::shared_ptr<dbms::CatalogManager>> generations_;
};
}
