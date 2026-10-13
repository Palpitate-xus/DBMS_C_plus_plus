// test_sources: src/replication/LogicalDecoder.cpp
#include "replication/LogicalDecoder.h"
#include "common/DbError.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

int main() {
    using namespace dbms;
    namespace fs = std::filesystem;
    char pattern[] = "/tmp/dbms-publication-read-XXXXXX";
    const char* created = ::mkdtemp(pattern);
    assert(created);
    const fs::path directory(created);
    auto& catalog = PublicationCatalog::instance();
    Publication publication;
    publication.name = "normal_rows";
    publication.owner = "test";
    publication.tables = {"rows"};
    publication.publishDelete = false;
    std::string error;
    assert(catalog.create(directory.string(), publication, error));
    assert(catalog.publishes(directory.string(), "rows"));
    assert(!catalog.publishes(directory.string(), "other"));
    assert(catalog.publishes(directory.string(), "rows", LogicalChange::Op::Insert));
    assert(!catalog.publishes(directory.string(), "rows", LogicalChange::Op::Delete));
    const fs::path invalid = directory / "incomplete.publication";
    { std::ofstream out(invalid); out << "INCOMPLETE\n"; assert(out.good()); }
    int failures = 0;
    for (int kind = -1; kind < 4; ++kind) {
        bool rejected = false;
        try {
            if (kind == -1) (void)catalog.publishes(directory.string(), "rows");
            else (void)catalog.publishes(directory.string(), "rows", static_cast<LogicalChange::Op>(kind));
        } catch (const DbError& failure) {
            rejected = failure.sqlState() == "58030";
        }
        if (!rejected) { ++failures; std::cerr << "publication read failure swallowed kind=" << kind << '\n'; }
    }
    // Rejection must leave both images in place. A normal repair permits a
    // retry; it is not interpreted as a lasting negative membership cache.
    assert(fs::is_regular_file(invalid));
    assert(fs::is_regular_file(directory / "normal_rows.publication"));
    fs::remove(invalid);
    assert(catalog.publishes(directory.string(), "rows"));
    assert(!catalog.publishes(directory.string(), "rows", LogicalChange::Op::Delete));
    fs::remove_all(directory);
    std::cout << "[PUBLICATION READ] swallowed failures=" << failures << '\n';
    return failures ? 1 : 0;
}
