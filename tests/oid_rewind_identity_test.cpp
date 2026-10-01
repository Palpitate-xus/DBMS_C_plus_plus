// test_sources: src/catalog/oid.cpp
#include "catalog/oid.h"
#include "common/DbError.h"

#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

int main() {
    const std::string counter = "oid_rewind_identity_counter";
    dbms::Oid first;
    {
        dbms::OidGenerator generator(counter);
        first = generator.allocate();
        assert(generator.persist());
    }
    // Simulate restoring a physical DDL snapshot taken before allocation.
    { std::ofstream savedImage(counter); savedImage << first << '\n'; }
    {
        dbms::OidGenerator restored(counter);
        assert(restored.allocate() > first);
        const dbms::Oid start = restored.allocateBatch(3);
        assert(start > first);
        assert(restored.peekNext() == start + 3);
    }
    dbms::OidGenerator left(counter), right(counter);
    std::vector<dbms::Oid> allocated;
    std::mutex valuesMutex;
    const auto reserve = [&](dbms::OidGenerator& generator) {
        for (int i = 0; i < 100; ++i) {
            const dbms::Oid oid = generator.allocate();
            std::lock_guard<std::mutex> lock(valuesMutex);
            allocated.push_back(oid);
        }
    };
    std::thread a(reserve, std::ref(left)), b(reserve, std::ref(right));
    a.join(); b.join();
    std::sort(allocated.begin(), allocated.end());
    assert(allocated.size() == 200);
    assert(std::adjacent_find(allocated.begin(), allocated.end()) == allocated.end());
    assert(left.persist() && right.persist());
    dbms::OidGenerator reopened(counter);
    assert(reopened.allocate() > allocated.back());
    dbms::OidGenerator exhausted("oid_exhaustion_identity_counter");
    exhausted.setNext(std::numeric_limits<dbms::Oid>::max() - 1);
    assert(exhausted.allocateBatch(0) == dbms::OidGenerator::kInvalidOid);
    try {
        (void)exhausted.allocateBatch(2);
        assert(false);
    } catch (const dbms::DbError& error) {
        assert(error.sqlState() == "54000");
    }
    assert(exhausted.peekNext() == std::numeric_limits<dbms::Oid>::max() - 1);
    assert(exhausted.allocate() == std::numeric_limits<dbms::Oid>::max() - 1);
    try {
        (void)exhausted.allocate();
        assert(false);
    } catch (const dbms::DbError& error) {
        assert(error.sqlState() == "54000");
    }
    std::cout << "[OID REWIND IDENTITY] passed" << std::endl;
}
