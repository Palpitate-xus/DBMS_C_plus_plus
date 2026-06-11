#include "oid.h"
#include <fstream>
#include <iostream>

namespace dbms {

OidGenerator::OidGenerator(const std::string& persistPath)
    : persistPath_(persistPath) {
    std::ifstream in(persistPath);
    uint32_t val = 0;
    if (in >> val) {
        nextOid_.store(val, std::memory_order_relaxed);
    }
}

Oid OidGenerator::allocate() {
    Oid oid = nextOid_.fetch_add(1, std::memory_order_relaxed);
    // Lazy persist: write every 100 allocations
    if (oid % 100 == 0) {
        persist();
    }
    return oid;
}

Oid OidGenerator::allocateBatch(uint32_t count) {
    Oid start = nextOid_.fetch_add(count, std::memory_order_relaxed);
    persist();
    return start;
}

Oid OidGenerator::peekNext() const {
    return nextOid_.load(std::memory_order_relaxed);
}

void OidGenerator::persist() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ofstream out(persistPath_);
    if (out) {
        out << nextOid_.load(std::memory_order_relaxed) << "\n";
    }
}

void OidGenerator::setNext(Oid next) {
    nextOid_.store(next, std::memory_order_relaxed);
    persist();
}

} // namespace dbms
