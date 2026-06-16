// OID 分配器测试：分配、批量分配、回收复用、持久化。
// test_sources: src/catalog/oid.cpp

#include "catalog/oid.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    std::string counter = "/tmp/dbms_oid_test_counter";
    std::string freelist = counter + ".free";
    std::filesystem::remove(counter);
    std::filesystem::remove(freelist);

    {
        OidGenerator gen(counter);
        assert(gen.peekNext() == OidGenerator::kFirstUserOid);

        Oid a = gen.allocate();
        Oid b = gen.allocate();
        assert(a == OidGenerator::kFirstUserOid);
        assert(b == a + 1);
        std::cout << "[OID] sequential allocation OK\n";

        Oid batchStart = gen.allocateBatch(3);
        assert(batchStart == b + 1);
        assert(gen.peekNext() == batchStart + 3);
        std::cout << "[OID] batch allocation OK\n";

        gen.deallocate(a);
        assert(gen.freeCount() == 1);
        std::cout << "[OID] deallocation OK\n";

        Oid reused = gen.allocate();
        assert(reused == a);
        assert(gen.freeCount() == 0);
        std::cout << "[OID] reuse freed OID OK\n";
    }

    {
        // 验证持久化：计数器与空闲列表都能从文件恢复
        OidGenerator gen(counter);
        assert(gen.peekNext() == OidGenerator::kFirstUserOid + 5);
        assert(gen.freeCount() == 0);
        std::cout << "[OID] persistence OK\n";
    }

    std::filesystem::remove(counter);
    std::filesystem::remove(freelist);
    std::cout << "[OID] all passed\n";
    return 0;
}
