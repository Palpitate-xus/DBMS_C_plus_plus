// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::string database = "large_object_reopen_test_db";
    std::filesystem::remove_all(database);

    int firstId = 0;
    {
        dbms::LargeObjectManager objects(database);
        firstId = objects.create();
        assert(firstId == 1);
        assert(objects.write(firstId, 0, "persistent payload"));
        assert(objects.size(firstId) == 18);
    }

    {
        dbms::LargeObjectManager reopened(database);
        assert(reopened.read(firstId) == "persistent payload");
        assert(reopened.size(firstId) == 18);

        const int secondId = reopened.create();
        assert(secondId == 2);
        assert(reopened.write(secondId, 0, "second payload"));

        // Allocating and writing the second object must not overwrite the
        // object discovered when the manager was reopened.
        assert(reopened.read(firstId) == "persistent payload");
        assert(reopened.read(secondId) == "second payload");
    }

    // Managers opened against the same starting snapshot must also avoid an
    // allocation collision when one of them creates an object first.
    {
        dbms::LargeObjectManager first(database);
        dbms::LargeObjectManager second(database);
        const int thirdId = first.create();
        const int fourthId = second.create();
        assert(thirdId == 3);
        assert(fourthId == 4);
    }

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT REOPEN TEST] passed\n";
    return 0;
}
