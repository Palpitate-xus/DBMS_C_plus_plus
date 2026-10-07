#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

namespace fs = std::filesystem;

static std::vector<unsigned char> bytes(const fs::path& name) {
    std::ifstream input(name, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

template<class Map, class Mutate>
static void replacedOwner(const std::string& name, Mutate mutate) {
    Map map(name);
    assert(map.open());
    mutate(map, 1);
    map.flush();
    const auto before = bytes(name);
    fs::rename(name, name + ".retired");
    {
        std::ofstream out(name, std::ios::binary);
        out.put('x');
    }
    mutate(map, 2);
    map.flush();
    // The file name is not authority to change the unrelated retired owner.
    assert(bytes(name + ".retired") == before);
    assert(bytes(name) == std::vector<unsigned char>{'x'});
    map.close();
    assert(bytes(name + ".retired") == before);
    assert(bytes(name) == std::vector<unsigned char>{'x'});
}

int main() {
    replacedOwner<dbms::FreeSpaceMap>("map.fsm", [](auto& map, int step) {
        map.setFreePercent(1, step == 1 ? 40 : 90);
    });
    replacedOwner<dbms::VisibilityMap>("map.vm", [](auto& map, int step) {
        map.setAllVisible(1, step == 1);
    });
    std::cout << "derived map retired owner remains unchanged\n";
}
