#include "access/IndexFileUtil.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

namespace fs = std::filesystem;

int main() {
    const fs::path database = "schema_temp_collision";
    fs::create_directory(database);
    const fs::path marker = database / ".schema_customer";
    const fs::path stale = fs::path(
        marker.string() + ".tmp." + std::to_string(::getpid()) + ".0");
    {
        std::ofstream output(stale);
        output << "old crash residue";
    }
    assert(fs::is_regular_file(stale));
    assert(dbms::index_file::writeAtomicallyNoReplace(marker, "") ==
           dbms::index_file::WriteResult::OK);
    assert(fs::is_regular_file(marker));
    assert(fs::file_size(marker) == 0);
    std::ifstream input(stale);
    std::string contents;
    std::getline(input, contents);
    assert(contents == "old crash residue");
    for (const auto& entry : fs::directory_iterator(database)) {
        const auto name = entry.path().filename().string();
        assert(name.rfind(".dbms_atomic_", 0) != 0);
        assert(name.rfind(".schema_", 0) != 0 ||
               entry.path() == marker || entry.path() == stale);
    }
    fs::remove_all(database);
    std::cout << "[SCHEMA MARKER TEMP COLLISION] passed" << std::endl;
    return 0;
}
