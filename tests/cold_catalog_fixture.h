#pragma once
#include "commands/TableManage.h"
#include <filesystem>
#include <fstream>

namespace dbms::test {
// Deliberately model a legacy database without a materialized pg_catalog.
// CREATE DATABASE owns system-catalog initialization; invoking it cannot
// construct the cold-reader input these tests are intended to exercise.
inline bool createColdCatalogFixture(StorageEngine& engine,const std::string& database) {
    std::error_code error;
    const auto directory=engine.dbPath(database);
    if(!std::filesystem::create_directory(directory,error) || error)return false;
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"tlist.lst",""},{".charset","utf8\n"},{".schema_public",""}}) {
        std::ofstream output(directory/item.first,std::ios::binary);
        output<<item.second;output.close();if(!output)return false;
    }
    return engine.databaseExists(database) && !std::filesystem::exists(directory/"pg_catalog");
}
} // namespace dbms::test
