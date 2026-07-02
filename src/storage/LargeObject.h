#pragma once

#include <map>
#include <string>
#include <vector>

namespace dbms {

// Large Object (bytea LO) support — Phase 10.13
// Stores large blobs as separate chunk files inside {db}/large_objects/
class LargeObjectManager {
public:
    explicit LargeObjectManager(const std::string& dbPath);

    // Create a new LO, returns OID-like ID (here: sequential int).
    int create();

    // Write data to LO at given offset.
    bool write(int loId, size_t offset, const std::string& data);

    // Read data from LO.
    std::string read(int loId, size_t offset = 0, size_t length = 0) const;

    // Truncate LO to given size.
    bool truncate(int loId, size_t size);

    // Delete LO.
    bool drop(int loId);

    // Size of LO.
    size_t size(int loId) const;

    // Import from external file.
    bool importFile(int loId, const std::string& filePath);

    // Export to external file.
    bool exportFile(int loId, const std::string& filePath) const;

private:
    std::string loPath(int loId) const;
    std::string dbPath_;
    std::map<int, size_t> sizes_;
    int nextId_ = 1;
};

} // namespace dbms
