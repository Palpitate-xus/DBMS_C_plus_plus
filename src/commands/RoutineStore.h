#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

// Signature identity is the ordered vector of input type OIDs, never arity,
// parameter names, return type, typmods, or a currently visible SQL spelling.
struct StoredRoutine {
    std::string schema;
    std::string name;
    uint32_t oid = 0;
    std::vector<std::string> parameterNames;
    std::vector<std::string> parameterTypes;
    std::vector<uint32_t> inputTypeOids;
    std::string returnType = "text";
    uint32_t returnTypeOid = 0;
    std::string language = "sql";
    std::string body;
    char volatility = 'v';
    bool strict = false;
};

class RoutineStore {
public:
    // An embedding decodes its historical files using their actual producer's
    // rules and resolves their declarations without executing the body. V3
    // records already own their OIDs and must not be reinterpreted on load.
    using LegacyReader = std::function<std::optional<StoredRoutine>(
        const std::filesystem::path&, const std::string&, const std::string&)>;

    explicit RoutineStore(std::filesystem::path database, LegacyReader legacy = {});
    std::vector<StoredRoutine> candidates(const std::string& schema,
                                          const std::string& name) const;
    std::vector<std::string> names(const std::string& schema) const;
    std::optional<StoredRoutine> find(const std::string& schema,
        const std::string& name, const std::vector<uint32_t>& inputs) const;

    // The caller owns the database DDL lock and statement/transaction rollback.
    // Publication is atomic and durable. Multi-file legacy retirement or a
    // move may fail after publishing a complete generation; errors are not
    // swallowed and the enclosing owner must restore its snapshot on failure.
    StoredRoutine put(StoredRoutine routine, bool replace = false) const;
    bool erase(const std::string& schema, const std::string& name,
               const std::vector<uint32_t>& inputs) const;
    StoredRoutine move(const std::string& schema, const std::string& name,
        const std::vector<uint32_t>& inputs, const std::string& newSchema,
        const std::string& newName) const;

    static std::string encode(const StoredRoutine& routine);
    static StoredRoutine decode(const std::string& bytes);
    std::filesystem::path path(const std::string& schema,
        const std::string& name, const std::vector<uint32_t>& inputs) const;

private:
    std::filesystem::path database_;
    LegacyReader legacy_;
    std::optional<std::filesystem::path> legacyPath(
        const std::string& schema, const std::string& name) const;
    std::optional<StoredRoutine> legacy(const std::string& schema,
                                       const std::string& name) const;
};

} // namespace dbms
