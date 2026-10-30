#pragma once
#include <string>

namespace dbms {
class StorageEngine;
class DdlTransaction;

namespace detail {
// Only the executor owning a live, dirty physical DDL rollback snapshot may
// lend this scope to a heap rewrite. Internal rewritten rows use that outer
// statement boundary; trigger-issued and ordinary INSERTs keep their own
// per-INSERT savepoints. Scope nesting is backend-local and relation-specific.
class DdlRewriteScope {
public:
    DdlRewriteScope(const DdlTransaction& owner, StorageEngine& engine,
                    const std::string& database, const std::string& table);
    ~DdlRewriteScope();
    DdlRewriteScope(const DdlRewriteScope&) = delete;
    DdlRewriteScope& operator=(const DdlRewriteScope&) = delete;
    bool covers(const StorageEngine&, const std::string&, const std::string&) const;
private:
    const DdlTransaction* owner_;
    StorageEngine* engine_;
    std::string database_, table_;
    const DdlRewriteScope* previous_;
};
bool ownsDdlRewrite(const StorageEngine&, const std::string&, const std::string&);
} // namespace detail
} // namespace dbms
