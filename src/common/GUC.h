#pragma once

#include <string>
#include <map>
#include <vector>
#include <functional>
#include <optional>
#include <climits>

namespace dbms {

// ============================================================================
// GUC (Grand Unified Configuration) variable framework
// Phase 1.7: defines the variable registry so SET/SHOW/RESET can be parsed
// and executed against a single source of truth.
// ============================================================================

enum class GucContext {
    Internal,      // cannot be set by users
    Postmaster,    // requires server restart
    Sighup,        // can be changed by reload
    UserSet,       // can be changed by any user (SET)
    SuperuserSet,  // only superuser can SET
    Backend        // only at connection start
};

enum class GucType {
    Bool,
    Int,
    Real,
    String,
    Enum
};

struct GUCVariable {
    std::string name;           // canonical lower-case name
    std::string shortDesc;
    std::string longDesc;
    std::string value;          // current value (string representation)
    std::string bootValue;      // default / reset value
    GucType type = GucType::String;
    GucContext context = GucContext::UserSet;
    int minValue = 0;           // for Int
    int maxValue = INT_MAX;     // for Int
    std::vector<std::string> allowedValues; // for Enum
    std::function<bool(const std::string&)> checkHook; // optional validation
    bool assigned = false;      // true if explicitly set this session

    GUCVariable() = default;
    GUCVariable(std::string name_, std::string shortDesc_,
                std::string longDesc_, std::string value_,
                std::string bootValue_, GucType type_ = GucType::String,
                GucContext context_ = GucContext::UserSet,
                int minValue_ = 0, int maxValue_ = INT_MAX,
                std::vector<std::string> allowedValues_ = {},
                std::function<bool(const std::string&)> checkHook_ = nullptr,
                bool assigned_ = false)
        : name(std::move(name_)), shortDesc(std::move(shortDesc_)),
          longDesc(std::move(longDesc_)), value(std::move(value_)),
          bootValue(std::move(bootValue_)), type(type_), context(context_),
          minValue(minValue_), maxValue(maxValue_),
          allowedValues(std::move(allowedValues_)), checkHook(std::move(checkHook_)),
          assigned(assigned_) {}
};

class GUCRegistry {
public:
    static GUCRegistry& instance();

    // Bootstrap standard PostgreSQL-like GUCs.
    void initialize();

    // Register a new variable. Returns false if already registered.
    bool define(const GUCVariable& var);

    // SET (returns error message on failure, empty on success).
    std::string set(const std::string& name, const std::string& value,
                    bool isLocal = false, bool isSuperuser = false);

    // RESET name / RESET ALL
    std::string reset(const std::string& name);
    void resetAll();

    // SHOW name
    std::optional<std::string> get(const std::string& name) const;

    // All variables for SHOW ALL / pg_settings
    std::vector<const GUCVariable*> all() const;

    const GUCVariable* find(const std::string& name) const;

private:
    GUCRegistry() = default;
    std::map<std::string, GUCVariable> vars_;
};

} // namespace dbms
