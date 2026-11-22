#pragma once

#include "common/DbError.h"
#include "common/Config.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "utils/Session.h"
#include "utils/permissions.h"
#include <algorithm>
#include <optional>
#include <string>
#include <string_view>

namespace dbms {

inline bool isTrackedSessionSetting(std::string_view name) {
    return name == "work_mem" || findTimeoutParameter(name).has_value();
}

inline bool isSessionSettingFunction(std::string_view name) {
    return name == "current_setting" || name == "set_config";
}

inline bool sessionSettingArity(bool write, size_t count) {
    return write ? count == 3 : count == 1 || count == 2;
}

inline bool sessionSettingBooleanInput(bool write, size_t position) {
    return position == (write ? 2u : 1u);
}

inline std::optional<std::string> showTrackedSetting(const Session& session,
        std::string_view name, size_t defaultWorkMemKb) {
    if (const auto timeout = findTimeoutParameter(name))
        return formatTimeoutMs(session.timeouts.get(*timeout));
    if (name == "work_mem")
        return formatWorkMemKb(session.workMem.effective(defaultWorkMemKb));
    return std::nullopt;
}

enum class SettingChangeScope { Session, Local, ValidateOnly };

// SHOW, SET/RESET and the SQL functions use the same value conversions and
// the same session snapshots. FROM CURRENT copies the typed effective value,
// including an instance default that SQL could not assign explicitly.
inline void assignTrackedSetting(Session& session, StorageEngine& engine,
        const std::string& name, size_t defaultWorkMemKb,
        const std::optional<std::string>& text, SettingChangeScope scope,
        bool fromCurrent = false) {
    const bool local = scope == SettingChangeScope::Local;
    const auto invalidValue = [&]() {
        throw DbError("22023", "invalid value for parameter \"" + name + "\"");
    };
    if (const auto timeout = findTimeoutParameter(name)) {
        if (*timeout == TimeoutParameter::Deadlock && !sessionIsAdmin(session))
            throw DbError("42501", "permission denied to set parameter \"" + name + "\"");
        std::optional<int> value;
        if (fromCurrent) value = session.timeouts.get(*timeout);
        else if (text) {
            value = parseTimeoutMs(*text, timeoutMinimum(*timeout, name));
            if (!value) invalidValue();
        }
        if (scope != SettingChangeScope::ValidateOnly) {
            if (engine.inTransaction()) session.timeouts.begin();
            session.timeouts.set(*timeout, value, local);
            session.timeouts.syncLocks(engine.getLockManager());
        }
        return;
    }
    if (name != "work_mem")
        throw DbError("42704", "unrecognized configuration parameter \"" + name + "\"");

    std::optional<size_t> value;
    if (fromCurrent) value = session.workMem.effective(defaultWorkMemKb);
    else if (text) {
        value = parseWorkMemKb(*text);
        if (!value) invalidValue();
    }
    if (scope != SettingChangeScope::ValidateOnly) {
        if (engine.inTransaction()) session.workMem.begin();
        session.workMem.assign(value, local);
    }
}

inline ExprValue invokeSessionSetting(Session* session, StorageEngine& engine,
        const Config& config, bool write, const std::vector<ExprValue>& args) {
    if (!sessionSettingArity(write, args.size()))
        throw DbError("42883", "configuration function does not accept these arguments");
    if (!write && (args[0].isNull || (args.size() == 2 && args[1].isNull)))
        return ExprValue("text", "", true);
    if (args[0].isNull) throw DbError("22004", "SET requires parameter name");
    if (!session) throw DbError("XX000", "configuration function requires a session");
    std::string name = args[0].value;
    for (char& ch : name)
        if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch - 'A' + 'a');
    // Utility commands additionally accept project-specific _ms aliases.
    if (name != "work_mem" && std::find(timeoutNames.begin(), timeoutNames.end(), name) == timeoutNames.end()) {
        if (!write && args.size() == 2 && args[1].asBool()) return ExprValue("text", "", true);
        throw DbError("42704", "unrecognized configuration parameter \"" + args[0].value + "\"");
    }
    const auto defaultWorkMemKb = config.workMemKb;
    if (write) {
        const auto value = args[1].isNull ? std::nullopt : std::optional<std::string>(args[1].value);
        const auto scope = !args[2].isNull && args[2].asBool()
            ? SettingChangeScope::Local : SettingChangeScope::Session;
        assignTrackedSetting(*session, engine, name, defaultWorkMemKb, value, scope);
    }
    return ExprValue("text", showTrackedSetting(*session, name, defaultWorkMemKb).value());
}

} // namespace dbms
