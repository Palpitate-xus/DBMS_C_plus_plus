#pragma once

#include "common/DbError.h"
#include "commands/TableManage.h"
#include "utils/Session.h"
#include "utils/permissions.h"
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

} // namespace dbms
