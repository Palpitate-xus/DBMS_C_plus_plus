#pragma once

#include <string>

namespace dbms {

// A physical producer's output role, independent of displayed values and
// result-type inference. None includes virtual/generated expression output.
struct StoredReferenceOutputRole {
    enum class Kind { None, Regtype, RegtypeArray, Regclass, RegclassArray };
    Kind kind = Kind::None;
    std::string sourceName;

    bool operator==(const StoredReferenceOutputRole& other) const noexcept {
        return kind == other.kind && sourceName == other.sourceName;
    }
    bool operator!=(const StoredReferenceOutputRole& other) const noexcept {
        return !(*this == other);
    }
};

// Call only with a genuine physical column declaration. Stored domains
// already carry their resolved base dataType; unresolved names stay None.
template<class Column>
StoredReferenceOutputRole storedReferenceOutputRoleForColumn(const Column& column) {
    StoredReferenceOutputRole result;
    if (column.generatedKind == 'v') return result;
    if (column.dataType == "regtype")
        result.kind = column.isArray ? StoredReferenceOutputRole::Kind::RegtypeArray
                                     : StoredReferenceOutputRole::Kind::Regtype;
    else if (column.dataType == "regclass")
        result.kind = column.isArray ? StoredReferenceOutputRole::Kind::RegclassArray
                                     : StoredReferenceOutputRole::Kind::Regclass;
    if (result.kind != StoredReferenceOutputRole::Kind::None)
        result.sourceName = column.dataName;
    return result;
}

} // namespace dbms
