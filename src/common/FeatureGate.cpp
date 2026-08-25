// Feature gate implementation (gap DIV-14 / CAT-22).
// See src/common/FeatureGate.h for the contract.
#include "common/FeatureGate.h"

#include <string>
#include <unordered_set>

namespace dbms {

bool isExtendedCompatMode(const std::string& mode) {
    return mode == kCompatModeExtended;
}

bool compatKindHasRuntime(const std::string& kind) {
    // Kinds whose CREATE/ALTER/DROP previously fell through to the generic
    // compatibility-object record but do have a dedicated runtime handler
    // dispatched *before* the compat layer.  They are listed here so the
    // gate can stay honest if dispatch order changes.
    static const std::unordered_set<std::string> kRuntimeKinds = {
        // CREATE PUBLICATION has a real handler (write-path change capture
        // publication registry) dispatched before the compat layer.
        "publication",
    };
    return kRuntimeKinds.count(kind) > 0;
}

bool compatKindAlwaysUnsupported(const std::string& kind) {
    static const std::unordered_set<std::string> kNeverKinds = {
        // DIV-08: PostgreSQL 18 itself has no SQL ASSERTION implementation;
        // neither compatibility mode may fake one.
        "assertion",
    };
    return kNeverKinds.count(kind) > 0;
}

std::string featureNotSupportedError(const std::string& command) {
    return "ERROR: feature not supported: " + command +
           " is not implemented (SQLSTATE 0A000)";
}

} // namespace dbms
