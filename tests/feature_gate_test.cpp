#include "common/FeatureGate.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    using namespace dbms;

    assert(std::string(kCompatModePostgresql18) == "postgresql18");
    assert(std::string(kCompatModeExtended) == "extended");
    assert(std::string(kCompatKindAssertion) == "assertion");
    assert(compatKindAlwaysUnsupported("assertion"));
    assert(!compatKindAlwaysUnsupported("extension"));
    assert(!compatKindAlwaysUnsupported("ASSERTION"));

    assert(featureNotSupportedError("CREATE assertion") ==
           "ERROR: feature not supported: CREATE assertion is not implemented "
           "(SQLSTATE 0A000)");

    std::cout << "[FEATURE GATE] SQL ASSERTION is permanently unsupported\n";
    return 0;
}
