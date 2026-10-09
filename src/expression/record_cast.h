#pragma once

#include "expression/common_type.h"

namespace dbms::record_cast_detail {
// Anonymous records have no input descriptor. String I/O casts are admitted
// by their types, but non-NULL input is rejected by the runtime codec.
inline void validate(const std::string& input, const std::string& target) {
    const auto source = common_type_detail::canonical(input);
    const auto destination = common_type_detail::canonical(target);
    const auto stringType = [](const std::string& type) {
        return type == "text" || type == "varchar" || type == "bpchar" || type == "name";
    };
    if ((source == "record" && destination != "record" && !stringType(destination)) ||
        (destination == "record" && source != "record" && source != "unknown" && !stringType(source)))
        throw DbError("42846", "cannot cast type " + input + " to " + target);
}
} // namespace dbms::record_cast_detail
