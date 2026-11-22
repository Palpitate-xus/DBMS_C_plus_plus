#pragma once

#include "commands/TableManage.h"

namespace dbms {

// The projection is bound once before COPY starts. An absent key in a
// supplied field denotes a DEFAULT marker. A field past fieldCount is missing.
struct StorageEngine::CopyInputOrder {
    struct Field {
        size_t index;
        unsigned integerBits;
    };
    const std::vector<Field>& columns;
    size_t fieldCount;
    // If supplied, report the input field rejected during conversion.
    std::optional<size_t>* rejectedField = nullptr;
};

} // namespace dbms
