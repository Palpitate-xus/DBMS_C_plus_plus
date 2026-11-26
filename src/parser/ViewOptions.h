#pragma once

#include "parser/ast.h"

namespace dbms {

struct ViewOption {
    std::string nameSpace;
    std::string name;
    std::optional<std::string> value;
};

struct CreateViewWithOptionsStmt : CreateViewStmt {
    std::vector<ViewOption> options;
};

struct AlterViewOptionsStmt : AlterObjectStmt {
    explicit AlterViewOptionsStmt(AlterObjectStmt&& view)
        : AlterObjectStmt(std::move(view)) {}

    struct Change {
        bool reset;
        std::vector<ViewOption> options;
    };
    std::vector<Change> changes;
};

} // namespace dbms
