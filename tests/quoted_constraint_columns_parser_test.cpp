#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    auto self = parser.parse("CREATE TABLE \"schema space\".\"self.node\"(\"key id\" INT PRIMARY KEY,\"parent id\" INT REFERENCES \"schema space\".\"self.node\"(\"key id\"))");
    assert(self.success);
    const auto* selfTable = dynamic_cast<const dbms::CreateTableStmt*>(self.stmt.get());
    assert(selfTable && selfTable->constraints.size() == 1);
    assert(selfTable->constraints[0].refTable == "\"schema space\".\"self.node\"");
    assert(selfTable->constraints[0].columns == std::vector<std::string>{"parent id"});
    assert(selfTable->constraints[0].refColumns == std::vector<std::string>{"key id"});
    auto table = parser.parse("CREATE TABLE child(\"a key\" INT,\"b\"\"key\" TEXT,PRIMARY KEY(\"a key\"),UNIQUE(\"a key\",\"b\"\"key\"),CONSTRAINT pair_fk FOREIGN KEY(\"a key\",\"b\"\"key\") REFERENCES \"Parent.Table\"(\"a key\",\"b\"\"key\"))");
    assert(table.success);
    const auto* definition = dynamic_cast<const dbms::CreateTableStmt*>(table.stmt.get());
    assert(definition && definition->constraints.size() == 3);
    assert(definition->constraints[0].columns == std::vector<std::string>{"a key"});
    const std::vector<std::string> pair{"a key", "b\"key"};
    assert(definition->constraints[1].columns == pair);
    assert(definition->constraints[2].columns == pair);
    assert(definition->constraints[2].refColumns == pair);
    assert(definition->constraints[2].refTable == "\"Parent.Table\"");
    auto alter = parser.parse("ALTER TABLE child ADD CONSTRAINT pair_fk FOREIGN KEY(\"a key\",\"b\"\"key\") REFERENCES \"schema space\".\"Parent.Table\"(\"a key\",\"b\"\"key\")");
    assert(alter.success);
    const auto* change = dynamic_cast<const dbms::AlterTableStmt*>(alter.stmt.get());
    assert(change && change->subCommands.size() == 1);
    assert(change->subCommands[0].constraint.columns == pair);
    assert(change->subCommands[0].constraint.refColumns == pair);
    assert(change->subCommands[0].constraint.refTable == "\"schema space\".\"Parent.Table\"");
    auto upper = parser.parse("CREATE TABLE upper_child(ID INT,CONSTRAINT upper_fk FOREIGN KEY(ID) REFERENCES UPPER_PARENT(ID))");
    assert(upper.success);
    const auto* upperTable = dynamic_cast<const dbms::CreateTableStmt*>(upper.stmt.get());
    assert(upperTable && upperTable->constraints[0].columns == std::vector<std::string>{"id"});
    assert(upperTable->constraints[0].refColumns == std::vector<std::string>{"id"});
    assert(upperTable->constraints[0].refTable == "upper_parent");
    std::cout << "[QUOTED CONSTRAINT COLUMNS PARSER] passed" << std::endl;
}
