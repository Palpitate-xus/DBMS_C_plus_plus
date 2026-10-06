#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const std::string sql : {
        "INSERT INTO t VALUES(1,2),(3,4)",
        "INSERT INTO t(a,b) VALUES(DEFAULT,CAST(2 AS INT)),(NULL,((3+4)))",
        "INSERT INTO t VALUES(1,'VALUES ( )'),(2,'a,b') RETURNING a",
        "INSERT INTO t VALUES(1,2 /* ) , ( */),(3,4) ON CONFLICT(a) DO NOTHING",
        "INSERT INTO t VALUES(1,2) ON CONFLICT(a) DO UPDATE SET b=excluded.b WHERE true",
        "INSERT INTO t VALUES(1,ARRAY[1,2]),(3,ARRAY[4,5])",
        "INSERT INTO \"values\"(\"V\") VALUES(E'escaped\\ntext'),($tag$a ( ) VALUES$tag$)",
        "INSERT INTO t DEFAULT VALUES", "INSERT INTO t SELECT 1,2",
        "INSERT INTO t VALUES(1,2),(3)"}) {
        const auto ordinary=parser.parse(sql);
        const auto strict=parser.parseForBinding(sql);
        if (!ordinary.success || !strict.success)
            std::cerr<<sql<<": ordinary="<<ordinary.error<<" strict="<<strict.error<<'\n';
        assert(ordinary.success && strict.success);
    }
    for (const std::string sql : {
        "INSERT INTO t VALUES(1,2)(3,4)",
        "INSERT INTO t VALUES(1 2,3)",
        "INSERT INTO t VALUES(1,2 bogus)",
        "INSERT INTO t VALUES(1,2),(3,4 bogus)",
        "INSERT INTO t VALUES(1,2),", "INSERT INTO t VALUES(1,2), RETURNING a",
        "INSERT INTO t VALUES(1,2", "INSERT INTO t VALUES(1,)",
        "INSERT INTO t VALUES(,1)", "INSERT INTO t VALUES()",
        "INSERT INTO t VALUES",
        "INSERT INTO t VALUES(1,2),,(3,4)", "INSERT INTO t VALUES(*)"}) {
        const auto ordinary=parser.parse(sql);
        const auto strict=parser.parseForBinding(sql);
        if (ordinary.success || strict.success)
            std::cerr<<"malformed VALUES accepted: "<<sql<<'\n';
        assert(!ordinary.success && !strict.success);
    }
    std::cout<<"[INSERT VALUES SYNTAX] separators, row shape and supported grammar passed\n";
}
