#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    SQLParser parser;
    for(const bool remove:{false,true}) {
        const auto prefix=remove?"DELETE FROM target USING ":"UPDATE target SET a=1 FROM ";
        auto parsed=parser.parseForBinding(std::string(prefix)+"one a,two b JOIN three c ON b.id=c.id WHERE a.id=b.id RETURNING a.id");
        assert(parsed.isValid());
        const auto* update=dynamic_cast<UpdateStmt*>(parsed.stmt.get());
        const auto* deletion=dynamic_cast<DeleteStmt*>(parsed.stmt.get());
        const auto* source=update?update->fromClause.get():deletion->usingClause.get();
        assert(source->type==FromItem::Type::Join && source->joinType=="CROSS");
        assert(source->left->type==FromItem::Type::Table && source->left->alias=="a");
        assert(source->right->type==FromItem::Type::Join && source->right->joinCondition);
        assert(source->right->left->alias=="b" && source->right->right->alias=="c");
        for(const auto& suffix:{"one,","one, WHERE false","one, RETURNING id","one,b JOIN c"})
            assert(!parser.parseForBinding(std::string(prefix)+suffix).isValid());
    }
    auto parsed=parser.parseForBinding("WITH w AS(SELECT 1) UPDATE target t SET a=CAST(s.t AS INT),b=nextval('seq') FROM source s,w WHERE t.id=s.id");
    assert(parsed.isValid());
    assert(dynamic_cast<WithStmt*>(parsed.stmt.get()));
    std::cout<<"[DML SOURCE LIST] comma source occurrence and explicit JOIN precedence passed\n";
}
