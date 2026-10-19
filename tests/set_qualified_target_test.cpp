#include "parser/parser.h"
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace {
size_t checks=0,failures=0;
void record(bool pass,const std::string& sql,const std::string& detail) {
    ++checks;
    if(!pass) {++failures;std::cerr<<"SET_TARGET_FAIL "<<checks<<' '<<sql<<' '<<detail<<'\n';}
}
void setting(const std::string& sql,const std::string& name,
             const std::vector<std::string>& values,bool valid=true,bool local=false) {
    dbms::SQLParser parser;
    for(const bool binding:{false,true}) {
        const auto parsed=binding?parser.parseForBinding(sql):parser.parse(sql);
        const auto* statement=parsed.success?dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get()):nullptr;
        const bool pass=valid
            ?statement && statement->name==name && statement->values==values &&
                statement->scope==(local?dbms::SetStmt::Scope::Local:dbms::SetStmt::Scope::Session)
            :!parsed.success && parsed.sqlState=="42601";
        record(pass,sql,(binding?"binding ":"ordinary ")+parsed.error+
               (statement?" name="+statement->name:"")+" state="+parsed.sqlState);
    }
}
}

int main() {
    // Independent accepted golden keyword facts, not the classifier under test.
    // PG 18.6 ColId accepts U/C but excludes the 23 type/function-name keywords.
    const std::set<std::string> typeFunctionNames={
        "authorization","binary","collation","concurrently","cross","current_schema","freeze","full","ilike","inner","is","isnull","join","left","like","natural","notnull","outer","overlaps","right","similar","tablesample","verbose"
    };
    std::istringstream keywords(R"KEYWORDS(
abort:U absent:U absolute:U access:U action:U add:U admin:U
after:U aggregate:U all:R also:U alter:U always:U analyse:R
analyze:R and:R any:R array:R as:R asc:R asensitive:U
assertion:U assignment:U asymmetric:R at:U atomic:U attach:U attribute:U
authorization:C backward:U before:U begin:U between:C bigint:C binary:C
bit:C boolean:C both:R breadth:U by:U cache:U call:U
called:U cascade:U cascaded:U case:R cast:R catalog:U chain:U
char:C character:C characteristics:U check:R checkpoint:U class:U close:U
cluster:U coalesce:C collate:R collation:C column:R columns:U comment:U
comments:U commit:U committed:U compression:U concurrently:C conditional:U configuration:U
conflict:U connection:U constraint:R constraints:U content:U continue:U conversion:U
copy:U cost:U create:R cross:C csv:U cube:U current:U
current_catalog:R current_date:R current_role:R current_schema:C current_time:R current_timestamp:R current_user:R
cursor:U cycle:U data:U database:U day:U deallocate:U dec:C
decimal:C declare:U default:R defaults:U deferrable:R deferred:U definer:U
delete:U delimiter:U delimiters:U depends:U depth:U desc:R detach:U
dictionary:U disable:U discard:U distinct:R do:R document:U domain:U
double:U drop:U each:U else:R empty:U enable:U encoding:U
encrypted:U end:R enforced:U enum:U error:U escape:U event:U
except:R exclude:U excluding:U exclusive:U execute:U exists:C explain:U
expression:U extension:U external:U extract:C false:R family:U fetch:R
filter:U finalize:U first:U float:C following:U for:R force:U
foreign:R format:U forward:U freeze:C from:R full:C function:U
functions:U generated:U global:U grant:R granted:U greatest:C group:R
grouping:C groups:U handler:U having:R header:U hold:U hour:U
identity:U if:U ilike:C immediate:U immutable:U implicit:U import:U
in:R include:U including:U increment:U indent:U index:U indexes:U
inherit:U inherits:U initially:R inline:U inner:C inout:C input:U
insensitive:U insert:U instead:U int:C integer:C intersect:R interval:C
into:R invoker:U is:C isnull:C isolation:U join:C json:C
json_array:C json_arrayagg:C json_exists:C json_object:C json_objectagg:C json_query:C json_scalar:C
json_serialize:C json_table:C json_value:C keep:U key:U keys:U label:U
language:U large:U last:U lateral:R leading:R leakproof:U least:C
left:C level:U like:C limit:R listen:U load:U local:U
localtime:R localtimestamp:R location:U lock:U locked:U logged:U mapping:U
match:U matched:U materialized:U maxvalue:U merge:U merge_action:C method:U
minute:U minvalue:U mode:U month:U move:U name:U names:U
national:C natural:C nchar:C nested:U new:U next:U nfc:U
nfd:U nfkc:U nfkd:U no:U none:C normalize:C normalized:U
not:R nothing:U notify:U notnull:C nowait:U null:R nullif:C
nulls:U numeric:C object:U objects:U of:U off:U offset:R
oids:U old:U omit:U on:R only:R operator:U option:U
options:U or:R order:R ordinality:U others:U out:C outer:C
over:U overlaps:C overlay:C overriding:U owned:U owner:U parallel:U
parameter:U parser:U partial:U partition:U passing:U password:U path:U
period:U placing:R plan:U plans:U policy:U position:C preceding:U
precision:C prepare:U prepared:U preserve:U primary:R prior:U privileges:U
procedural:U procedure:U procedures:U program:U publication:U quote:U quotes:U
range:U read:U real:C reassign:U recursive:U ref:U references:R
referencing:U refresh:U reindex:U relative:U release:U rename:U repeatable:U
replace:U replica:U reset:U restart:U restrict:U return:U returning:R
returns:U revoke:U right:C role:U rollback:U rollup:U routine:U
routines:U row:C rows:U rule:U savepoint:U scalar:U schema:U
schemas:U scroll:U search:U second:U security:U select:R sequence:U
sequences:U serializable:U server:U session:U session_user:R set:U setof:C
sets:U share:U show:U similar:C simple:U skip:U smallint:C
snapshot:U some:R source:U sql:U stable:U standalone:U start:U
statement:U statistics:U stdin:U stdout:U storage:U stored:U strict:U
string:U strip:U subscription:U substring:C support:U symmetric:R sysid:U
system:U system_user:R table:R tables:U tablesample:C tablespace:U target:U
temp:U template:U temporary:U text:U then:R ties:U time:C
timestamp:C to:R trailing:R transaction:U transform:U treat:C trigger:U
trim:C true:R truncate:U trusted:U type:U types:U uescape:U
unbounded:U uncommitted:U unconditional:U unencrypted:U union:R unique:R unknown:U
unlisten:U unlogged:U until:U update:U user:R using:R vacuum:U
valid:U validate:U validator:U value:U values:C varchar:C variadic:R
varying:U verbose:C version:U view:U views:U virtual:U volatile:U
when:R where:R whitespace:U window:R with:R within:U without:U
work:U wrapper:U write:U xml:U xmlattributes:C xmlconcat:C xmlelement:C
xmlexists:C xmlforest:C xmlnamespaces:C xmlparse:C xmlpi:C xmlroot:C xmlserialize:C
xmltable:C year:U yes:U zone:U
)KEYWORDS");
    size_t keywordCount=0;
    for(std::string item;keywords>>item;) {
        const auto colon=item.find(':');
        const auto word=item.substr(0,colon);
        const bool allowed=item.substr(colon+1)!="R" && typeFunctionNames.count(word)==0;
        setting("SET r157."+word+" TO 'one'","r157."+word,{"'one'"},allowed);
        setting("SET r157.\""+word+"\" TO 'one'","r157.\""+word+"\"",{"'one'"});
        setting("SET "+word+".r157 TO 'one'",word+".r157",{"'one'"},allowed);
        ++keywordCount;
    }
    record(keywordCount==494,"all independent keywords",std::to_string(keywordCount));
    struct Case {std::string sql,name;std::vector<std::string> values;bool local;};
    const std::vector<Case> valid={
        {"SET search_path.extra TO public,pg_catalog","search_path.extra",{"public",",","pg_catalog"},false},
        {"SET compatibility_mode.extra = 'extended'","compatibility_mode.extra",{"'extended'"},false},
        {"SET r157.foo.bar TO 'one'","r157.foo.bar",{"'one'"},false},
        {"SET r157 . foo . bar = 'one'","r157.foo.bar",{"'one'"},false},
        {"SET \"r157\".\"Upper\" TO 'one'","\"r157\".\"Upper\"",{"'one'"},false},
        {"SET r157.\"a.b\" TO 'one'","r157.\"a.b\"",{"'one'"},false},
        {"SET \"r157.x\".a TO 'one'","\"r157.x\".a",{"'one'"},false},
        {"SET r157.\"a\"\"b\" TO 'one'","r157.\"a\"\"b\"",{"'one'"},false},
        {"SET r157.\"a=b\" TO 'one'","r157.\"a=b\"",{"'one'"},false},
        {"SET r157.\"select\" TO 'one'","r157.\"select\"",{"'one'"},false},
        {"SET r157.中文 TO 'one'","r157.中文",{"'one'"},false},
        {"SET 中文.r157 TO 'one'","中文.r157",{"'one'"},false},
        {"SET r157._a9 TO 'one'","r157._a9",{"'one'"},false},
        {"SET r157.a$b TO 'one'","r157.a$b",{"'one'"},false},
        {"SET r157.a TO 'a=b'","r157.a",{"'a=b'"},false},
        {"SET SESSION r157.a = 'one'","r157.a",{"'one'"},false},
        {"SET LOCAL r157.a TO 'one'","r157.a",{"'one'"},true},
        {"SET local.extra TO 'one'","local.extra",{"'one'"},false},
        {"SET session.extra TO 'one'","session.extra",{"'one'"},false},
        {"SET role.extra TO 'one'","role.extra",{"'one'"},false},
        {"SET transaction.extra TO 'one'","transaction.extra",{"'one'"},false},
        {"SET r157.a FROM CURRENT","r157.a",{},false},
        {"SET r157.a TO DEFAULT","r157.a",{"DEFAULT"},false},
        {"SET r157.a = 'one';","r157.a",{"'one'"},false},
        {"SET r157.a TO 'one';;","r157.a",{"'one'"},false},
        {"/*lead*/SET/*gap*/r157/*dot*/./*part*/a TO 'one'","r157.a",{"'one'"},false},
        {"SET SESSION session.extra TO 'one'","session.extra",{"'one'"},false},
        {"SET LOCAL local.extra TO 'one'","local.extra",{"'one'"},true}
    };
    for(const auto& test:valid)setting(test.sql,test.name,test.values,true,test.local);
    for(const std::string sql:{
        "SET r157. TO 'one'",
        "SET r157.",
        "SET r157..a TO 'one'",
        "SET r157...a TO 'one'",
        "SET r157.1 TO 'one'",
        "SET r157. -1 TO 'one'",
        "SET r157. +a TO 'one'",
        "SET r157.'a' TO 'one'",
        "SET r157.* TO 'one'",
        "SET r157.foo().a TO 'one'",
        "SET r157.a[1] TO 'one'",
        "SET r157.E'a' TO 'one'",
        "SET r157.$$a$$ TO 'one'",
        "SET r157.a 'one'",
        "SET r157.a TO",
        "SET r157.a =",
        "SET r157.a TO;",
        "SET r157.a TO 'one'; SELECT 1",
        "SET r157 . TO 'one'",
        "SET r157.a..b TO 'one'",
        "SET .r157.a TO 'one'",
        "SET 1 . r157 TO 'one'",
        "SET r157.a FROM CURRENT junk",
        "SET r157.a FROM",
        "SET r157.a;"
    }) setting(sql,"",{},false);
    // Genuine transaction statements retain their owner; dotted near-matches do not.
    for(const std::string sql:{"SET transaction.extra TO 'one'",
                               "SET transaction . extra TO 'one'"})
        record(!dbms::SQLParser::isSetTransactionStatement(sql),sql,"not a transaction mode");
    for(const std::string sql:{"SET TRANSACTION READ ONLY",
                               "/*a*/ SET /*b*/ TRANSACTION READ WRITE"})
        record(dbms::SQLParser::isSetTransactionStatement(sql),sql,"true transaction mode");
    std::cout<<"SET_TARGET_NATIVE_COMPLETE "<<checks<<" EXPECTED 3075 FAILURES "<<failures<<'\n';
    return checks==3075 && failures==0?0:1;
}
