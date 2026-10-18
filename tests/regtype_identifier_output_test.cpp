#include "expression/ExprEvaluator.h"
#include "expression/regtype_input.h"
#include "catalog/type_registry.h"
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {
size_t checks=0,failures=0;
void require(bool pass,const std::string& label,const std::string& detail="") {
    ++checks;
    if (!pass) {++failures;std::cerr<<"REGTYPE_IDENTIFIER_FAIL "<<checks<<' '<<label<<' '<<detail<<'\n';}
}
std::string quoted(const std::string& text) {
    std::string out="\"";
    for(char ch:text) {if(ch=='"')out+='"';out+=ch;}
    return out+'"';
}
dbms::ExprValue call(const std::string& function,const std::vector<dbms::ExprValue>& cells) {
    dbms::FunctionCallExpr expression;expression.funcName=function;
    for(size_t slot=0;slot<cells.size();++slot) {
        auto parameter=std::make_unique<dbms::ParameterExpr>();
        parameter->slot=slot;parameter->declaredType=cells[slot].typeName;
        expression.args.push_back(std::move(parameter));
    }
    dbms::RowContext row;row.setParameters(cells);
    return dbms::ExprEvaluator().eval(&expression,row);
}
void quoteFunctions(const std::string& name,const std::string& expected) {
    for(const std::string function:{"quote_ident","format"}) {
        try {
            auto cells=std::vector<dbms::ExprValue>{{"text",name}};
            if(function=="format")cells.insert(cells.begin(),{"text","%I"});
            const auto value=call(function,cells);
            require(!value.isNull && value.typeName=="text" && value.value==expected,
                    function+" "+quoted(name),value.value);
        } catch(const std::exception& error) {require(false,function+" "+quoted(name),error.what());}
    }
}
// These are ordinary copied metadata input rows, not server stubs, DDL,
// array auto-creation, a disk fixture or a replacement for catalog tests.
void typeOutput(const std::string& name,const std::string& expectedName,
                const std::string& schema="review_schema",const std::string& expectedSchema="review_schema") {
    auto catalog=dbms::CatalogManager::MetadataSnapshot{};
    catalog.types=dbms::CatalogManager::builtinTypeRows();
    catalog.namespaces=dbms::CatalogManager::builtinNamespaceRows();
    catalog.namespaces.push_back({20000,schema,10});
    dbms::PgTypeRow scalar;
    scalar.oid=30000;scalar.typname=name;scalar.typnamespace=20000;scalar.typowner=10;
    scalar.typtype='e';scalar.typcategory='E';scalar.typlen=4;scalar.typbyval=true;
    scalar.typarray=30001;
    auto array=dbms::PgTypeRow{};
    array.oid=30001;array.typname="_identifier_array";array.typnamespace=20000;
    array.typelem=30000;array.typcategory='A';array.typstorage='x';
    catalog.types.push_back(scalar);catalog.types.push_back(array);
    for(const bool visible:{false,true}) {
        Session session;
        session.searchPath=visible?quoted(schema)+", pg_catalog":"public";
        for(const dbms::Oid oid:{30000U,30001U}) {
            const auto expected=(visible?"":expectedSchema+".")+expectedName+(oid==30001?"[]":"");
            std::string result;
            try {result=dbms::regtype_detail::output(oid,catalog,&session);
                 require(result==expected,"display "+quoted(name)+(visible?" visible":" qualified"),result);}
            catch(const std::exception& error) {require(false,"display "+quoted(name),error.what());}
            try {require(dbms::regtype_detail::inputOid(result,catalog,&session)==oid,
                         "rendered name retains exact identity "+quoted(name));}
            catch(const std::exception& error) {require(false,"roundtrip "+quoted(name),error.what());}
        }
    }
}
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    // Independent PG 18.6 kwlist.h golden facts, also exercised by the wire
    // fixture against the actual reference. Never derive expected categories
    // from the production parser/quote function under test.
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
        const auto name=item.substr(0,colon);
        const auto expected=item.substr(colon+1)=="U"?name:quoted(name);
        quoteFunctions(name,expected);
        typeOutput(name,expected);
        ++keywordCount;
    }
    const std::vector<std::pair<std::string,std::string>> special={
        {"","\"\""},{"plain","plain"},{"_","_"},{"_a9","_a9"},
        {"Upper","\"Upper\""},{"A","\"A\""},{"1a","\"1a\""},
        {"a$b","\"a$b\""},{"a.b","\"a.b\""},{"a b","\"a b\""},
        {"a,b","\"a,b\""},{"a\"b","\"a\"\"b\""},{"a'b","\"a'b\""},
        {"line\nbreak","\"line\nbreak\""},{"tab\tname","\"tab\tname\""},
        {"汉","\"汉\""},{"é","\"é\""},{"rank[]","\"rank[]\""},
        {"a\\b","\"a\\b\""},{"a;z","\"a;z\""},{"a\n","\"a\n\""},
        {std::string(63,'a'),std::string(63,'a')}
    };
    for(const auto& [name,expected]:special) {
        quoteFunctions(name,expected);
        if(!name.empty())typeOutput(name,expected);
    }
    const std::vector<std::pair<std::string,std::string>> schemas={
        {"Review Space","\"Review Space\""},{"select","\"select\""},
        {"schema\"q","\"schema\"\"q\""},{"汉","\"汉\""},{"a.b","\"a.b\""}
    };
    for(const auto& [schema,expected]:schemas)typeOutput("plain","plain",schema,expected);
    try {require(call("quote_ident",{{"text","",true}}).isNull,"quote_ident NULL stays NULL");}
    catch(const std::exception& error) {require(false,"quote_ident NULL",error.what());}
    try {(void)call("format",{{"text","%I"},{"text","",true}});require(false,"format NULL identifier rejected");}
    catch(const dbms::DbError& error) {require(error.sqlState()=="22004","format NULL SQLSTATE",error.sqlState());}
    // The native format callback's existing legacy API uses runtime_error;
    // the wire fixture separately checks the actual protocol SQLSTATE.
    catch(const std::runtime_error& error) {
        require(std::string(error.what()).find("SQLSTATE 22004")!=std::string::npos,
                "format NULL legacy SQLSTATE",error.what());
    }
    catch(const std::exception& error) {require(false,"format NULL identifier",error.what());}
    dbms::CatalogManager::MetadataSnapshot empty;
    require(dbms::regtype_detail::output(0,empty,nullptr)=="-","invalid OID rendering unchanged");
    require(dbms::regtype_detail::output(4294967295U,empty,nullptr)=="4294967295","unknown OID rendering unchanged");
    require(keywordCount==494,"all494 golden categories retained");
    const size_t expected=494*10+special.size()*2+(special.size()-1)*8+schemas.size()*8+5;
    std::cout<<"REGTYPE_IDENTIFIER_NATIVE_COMPLETE "<<checks<<" EXPECTED "<<expected
             <<" FAILURES "<<failures<<'\n';
    return checks==expected && failures==0?0:1;
}
