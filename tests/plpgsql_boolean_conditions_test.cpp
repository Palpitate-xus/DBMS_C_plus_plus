#include "utils/plpgsql.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <iostream>

using namespace dbms;

static PlPgsqlQueryResult scalar(const ExprEvalResult& value) {
    PlPgsqlQueryResult result;
    result.ok = value.ok; result.sqlState = value.sqlState; result.message = value.error;
    if (value.ok) {
        result.rowCount = result.columnCount = 1; result.columnTypes = {value.typeName};
        result.firstRow = {value.isNull ? std::nullopt : std::optional<std::string>{value.value}};
    }
    return result;
}

int main() {
    TypeRegistry::instance().bootstrap();
    PlPgsqlHost host;
    host.evalExprTyped = [](const auto& text, const auto& vars, const auto& nulls, const auto& types) {
        return scalar(ExprHelper::evalStringWithNulls(text, vars, nulls, types));
    };
    host.coerceValueTyped = [](const std::optional<std::string>& value,
                               const std::string& sourceType, const std::string& targetType) {
        return scalar(ExprHelper::evalStringWithNulls("CAST(__input AS " + targetType + ")",
            {{"__input", value.value_or("")}}, value ? std::set<std::string>{} : std::set<std::string>{"__input"},
            {{"__input", sourceType.empty() ? "unknown" : sourceType}}));
    };
    struct Case { const char* name; const char* body; const char* expected; const char* state; };
    const Case cases[] = {
        {R"r280(if_true)r280", R"r280(BEGIN IF TRUE THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_true)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF TRUE THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_true)r280", R"r280(BEGIN WHILE TRUE LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_true)r280", R"r280(BEGIN LOOP EXIT WHEN TRUE; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_false)r280", R"r280(BEGIN IF FALSE THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_false)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF FALSE THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_false)r280", R"r280(BEGIN WHILE FALSE LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_false)r280", R"r280(BEGIN LOOP EXIT WHEN FALSE; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_null)r280", R"r280(BEGIN IF NULL THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_null)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF NULL THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_null)r280", R"r280(BEGIN WHILE NULL LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_null)r280", R"r280(BEGIN LOOP EXIT WHEN NULL; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_int_zero)r280", R"r280(BEGIN IF 0 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_int_zero)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 0 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_int_zero)r280", R"r280(BEGIN WHILE 0 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_int_zero)r280", R"r280(BEGIN LOOP EXIT WHEN 0; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_int_one)r280", R"r280(BEGIN IF 1 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_int_one)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 1 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_int_one)r280", R"r280(BEGIN WHILE 1 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_int_one)r280", R"r280(BEGIN LOOP EXIT WHEN 1; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_int_two)r280", R"r280(BEGIN IF 2 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_int_two)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 2 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_int_two)r280", R"r280(BEGIN WHILE 2 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_int_two)r280", R"r280(BEGIN LOOP EXIT WHEN 2; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_negative)r280", R"r280(BEGIN IF -1 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_negative)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF -1 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_negative)r280", R"r280(BEGIN WHILE -1 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_negative)r280", R"r280(BEGIN LOOP EXIT WHEN -1; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_smallint_one)r280", R"r280(BEGIN IF CAST(1 AS smallint) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_smallint_one)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(1 AS smallint) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_smallint_one)r280", R"r280(BEGIN WHILE CAST(1 AS smallint) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_smallint_one)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(1 AS smallint); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_bigint_two)r280", R"r280(BEGIN IF CAST(2 AS bigint) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_bigint_two)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(2 AS bigint) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_bigint_two)r280", R"r280(BEGIN WHILE CAST(2 AS bigint) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_bigint_two)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(2 AS bigint); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_numeric_one)r280", R"r280(BEGIN IF CAST(1 AS numeric) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_numeric_one)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(1 AS numeric) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_numeric_one)r280", R"r280(BEGIN WHILE CAST(1 AS numeric) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_numeric_one)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(1 AS numeric); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_numeric_zero)r280", R"r280(BEGIN IF CAST(0 AS numeric) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_numeric_zero)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(0 AS numeric) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_numeric_zero)r280", R"r280(BEGIN WHILE CAST(0 AS numeric) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_numeric_zero)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(0 AS numeric); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_numeric_two)r280", R"r280(BEGIN IF CAST(2 AS numeric) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_numeric_two)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(2 AS numeric) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_numeric_two)r280", R"r280(BEGIN WHILE CAST(2 AS numeric) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_numeric_two)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(2 AS numeric); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_float_one)r280", R"r280(BEGIN IF CAST(1 AS double precision) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_float_one)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(1 AS double precision) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_float_one)r280", R"r280(BEGIN WHILE CAST(1 AS double precision) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_float_one)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(1 AS double precision); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_yes)r280", R"r280(BEGIN IF 'yes' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_yes)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'yes' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_yes)r280", R"r280(BEGIN WHILE 'yes' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_yes)r280", R"r280(BEGIN LOOP EXIT WHEN 'yes'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_on)r280", R"r280(BEGIN IF 'on' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_on)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'on' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_on)r280", R"r280(BEGIN WHILE 'on' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_on)r280", R"r280(BEGIN LOOP EXIT WHEN 'on'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_off_prefix)r280", R"r280(BEGIN IF 'of' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_off_prefix)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'of' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_off_prefix)r280", R"r280(BEGIN WHILE 'of' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_off_prefix)r280", R"r280(BEGIN LOOP EXIT WHEN 'of'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_ambiguous)r280", R"r280(BEGIN IF 'o' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_ambiguous)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'o' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_ambiguous)r280", R"r280(BEGIN WHILE 'o' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_ambiguous)r280", R"r280(BEGIN LOOP EXIT WHEN 'o'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_text_true)r280", R"r280(BEGIN IF 'true' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_text_true)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'true' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_text_true)r280", R"r280(BEGIN WHILE 'true' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_text_true)r280", R"r280(BEGIN LOOP EXIT WHEN 'true'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_text_one)r280", R"r280(BEGIN IF '1' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_text_one)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF '1' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_text_one)r280", R"r280(BEGIN WHILE '1' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_text_one)r280", R"r280(BEGIN LOOP EXIT WHEN '1'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_text_false)r280", R"r280(BEGIN IF 'false' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_text_false)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'false' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_text_false)r280", R"r280(BEGIN WHILE 'false' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_text_false)r280", R"r280(BEGIN LOOP EXIT WHEN 'false'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_text_zero)r280", R"r280(BEGIN IF '0' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_text_zero)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF '0' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_text_zero)r280", R"r280(BEGIN WHILE '0' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_text_zero)r280", R"r280(BEGIN LOOP EXIT WHEN '0'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_empty)r280", R"r280(BEGIN IF '' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_empty)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF '' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_empty)r280", R"r280(BEGIN WHILE '' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_empty)r280", R"r280(BEGIN LOOP EXIT WHEN ''; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_text_null)r280", R"r280(BEGIN IF 'null' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_text_null)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'null' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_text_null)r280", R"r280(BEGIN WHILE 'null' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_text_null)r280", R"r280(BEGIN LOOP EXIT WHEN 'null'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_invalid)r280", R"r280(BEGIN IF 'invalid' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_invalid)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'invalid' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_invalid)r280", R"r280(BEGIN WHILE 'invalid' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_invalid)r280", R"r280(BEGIN LOOP EXIT WHEN 'invalid'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_typed_text)r280", R"r280(BEGIN IF CAST('t' AS TEXT) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_typed_text)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST('t' AS TEXT) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_typed_text)r280", R"r280(BEGIN WHILE CAST('t' AS TEXT) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_typed_text)r280", R"r280(BEGIN LOOP EXIT WHEN CAST('t' AS TEXT); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_null_int)r280", R"r280(BEGIN IF CAST(NULL AS INT) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_null_int)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(NULL AS INT) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_null_int)r280", R"r280(BEGIN WHILE CAST(NULL AS INT) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_null_int)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(NULL AS INT); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_null_date)r280", R"r280(BEGIN IF CAST(NULL AS DATE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_null_date)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST(NULL AS DATE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_null_date)r280", R"r280(BEGIN WHILE CAST(NULL AS DATE) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_null_date)r280", R"r280(BEGIN LOOP EXIT WHEN CAST(NULL AS DATE); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_date)r280", R"r280(BEGIN IF CAST('2020-01-01' AS DATE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_date)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST('2020-01-01' AS DATE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_date)r280", R"r280(BEGIN WHILE CAST('2020-01-01' AS DATE) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_date)r280", R"r280(BEGIN LOOP EXIT WHEN CAST('2020-01-01' AS DATE); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_division_error)r280", R"r280(BEGIN IF (1/0) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22012)r280"},
        {R"r280(elsif_division_error)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF (1/0) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22012)r280"},
        {R"r280(while_division_error)r280", R"r280(BEGIN WHILE (1/0) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22012)r280"},
        {R"r280(exit_division_error)r280", R"r280(BEGIN LOOP EXIT WHEN (1/0); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22012)r280"},
        {R"r280(if_cast_error)r280", R"r280(BEGIN IF CAST('bad' AS INT) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_cast_error)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF CAST('bad' AS INT) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_cast_error)r280", R"r280(BEGIN WHILE CAST('bad' AS INT) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_cast_error)r280", R"r280(BEGIN LOOP EXIT WHEN CAST('bad' AS INT); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_uppercase_prefix)r280", R"r280(BEGIN IF 'Y' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_uppercase_prefix)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'Y' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_uppercase_prefix)r280", R"r280(BEGIN WHILE 'Y' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_uppercase_prefix)r280", R"r280(BEGIN LOOP EXIT WHEN 'Y'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_mixed_case)r280", R"r280(BEGIN IF 'YeS' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_mixed_case)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'YeS' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_mixed_case)r280", R"r280(BEGIN WHILE 'YeS' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_mixed_case)r280", R"r280(BEGIN LOOP EXIT WHEN 'YeS'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_true_prefix)r280", R"r280(BEGIN IF 'TR' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_true_prefix)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'TR' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_true_prefix)r280", R"r280(BEGIN WHILE 'TR' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_true_prefix)r280", R"r280(BEGIN LOOP EXIT WHEN 'TR'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_whitespace)r280", R"r280(BEGIN IF ' FALSE ' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_whitespace)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF ' FALSE ' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_whitespace)r280", R"r280(BEGIN WHILE ' FALSE ' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_whitespace)r280", R"r280(BEGIN LOOP EXIT WHEN ' FALSE '; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_off)r280", R"r280(BEGIN IF 'off' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_off)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'off' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_off)r280", R"r280(BEGIN WHILE 'off' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_off)r280", R"r280(BEGIN LOOP EXIT WHEN 'off'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_no)r280", R"r280(BEGIN IF 'no' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_no)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'no' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_no)r280", R"r280(BEGIN WHILE 'no' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_no)r280", R"r280(BEGIN LOOP EXIT WHEN 'no'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_mixed_on)r280", R"r280(BEGIN IF 'oN' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_mixed_on)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'oN' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_mixed_on)r280", R"r280(BEGIN WHILE 'oN' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_mixed_on)r280", R"r280(BEGIN LOOP EXIT WHEN 'oN'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_false_suffix)r280", R"r280(BEGIN IF 'falsely' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_false_suffix)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF 'falsely' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_false_suffix)r280", R"r280(BEGIN WHILE 'falsely' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_false_suffix)r280", R"r280(BEGIN LOOP EXIT WHEN 'falsely'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_variable_yes)r280", R"r280(DECLARE x TEXT:='yes'; BEGIN IF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_variable_yes)r280", R"r280(DECLARE x TEXT:='yes'; BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_variable_yes)r280", R"r280(DECLARE x TEXT:='yes'; BEGIN WHILE x LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_variable_yes)r280", R"r280(DECLARE x TEXT:='yes'; BEGIN LOOP EXIT WHEN x; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_variable_text_null)r280", R"r280(DECLARE x TEXT:='null'; BEGIN IF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_variable_text_null)r280", R"r280(DECLARE x TEXT:='null'; BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_variable_text_null)r280", R"r280(DECLARE x TEXT:='null'; BEGIN WHILE x LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_variable_text_null)r280", R"r280(DECLARE x TEXT:='null'; BEGIN LOOP EXIT WHEN x; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_variable_null)r280", R"r280(DECLARE x TEXT:=NULL; BEGIN IF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_variable_null)r280", R"r280(DECLARE x TEXT:=NULL; BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_variable_null)r280", R"r280(DECLARE x TEXT:=NULL; BEGIN WHILE x LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_variable_null)r280", R"r280(DECLARE x TEXT:=NULL; BEGIN LOOP EXIT WHEN x; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_variable_one)r280", R"r280(DECLARE x INT:=1; BEGIN IF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_variable_one)r280", R"r280(DECLARE x INT:=1; BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_variable_one)r280", R"r280(DECLARE x INT:=1; BEGIN WHILE x LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_variable_one)r280", R"r280(DECLARE x INT:=1; BEGIN LOOP EXIT WHEN x; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_variable_two)r280", R"r280(DECLARE x INT:=2; BEGIN IF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(elsif_variable_two)r280", R"r280(DECLARE x INT:=2; BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(while_variable_two)r280", R"r280(DECLARE x INT:=2; BEGIN WHILE x LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(exit_variable_two)r280", R"r280(DECLARE x INT:=2; BEGIN LOOP EXIT WHEN x; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280()r280", R"r280(22P02)r280"},
        {R"r280(if_variable_bool_null)r280", R"r280(DECLARE x BOOLEAN:=NULL; BEGIN IF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_variable_bool_null)r280", R"r280(DECLARE x BOOLEAN:=NULL; BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF x THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_variable_bool_null)r280", R"r280(DECLARE x BOOLEAN:=NULL; BEGIN WHILE x LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_variable_bool_null)r280", R"r280(DECLARE x BOOLEAN:=NULL; BEGIN LOOP EXIT WHEN x; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_found_false)r280", R"r280(BEGIN IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(elsif_found_false)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(while_found_false)r280", R"r280(BEGIN WHILE FOUND LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(exit_found_false)r280", R"r280(BEGIN LOOP EXIT WHEN FOUND; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(no)r280", R"r280()r280"},
        {R"r280(if_found_true)r280", R"r280(BEGIN IF NOT FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_found_true)r280", R"r280(BEGIN IF FALSE THEN RETURN 'wrong'; ELSIF NOT FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_found_true)r280", R"r280(BEGIN WHILE NOT FOUND LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(exit_found_true)r280", R"r280(BEGIN LOOP EXIT WHEN NOT FOUND; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(if_skip_elsif_error)r280", R"r280(BEGIN IF 'yes' THEN RETURN 'yes'; ELSIF (1/0) THEN RETURN 'bad'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(elsif_skip_later_error)r280", R"r280(BEGIN IF NULL THEN RETURN 'bad'; ELSIF 'on' THEN RETURN 'yes'; ELSIF (1/0) THEN RETURN 'bad'; ELSE RETURN 'no'; END IF; END;)r280", R"r280(yes)r280", R"r280()r280"},
        {R"r280(while_recheck)r280", R"r280(DECLARE i INT:=0; BEGIN WHILE i<3 LOOP i:=i+1; END LOOP; RETURN i; END;)r280", R"r280(3)r280", R"r280()r280"},
        {R"r280(exit_recheck)r280", R"r280(DECLARE i INT:=0; BEGIN LOOP i:=i+1; EXIT WHEN i=3; END LOOP; RETURN i; END;)r280", R"r280(3)r280", R"r280()r280"},
    };
    size_t controls=0,failures=0;
    for (const bool hostless : {false,true}) for (const auto& test : cases) {
        std::string value,error,state; bool isNull=false; bool ok=false;
        try { ok=PlPgsql::run(test.body,{},hostless ? PlPgsqlHost{} : host,value,error,nullptr,&isNull,nullptr,&state); }
        catch (const std::exception& exception) { error=std::string("uncaught: ")+exception.what(); }
        const bool expectedOk=!*test.state;
        ++controls;
        if (ok!=expectedOk || state!=test.state) {
            ++failures; std::cerr << "BOOLEAN_CONDITION_NATIVE_FAIL " << test.name << " hostless=" << hostless
                << " ok=" << ok << " state=" << state << " error=" << error << '\n';
        }
        ++controls;
        if (expectedOk && (!ok || isNull || value!=test.expected)) {
            ++failures; std::cerr << "BOOLEAN_CONDITION_NATIVE_VALUE_FAIL " << test.name << " hostless=" << hostless
                << " value=" << value << " isNull=" << isNull << " error=" << error << '\n';
        }
    }
    std::cout << "PLPGSQL_BOOLEAN_CONDITIONS_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    size_t apiControls = 0, apiFailures = 0;
    const auto check = [&](const std::string& label, const std::function<bool()>& action) {
        ++apiControls;
        try { if (action()) return; }
        catch (const std::exception& exception) { std::cerr << exception.what() << '\n'; }
        ++apiFailures; std::cerr << "BOOLEAN_CONDITION_API_FAIL " << label << '\n';
    };
    const auto program = [](const std::string& site) {
        if (site == "if") return std::string("BEGIN IF probe() THEN PERFORM then_effect(); ELSE PERFORM else_effect(); END IF; PERFORM after_effect(); RETURN 'done'; END;");
        if (site == "elsif") return std::string("BEGIN IF FALSE THEN PERFORM unreachable(); ELSIF probe() THEN PERFORM then_effect(); ELSE PERFORM else_effect(); END IF; PERFORM after_effect(); RETURN 'done'; END;");
        if (site == "while") return std::string("BEGIN WHILE probe() LOOP PERFORM body_effect(); EXIT; END LOOP; PERFORM after_effect(); RETURN 'done'; END;");
        return std::string("BEGIN LOOP EXIT WHEN probe(); PERFORM body_effect(); EXIT; END LOOP; PERFORM after_effect(); RETURN 'done'; END;");
    };
    for (const std::string site : {"if", "elsif", "while", "exit"}) {
        for (int mode = 0; mode < 6; ++mode) {
            check(site + "/metadata/" + std::to_string(mode), [&] {
                auto probe = host; size_t calls = 0, effects = 0;
                probe.evalExprTyped = [&](const auto&, const auto&, const auto&, const auto&) {
                    ++calls; PlPgsqlQueryResult value;
                    if (mode == 0) { value.sqlState="22012"; value.message="probe failure"; return value; }
                    value.ok=true; value.rowCount=value.columnCount=1; value.columnTypes={"text"};
                    value.firstRow={mode == 1 ? std::optional<std::string>{"invalid"} : mode == 2 ?
                        std::optional<std::string>{} : mode == 3 ? std::optional<std::string>{"yes"} : std::optional<std::string>{"no"}};
                    if (mode == 5) value.rowCount=0;
                    return value;
                };
                probe.execStmt=[&](const auto&, const auto&) { ++effects; return true; };
                // Explicit SQL integer casts are not PL/pgSQL boolean coercion.
                // No condition is to be reevaluated through this callback.
                probe.coerceValueTyped=[](const auto&, const auto&, const auto&) {
                    PlPgsqlQueryResult result; result.sqlState="XX001"; return result;
                };
                std::string value,error,state;
                const bool ok=PlPgsql::run(program(site),{},probe,value,error,nullptr,nullptr,nullptr,&state);
                if (mode == 0 || mode == 1 || mode == 5)
                    return !ok && state==(mode == 0 ? "22012" : mode == 1 ? "22P02" : "XX000") && effects==0 && calls==1;
                const size_t expectedEffects=(site == "if" || site == "elsif") ? 2 :
                    site == "while" ? (mode == 3 ? 2 : 1) : (mode == 3 ? 1 : 2);
                return ok && state.empty() && value=="done" && effects==expectedEffects && calls==1;
            });
        }
        for (const std::string input : {"yes", "of", "invalid", "error"}) {
            check(site + "/legacy/" + input,[&] {
                PlPgsqlHost probe; size_t calls=0,effects=0;
                probe.evalExpr=[&](const auto&, const auto&) -> std::optional<std::string> {
                    ++calls; if (input=="error") return std::nullopt; return input;
                };
                probe.execStmt=[&](const auto&, const auto&) { ++effects; return true; };
                std::string value,error,state;
                const bool ok=PlPgsql::run(program(site),{},probe,value,error,nullptr,nullptr,nullptr,&state);
                if (input=="invalid" || input=="error")
                    return !ok && effects==0 && calls==1 && state==(input=="invalid" ? "22P02" : "XX000");
                const size_t expectedEffects=(site=="if" || site=="elsif") ? 2 :
                    site=="while" ? (input=="yes" ? 2 : 1) : (input=="yes" ? 1 : 2);
                return ok && state.empty() && effects==expectedEffects && calls==1;
            });
        }
    }
    check("WHILE evaluates once per test including final false",[&] {
        auto probe=host; size_t calls=0,effects=0;
        probe.evalExprTyped=[&](const auto&, const auto&, const auto&, const auto&) {
            PlPgsqlQueryResult result; result.ok=true; result.rowCount=result.columnCount=1;
            result.columnTypes={"text"}; result.firstRow={++calls<=2 ? "yes" : "no"}; return result;
        };
        probe.execStmt=[&](const auto&, const auto&) { ++effects; return true; };
        std::string value,error,state;
        const bool ok=PlPgsql::run("BEGIN WHILE probe() LOOP PERFORM effect(); END LOOP; RETURN 'done'; END;",{},probe,value,error,nullptr,nullptr,nullptr,&state);
        return ok && calls==3 && effects==2 && state.empty();
    });
    check("ELSIF remains unevaluated after matching IF",[&] {
        auto probe=host; size_t calls=0;
        probe.evalExprTyped=[&](const auto&, const auto&, const auto&, const auto&) {
            ++calls; PlPgsqlQueryResult result; result.sqlState="22012"; return result;
        };
        std::string value,error,state;
        const bool ok=PlPgsql::run("BEGIN IF TRUE THEN RETURN 'yes'; ELSIF probe() THEN RETURN 'no'; END IF; END;",{},probe,value,error,nullptr,nullptr,nullptr,&state);
        return ok && value=="yes" && calls==0 && state.empty();
    });
    // Integer output canonicalizes 0001/-0, whereas numeric 1.0 retains
    // its scale. Interpret the evaluated SQL value, not the source spelling.
    for (int mode=0; mode<3; ++mode) {
        auto embedding=mode==1 ? PlPgsqlHost{} : host;
        if (mode==2) {
            embedding=PlPgsqlHost{};
            embedding.evalExpr=[](const auto& text,const auto& vars) -> std::optional<std::string> {
                const auto result=ExprHelper::evalStringWithNulls(text,vars,{});
                if (!result.ok) return std::nullopt;
                return result.isNull ? "null" : result.value;
            };
        }
        check("if_0001", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF 0001 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("elsif_0001", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF 0001 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("while_0001", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN WHILE 0001 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("exit_0001", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN LOOP EXIT WHEN 0001; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("if_-0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF -0 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="no";
        });
        check("elsif_-0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF -0 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="no";
        });
        check("while_-0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN WHILE -0 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="no";
        });
        check("exit_-0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN LOOP EXIT WHEN -0; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="no";
        });
        check("if_1.0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF 1.0 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("elsif_1.0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF 1.0 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("while_1.0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN WHILE 1.0 LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("exit_1.0", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN LOOP EXIT WHEN 1.0; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("if_'0001'", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF '0001' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("elsif_'0001'", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF '0001' THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("while_'0001'", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN WHILE '0001' LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("exit_'0001'", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN LOOP EXIT WHEN '0001'; RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return !ok && state=="22P02";
        });
        check("if_CAST(2 AS BOOLEAN)", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF CAST(2 AS BOOLEAN) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("elsif_CAST(2 AS BOOLEAN)", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF CAST(2 AS BOOLEAN) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("while_CAST(2 AS BOOLEAN)", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN WHILE CAST(2 AS BOOLEAN) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
        check("exit_CAST(2 AS BOOLEAN)", [&] {
            std::string value,error,state;
            const bool ok=PlPgsql::run(R"r280(BEGIN LOOP EXIT WHEN CAST(2 AS BOOLEAN); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r280",{},embedding,value,error,nullptr,nullptr,nullptr,&state);
            return ok && state.empty() && value=="yes";
        });
    }
    std::cout << "PLPGSQL_BOOLEAN_CONDITIONS_API_COMPLETE " << apiControls << " FAILURES " << apiFailures << '\n';
    return failures || apiFailures ? 1 : 0;
}
