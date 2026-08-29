-- LIKE pattern semantics: literal % in text, ESCAPE clause
SELECT 'ab%c' LIKE 'ab%'
SELECT 'a!%c' LIKE 'a!%'
SELECT 'a!%c' LIKE 'a!%' ESCAPE '!'
SELECT 'ax_c' LIKE 'a__c'
SELECT 'ax_c' LIKE 'a!__c' ESCAPE '!'
SELECT '100%' LIKE '100!%' ESCAPE '!'
SELECT 'abc' LIKE 'a%'
SELECT 'abc' NOT LIKE 'x%'
