-- SIMILAR TO pattern semantics (alternation, escape, negation)
SELECT 'abc' SIMILAR TO 'a%'
SELECT 'abc' SIMILAR TO '(a|b)%'
SELECT 'abc' SIMILAR TO '%(b|d)%'
SELECT 'abc' SIMILAR TO 'abc'
SELECT 'abc' SIMILAR TO 'x%'
SELECT 'a!%c' SIMILAR TO 'a!%'
SELECT 'a!%c' SIMILAR TO 'a!%' ESCAPE '!'
SELECT 'abc' NOT SIMILAR TO 'x%'
SELECT not ('abc' SIMILAR TO 'x%')
