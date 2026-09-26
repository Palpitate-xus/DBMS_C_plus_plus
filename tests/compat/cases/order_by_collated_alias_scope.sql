-- An output alias is available as a bare ORDER BY name, not as a COLLATE expression operand.
CREATE TEMP TABLE diff_collated_alias_scope(v text);
INSERT INTO diff_collated_alias_scope VALUES ('apple'), ('Zoo');
SELECT left(v, 5) AS value FROM diff_collated_alias_scope ORDER BY value COLLATE "C";
DROP TABLE diff_collated_alias_scope;
