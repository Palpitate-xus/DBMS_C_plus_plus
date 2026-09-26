-- Text concatenation carries the explicit collation of its operands.
SELECT ('app' COLLATE "C" || 'le') < 'Zoo';
SELECT ('app' || 'le' COLLATE "C") < 'Zoo';
SELECT ('app' COLLATE "default" || 'le') < 'Zoo';
SELECT ('app' COLLATE "C" || 'le' COLLATE "default") < 'Zoo';
SELECT 'a' COLLATE "C" || 'b' COLLATE "default";
SELECT NULL::text COLLATE "C" || 'b' COLLATE "default";
