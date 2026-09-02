DROP TABLE IF EXISTS diff_ufn;
CREATE TABLE diff_ufn (id INT, name VARCHAR(10));
INSERT INTO diff_ufn VALUES (1, 'abc');
SELECT id FROM diff_ufn WHERE upper(name) = 'ABC';
SELECT id FROM diff_ufn WHERE length(name) > 0;
SELECT id FROM diff_ufn WHERE nosuchfn(id) > 0;
SELECT id FROM diff_ufn WHERE nosuchstrfn(name) > 0;
