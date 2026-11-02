#!/usr/bin/env python3
"""A heap scan retains its command snapshot across writing sort functions."""
from compat.numeric_type_review import Review


def main():
    review = Review("TABLE_SCAN_EFFECT_SNAPSHOT")
    payload = "x" * 1024
    try:
        review.query("CREATE TABLE scan_effect_rows(id INTEGER,v INTEGER,payload VARCHAR(1024))")
        review.query("INSERT INTO scan_effect_rows VALUES " + ",".join(
            f"({i},{i},'{payload}')" for i in range(24)))
        review.query("CREATE FUNCTION scan_effect_key(arg INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$ "
                     "BEGIN IF arg=0 THEN UPDATE scan_effect_rows SET v=999 WHERE id=23; END IF; RETURN arg; END; $$")
        wanted = [[str(i), str(i)] for i in range(24)]
        review.query("SELECT id,v FROM scan_effect_rows ORDER BY scan_effect_key(id),id LIMIT 24",
                     wanted, [23, 23])
        review.query("SELECT v FROM scan_effect_rows WHERE id=23", [["999"]], [23])
        review.query("UPDATE scan_effect_rows SET v=23 WHERE id=23")
        review.query("BEGIN")
        review.query("SELECT id,v FROM scan_effect_rows ORDER BY scan_effect_key(id),id LIMIT 24",
                     wanted, [23, 23])
        review.query("SELECT v FROM scan_effect_rows WHERE id=23", [["999"]], [23])
        review.query("ROLLBACK")
        review.query("SELECT v FROM scan_effect_rows WHERE id=23", [["23"]], [23])
        review.query("SELECT id,payload FROM scan_effect_rows ORDER BY id LIMIT 2",
                     [["0", payload], ["1", payload]], [23, 1043])
        review.parameter("SELECT id,v FROM scan_effect_rows WHERE id >= $1 ORDER BY id LIMIT 4 OFFSET 19",
                         ["0"], [23], [[str(i), str(i)] for i in range(19, 23)], [23, 23])
        review.complete(13)
    finally:
        review.close()


if __name__ == "__main__":
    main()
