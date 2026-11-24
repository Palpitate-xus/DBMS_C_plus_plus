#!/usr/bin/env python3
"""Exercise persistent unsigned OID columns, indexes and wire types."""

import struct

from compat.numeric_type_review import Review


def main():
    review = Review("OID_COLUMN_STORAGE")
    client, sock = review.client, review.sock
    packet = client.typed

    def command(sql, tag):
        frames = [item for item in client.simple_query(sock, sql) if item[0] != b"N"]
        expected = [(b"C", tag.encode() + b"\0"), (b"Z", b"I")]
        review.record(sql, frames == expected, frames, expected)

    def copy_in(payload, tag, state=None):
        sql = "COPY stored_oid (id,k,note) FROM STDIN"
        sock.sendall(packet(b"Q", sql.encode() + b"\0"))
        header = client.read_message(sock)
        expected = (b"G", b"\0" + struct.pack("!HHHH", 3, 0, 0, 0))
        review.record("COPY text header", header == expected, header, expected)
        if header[0] != b"G":
            client.read_until_ready(sock)
            review.record("COPY completion", False, None, tag or state)
            return
        sock.sendall(packet(b"d", payload) + packet(b"c"))
        frames = client.read_until_ready(sock)
        if state:
            valid = (b"".join(kind for kind, _ in frames) == b"EZ"
                     and client.diagnostic_fields(frames[0][1])[b"C"].decode() == state)
        else:
            valid = frames == [(b"C", tag.encode() + b"\0"), (b"Z", b"I")]
        review.record("COPY completion", valid, frames, tag or state)

    try:
        command("CREATE TABLE stored_oid(id integer PRIMARY KEY,k oid DEFAULT '0',note text)", "CREATE TABLE")
        review.query("SELECT id FROM stored_oid WHERE k=''", state="22P02")
        review.query("SELECT id FROM stored_oid WHERE k='bad'", state="22P02")
        command("INSERT INTO stored_oid VALUES (1,DEFAULT,'zero'),(2,NULL,'missing'),"
                "(3,2147483648,'high'),(4,4294967295,'max'),(5,' 0x10 ','hex'),(6,2147483647,'signed-max')", "INSERT 0 6")
        order = [["1", "0"], ["5", "16"], ["6", "2147483647"],
                 ["3", "2147483648"], ["4", "4294967295"], ["2", None]]
        review.query("SELECT id,k FROM stored_oid ORDER BY k NULLS LAST", rows=order, oids=[23, 26])
        review.query("SELECT id FROM stored_oid WHERE k='-1'", rows=[["4"]], oids=[23])
        review.query("SELECT id FROM stored_oid WHERE k=2147483648::oid", rows=[["3"]], oids=[23])
        review.query("SELECT id FROM stored_oid WHERE k>2147483647::oid ORDER BY k", rows=[["3"], ["4"]], oids=[23])
        review.query("SELECT id FROM stored_oid WHERE k=0", rows=[["1"]], oids=[23])
        review.query("SELECT id FROM stored_oid WHERE k IS NULL", rows=[["2"]], oids=[23])
        review.query("SELECT id FROM stored_oid WHERE k='bad'", state="22P02")
        review.query("INSERT INTO stored_oid VALUES (9,-1::bigint,'negative')", state="22003")
        review.query("INSERT INTO stored_oid VALUES (9,4294967296,'overflow')", state="22003")
        review.query("INSERT INTO stored_oid VALUES (9,'','bad')", state="22P02")
        review.query("INSERT INTO stored_oid VALUES (9,'10'::text,'typed')", state="42804")
        review.query("INSERT INTO stored_oid VALUES (9,2.5,'decimal')", state="42804")
        review.query("INSERT INTO stored_oid VALUES (9,true,'boolean')", state="42804")
        review.query("INSERT INTO stored_oid SELECT 9,'14'::text,'typed'", state="42804")
        review.query("SELECT id FROM stored_oid WHERE k='0xffffffff'::text", state="42883")
        command("CREATE INDEX stored_oid_order ON stored_oid(k)", "CREATE INDEX")
        review.query("SELECT id,k FROM stored_oid WHERE k='0xffffffff'", rows=[["4", "4294967295"]], oids=[23, 26])
        review.query("UPDATE stored_oid SET k='010' WHERE id=5 RETURNING k", rows=[["8"]], oids=[26], tag="UPDATE 1")
        review.query("UPDATE stored_oid SET k=CAST(-1 AS bigint) WHERE id=3", state="22003")
        review.query("UPDATE stored_oid SET k=4294967296 WHERE id=3", state="22003")
        review.query("UPDATE stored_oid SET k='10'::text WHERE id=3", state="42804")
        review.query("SELECT k FROM stored_oid WHERE id=3", rows=[["2147483648"]], oids=[26])
        copy_in(b"7\t+9\tcopy\n8\t\\N\tnull\n", "COPY 2")
        copy_in(b"9\t42\troll back\n10\t4294967296\toverflow\n", None, "22003")
        review.query("SELECT id,k FROM stored_oid WHERE id>=7 ORDER BY id", rows=[["7", "9"], ["8", None]], oids=[23, 26])
        review.query("DELETE FROM stored_oid WHERE k='010' RETURNING id,k", rows=[["5", "8"]], oids=[23, 26], tag="DELETE 1")
        command("REINDEX TABLE stored_oid", "REINDEX")
        review.query("SELECT id,k FROM stored_oid WHERE k='2147483648'", rows=[["3", "2147483648"]], oids=[23, 26])

        command("CREATE TABLE oid_primary(k pg_catalog.oid PRIMARY KEY, memo text)", "CREATE TABLE")
        command("INSERT INTO oid_primary VALUES (3,'low'),(20,'middle'),(4294967295,'high'),(0,'zero')", "INSERT 0 4")
        review.query("INSERT INTO oid_primary VALUES ('-1','duplicate')", state="23505")
        review.query("SELECT k FROM oid_primary ORDER BY k", rows=[["0"], ["3"], ["20"], ["4294967295"]], oids=[26])
        review.query("SELECT memo FROM oid_primary WHERE k='0xffffffff'", rows=[["high"]], oids=[25])
        command("ALTER TABLE oid_primary ADD COLUMN parent_oid oid", "ALTER TABLE")
        command("UPDATE oid_primary SET parent_oid=k", "UPDATE 4")
        review.query("SELECT parent_oid FROM oid_primary WHERE k='-1'", rows=[["4294967295"]], oids=[26])
        review.query("SELECT a.k FROM oid_primary a JOIN oid_primary b ON a.k=b.parent_oid ORDER BY a.k",
                     rows=[["0"], ["3"], ["20"], ["4294967295"]], oids=[26])

        metadata = client.simple_query(sock, "SELECT oid FROM pg_catalog.pg_class WHERE relname='stored_oid'")
        rows = client.data_row_values(metadata)
        relation = int(rows[0][0]) if len(rows) == 1 else 0
        review.query("SELECT atttypid,attlen,atttypmod,attbyval FROM pg_catalog.pg_attribute "
                     f"WHERE attrelid={relation}::oid AND attname='k'",
                     rows=[["26", "4", "-1", "t"]], oids=[26, 21, 23, 16])
        sql = "SELECT k FROM stored_oid WHERE k=$1 ORDER BY id"
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, 26)
        sock.sendall(packet(b"P", parse) + packet(b"D", b"S\0") + packet(b"S"))
        description = client.read_until_ready(sock)
        expected = [(b"k", relation, 2, 26, 4, -1, 0)]
        type_fields = client.row_description_fields(description) if any(kind == b"T" for kind, _ in description) else []
        valid = bool(relation and b"".join(kind for kind, _ in description) == b"1tTZ"
                     and description[1] == (b"t", struct.pack("!HI", 1, 26)) and type_fields == expected)
        review.record("physical OID column Describe", valid, type_fields, expected)
        for value, count in ((0, 1), (2147483648, 1), (4294967295, 1), (17, 0), (None, 0)):
            payload = struct.pack("!I", value) if value is not None else b""
            bind = b"\0\0" + struct.pack("!HHHi", 1, 1, 1, len(payload) if payload else -1)
            bind += payload + struct.pack("!HH", 1, 1)
            sock.sendall(packet(b"B", bind) + packet(b"D", b"P\0")
                         + packet(b"E", b"\0" + struct.pack("!I", 0)) + packet(b"S"))
            frames = client.read_until_ready(sock)
            data = client.data_row_values(frames)
            fields = client.row_description_fields(frames) if any(kind == b"T" for kind, _ in frames) else []
            shape = b"2T" + b"D" * count + b"CZ"
            valid = (b"".join(kind for kind, _ in frames) == shape and data == [[payload]] * count
                     and fields == [(*field[:-1], 1) for field in expected]
                     and frames[-2:] == [(b"C", f"SELECT {count}\0".encode()), (b"Z", b"I")])
            review.record(f"binary OID {value}", valid, (data, fields), ([[payload]] * count, shape))

        command("CREATE TABLE oid_staging(id integer,k bigint,note text)", "CREATE TABLE")
        command("INSERT INTO oid_staging VALUES (11,4294967295,'high'),(12,-1,'negative'),(13,0,'zero')", "INSERT 0 3")
        command("INSERT INTO stored_oid SELECT * FROM oid_staging WHERE id=11", "INSERT 0 1")
        review.query("INSERT INTO stored_oid SELECT * FROM oid_staging WHERE id=13 "
                     "UNION ALL SELECT * FROM oid_staging WHERE id=12", state="22003")
        review.query("SELECT id,k FROM stored_oid WHERE id>=11 ORDER BY id", rows=[["11", "4294967295"]], oids=[23, 26])
        command("DELETE FROM oid_staging WHERE id=12", "DELETE 1")
        command("ALTER TABLE oid_staging ALTER COLUMN k TYPE pg_catalog.oid USING k::oid", "ALTER TABLE")
        review.query("SELECT id,k FROM oid_staging ORDER BY id",
                     rows=[["11", "4294967295"], ["13", "0"]], oids=[23, 26])

        review.query("INSERT INTO stored_oid(id,k) VALUES (11,0) ON CONFLICT(id) DO UPDATE SET k=2147483648::bigint "
                     "RETURNING k", rows=[["2147483648"]], oids=[26], tag="INSERT 0 1")
        review.query("INSERT INTO stored_oid(id,k) VALUES (11,0) ON CONFLICT(id) DO UPDATE SET k=-1::bigint", state="22003")
        review.query("INSERT INTO stored_oid(id,k) VALUES (11,0) ON CONFLICT(id) DO UPDATE SET k='5'::text", state="42804")
        review.query("SELECT k FROM stored_oid WHERE id=11", rows=[["2147483648"]], oids=[26])
        review.query("SELECT 1.25::numeric::oid", state="42846")
        review.query("SELECT NULL::numeric::oid", state="42846")
        review.query("SELECT '0xffffffff'::text::oid", rows=[["4294967295"]], oids=[26])
        command("CREATE TABLE oid_merge_input(id integer PRIMARY KEY,k bigint)", "CREATE TABLE")
        command("INSERT INTO oid_merge_input VALUES (11,4294967295),(14,2147483648),(15,NULL)", "INSERT 0 3")
        merge = "MERGE INTO stored_oid t USING oid_merge_input s ON (t.id=s.id)"
        merge_write = (merge + " WHEN MATCHED THEN UPDATE SET k=s.k"
                       " WHEN NOT MATCHED THEN INSERT (id,k) VALUES(s.id,s.k)")
        command(merge_write, "MERGE 3")
        expected = [["11", "4294967295"], ["14", "2147483648"], ["15", None]]
        read_merge = "SELECT id,k FROM stored_oid WHERE id>=11 ORDER BY id"
        review.query(read_merge, rows=expected, oids=[23, 26])
        command("UPDATE oid_merge_input SET k=-1 WHERE id=11", "UPDATE 1")
        review.query(merge_write, state="22003")
        review.query(merge + " WHEN MATCHED THEN UPDATE SET k='5'::text", state="42804")
        review.query(merge + " WHEN MATCHED THEN UPDATE SET k=2.5", state="42804")
        review.query("UPDATE stored_oid t SET k=s.k FROM oid_merge_input s WHERE t.id=s.id", state="22003")
        review.query(read_merge, rows=expected, oids=[23, 26])
        command(merge + " WHEN MATCHED THEN UPDATE SET k='010'", "MERGE 3")
        review.query(read_merge, rows=[["11", "8"], ["14", "8"], ["15", "8"]], oids=[23, 26])
        review.complete(77)
    finally:
        review.close()


if __name__ == "__main__":
    main()
