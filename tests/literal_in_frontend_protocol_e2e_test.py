#!/usr/bin/env python3
"""Literal IN uses one typed predicate, one sort and SQL NULL truth."""
import argparse
import importlib.util
import json
import socket
import uuid
from pathlib import Path

CONTROLS=json.loads(r'''[
  [
    "SELECT id FROM {table} WHERE v IN (B'',B'01') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "3"
      ],
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'',B'01') ORDER BY id DESC",
    [
      [
        "5"
      ],
      [
        "3"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',B'01') ORDER BY id",
    [
      [
        "2"
      ],
      [
        "6"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',B'01') ORDER BY id DESC",
    [
      [
        "6"
      ],
      [
        "2"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'',NULL) ORDER BY id",
    [
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'',NULL) ORDER BY id DESC",
    [
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) ORDER BY id",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) ORDER BY id DESC",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (NULL,B'01') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (NULL,B'01') ORDER BY id DESC",
    [
      [
        "5"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (NULL,B'01') ORDER BY id",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (NULL,B'01') ORDER BY id DESC",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'01',B'',B'01') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "3"
      ],
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'01',B'',B'01') ORDER BY id DESC",
    [
      [
        "5"
      ],
      [
        "3"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'01',B'',B'01') ORDER BY id",
    [
      [
        "2"
      ],
      [
        "6"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'01',B'',B'01') ORDER BY id DESC",
    [
      [
        "6"
      ],
      [
        "2"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN ('', '01') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "3"
      ],
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN ('', '01') ORDER BY id DESC",
    [
      [
        "5"
      ],
      [
        "3"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN ('','01') ORDER BY id",
    [
      [
        "2"
      ],
      [
        "6"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN ('','01') ORDER BY id DESC",
    [
      [
        "6"
      ],
      [
        "2"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'',B'01') AND id>1 ORDER BY id",
    [
      [
        "3"
      ],
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'',B'01') AND id>1 ORDER BY id DESC",
    [
      [
        "5"
      ],
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) OR id=3 ORDER BY id",
    [
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) OR id=3 ORDER BY id DESC",
    [
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN ('','01') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN ('','01') ORDER BY id LIMIT 1",
    [
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN ('','01') ORDER BY id DESC OFFSET 1",
    [
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t NOT IN ('',NULL) ORDER BY id",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t NOT IN ('',NULL) ORDER BY id LIMIT 1",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t NOT IN ('',NULL) ORDER BY id DESC OFFSET 1",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN ('a,b','it''s') ORDER BY id",
    [
      [
        "5"
      ],
      [
        "6"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN ('a,b','it''s') ORDER BY id LIMIT 1",
    [
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN ('a,b','it''s') ORDER BY id DESC OFFSET 1",
    [
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t NOT IN ('a,b','it''s') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "2"
      ],
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t NOT IN ('a,b','it''s') ORDER BY id LIMIT 1",
    [
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t NOT IN ('a,b','it''s') ORDER BY id DESC OFFSET 1",
    [
      [
        "2"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE i IN (0,1) ORDER BY id",
    [
      [
        "1"
      ],
      [
        "2"
      ],
      [
        "3"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE i IN (0,1) ORDER BY id LIMIT 1",
    [
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE i IN (0,1) ORDER BY id DESC OFFSET 1",
    [
      [
        "2"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE i NOT IN (0,NULL) ORDER BY id",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE i NOT IN (0,NULL) ORDER BY id LIMIT 1",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE i NOT IN (0,NULL) ORDER BY id DESC OFFSET 1",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'01',B'') ORDER BY id",
    [
      [
        "1"
      ],
      [
        "3"
      ],
      [
        "5"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'01',B'') ORDER BY id LIMIT 1",
    [
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'01',B'') ORDER BY id DESC OFFSET 1",
    [
      [
        "3"
      ],
      [
        "1"
      ]
    ],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) ORDER BY id",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) ORDER BY id LIMIT 1",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN (B'',NULL) ORDER BY id DESC OFFSET 1",
    [],
    null,
    [
      23
    ]
  ],
  [
    "SELECT id FROM {table} WHERE t IN (B'',B'01') ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {empty_table} WHERE t IN (B'',B'01') ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {table} WHERE i NOT IN (B'',NULL) ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {empty_table} WHERE i NOT IN (B'',NULL) ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {table} WHERE v IN (B'01',1) ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {empty_table} WHERE v IN (B'01',1) ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {table} WHERE v NOT IN ('01'::text,B'') ORDER BY id",
    [],
    "42883",
    []
  ],
  [
    "SELECT id FROM {empty_table} WHERE v NOT IN ('01'::text,B'') ORDER BY id",
    [],
    "42883",
    []
  ]
]''')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--reference18",action="store_true")
    options=parser.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("literal_in_frontend_runner",root/"tests/compat/pg_diff_runner.py")
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=None
    if options.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server["sock"]
    schema="literal_in_frontend_"+uuid.uuid4().hex[:12]
    table=schema+".bits";empty_table=schema+".empty_bits";created=False
    failures=[]
    def check(sql,expected,state,oids):
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
        assert actual[0]==expected and actual[1]==state,(sql,actual,expected,state)
        if state is None:
            assert actual[5]==oids,(sql,actual,oids)
            if sql.startswith("SELECT"):
                assert actual[3]==["id"] and actual[4]=="SELECT "+str(len(expected)),(sql,actual)
                fields=client.row_description_fields(messages)
                assert len(fields)==1 and fields[0][3:]==(23,4,-1,0),(sql,fields)
    try:
        check("CREATE SCHEMA "+schema,[],None,[]);created=True
        check("CREATE TABLE "+table+"(id int,v varbit,t text,i int)",[],None,[])
        check("CREATE TABLE "+empty_table+"(id int,v varbit,t text,i int)",[],None,[])
        check("INSERT INTO "+table+" VALUES "+
              "(1,B'01','01',1),(2,B'1','1',1),(3,B'','',0),(4,NULL,NULL,NULL),"+
              "(5,B'01','a,b',2),(6,B'10','it''s',3)",[],None,[])
        for template,expected,state,oids in CONTROLS:
            sql=template.format(table=table,empty_table=empty_table)
            try:check(sql,expected,state,oids)
            except AssertionError as error:
                failures.append(str(error));print("[LITERAL IN FRONTEND FAIL] "+str(error),flush=True)
        assert not failures,"%d/%d complete controls failed"%(len(failures),len(CONTROLS))
        print("[LITERAL IN FRONTEND PROTOCOL] all %d complete NULL/empty/type/order/limit/data controls passed"%len(CONTROLS))
    finally:
        try:
            if created:check("DROP SCHEMA "+schema+" CASCADE",[],None,[])
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=="__main__":main()
