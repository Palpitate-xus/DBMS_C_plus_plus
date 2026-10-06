#!/usr/bin/env python3
"""Independent unsupported-function diagnostic; intentionally not a green gate.

Retain PostgreSQL's result/OID expectation. ARRAY/|| fixes do not implement
the separate, previously unregistered array_positions builtin.
"""
import importlib.util
from pathlib import Path
root=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('array_positions_runner',root/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client();server=r.start_ours(c)
try:
    result=r.decode_wire_result(c.simple_query(server['sock'],'SELECT array_positions(ARRAY[1,2,1],1);'),include_types=True)
    print('ARRAY_POSITIONS_EXPECTED', [['{1,3}']],[1007],'ACTUAL',result,flush=True)
    assert result[1] is None and result[0]==[['{1,3}']] and result[5]==[1007],result
finally:r.stop_ours(server)
