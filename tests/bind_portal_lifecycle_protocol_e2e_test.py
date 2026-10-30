#!/usr/bin/env python3
"""Actual Bind allocation, aborted portal descriptors and result reuse.

All 463 PG-observed requests remain, including unsupported-format errors.
This fixture intentionally remains red while separately tracked gaps exist.
"""
from compat.numeric_type_review import Review
from compat import bind_portal_profiles
from pathlib import Path
import json
import sys

CONTROLS=463
def main():
    if sys.argv[1:]==['--list']:
        print(CONTROLS)
        return
    expected=json.loads((Path(__file__).with_name('compat')/'bind_portal_pg18_expected.json').read_text())
    assert expected['controls']==CONTROLS and len(expected['records'])==CONTROLS
    review=Review('BIND_PORTAL_LIFECYCLE')
    def observe(actual):
        reference=expected['records'][review.controls]
        valid=all(actual[key]==value for key,value in reference.items())
        review.record((actual['ordinal'],actual['profile'],actual['label']),valid,actual,reference)
    try:
        assert bind_portal_profiles.run(review,observe)==CONTROLS
        review.complete(CONTROLS)
    finally:
        review.close()
if __name__=='__main__':main()
