#!/usr/bin/env python3
"""TEMP sequence isolation, namespace, rollback and cleanup against PG18.

All 182 requests remain, including separately tracked Notice metadata gaps.
"""
from compat.numeric_type_review import Review
from compat import temp_sequence_profiles
from pathlib import Path
import json
import sys

CONTROLS=182
def main():
    if sys.argv[1:]==['--list']:
        print(CONTROLS)
        return
    expected=json.loads((Path(__file__).with_name('compat')/'temp_sequence_pg18_expected.json').read_text())
    assert expected['controls']==CONTROLS and len(expected['records'])==CONTROLS
    review=Review('TEMP_SEQUENCE_LIFECYCLE')
    def observe(actual):
        reference=expected['records'][review.controls]
        valid=all(actual[key]==value for key,value in reference.items())
        review.record((actual['ordinal'],actual['profile'],actual['label']),valid,actual,reference)
    try:
        assert temp_sequence_profiles.run(review,observe)==CONTROLS
        review.complete(CONTROLS)
    finally:review.close()
if __name__=='__main__':main()
