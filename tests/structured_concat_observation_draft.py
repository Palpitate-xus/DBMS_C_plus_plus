#!/usr/bin/env python3
"""Unaccepted diagnostic for neighboring structured || implementations."""
from compat.numeric_type_review import Review
import sys


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('STRUCTURED_CONCAT_OBSERVATION')
    try:
        for expression, value in (
                ("('{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb)::text", '{"a": 1, "b": 2}'),
                ("('[1]'::jsonb || '[2,3]'::jsonb)::text", '[1, 2, 3]'),
                ("('1'::jsonb || '2'::jsonb)::text", '[1, 2]'),
                ("('''a'':1'::tsvector || '''b'':1'::tsvector)::text", "'a':1 'b':2")):
            review.query('SELECT ' + expression + ' AS v', [[value]], [25])
        review.complete(4)
    finally:
        review.close()


if __name__ == '__main__':
    main()
