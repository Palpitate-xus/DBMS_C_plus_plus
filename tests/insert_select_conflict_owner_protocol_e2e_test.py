#!/usr/bin/env python3
"""Register all original R39 known-gap assertions without rewriting them."""
import importlib.util
from pathlib import Path


def main():
    path = Path(__file__).resolve().parent / 'compat/insert_select_conflict_owner_known_gap.py'
    spec = importlib.util.spec_from_file_location('insert_select_conflict_owner_regression', path)
    regression = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(regression)
    regression.main()


if __name__ == '__main__':
    main()
