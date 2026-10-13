"""Every top-level standalone E2E driver is scheduled once by the real runner."""
from pathlib import Path
import ast
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    result = subprocess.run(
        ["bash", "-c",
         'source "$1"; dbms_init_build_config "$2" >/dev/null || exit; '
         'printf "%s\\0" "${DBMS_E2E_TESTS[@]}"',
         "registration-inventory", str(ROOT / "scripts/build_common.sh"), str(ROOT)],
        check=True, stdout=subprocess.PIPE)
    registered = result.stdout.decode().rstrip("\0").split("\0")
    assert len(registered) == len(set(registered)), "duplicate driver registration"
    assert registered, "empty driver registration"
    for name in registered:
        path = ROOT / name
        assert path.is_file(), ("missing registered driver", name)
        assert path.resolve().is_relative_to(ROOT / "tests"), name
        ast.parse(path.read_text(encoding="utf-8"), filename=name)
    # Only the repository's standalone naming contract is enforced. Nested
    # reference clients/helpers and *_known_gap.py are not blindly scheduled.
    standalone = {str(p.relative_to(ROOT))
                  for p in (ROOT / "tests").glob("*_e2e_test.py")}
    missing = sorted(standalone - set(registered))
    assert not missing, ("unregistered standalone E2E drivers", missing)
    assert "tests/test_registration_inventory_test.py" in registered
    print("[TEST REGISTRATION]", len(registered), "unique drivers,",
          len(standalone), "standalone E2E files; complete inventory passed")


if __name__ == "__main__":
    main()
