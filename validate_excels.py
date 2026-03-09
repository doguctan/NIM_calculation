"""
Simple sanity-check script for the NII Excel inputs.

Usage
=====
    python validate_excels.py

What it does
============
- Uses the existing application logic in `app.py` (no code changes there).
- For each configured data source in `app.DATA_SOURCES`:
  - Tries to load the Excel via `InsertDataExcelReader`.
  - Reports the available SIM_DATE values the app sees.
  - Runs the full decomposition for all adjacent date pairs using
    `_build_figs_for_dates`.
  - Prints a concise OK / ERROR line per date pair, including the original
    error message from the app if something fails (e.g. missing TRY NIM row,
    missing Total TRY book balance, bad dates, etc.).

Interpretation
==============
- If ALL lines are OK for all sources:
    Your Excel files match the structure and content expected by the app.
- If you see any ERROR lines:
    Read the error text; it usually tells you exactly which row/label/date is
    missing or malformed.
"""

from __future__ import annotations

import sys
from typing import List, Tuple

import app


def _check_source(source: str) -> Tuple[bool, List[str]]:
    ok = True
    messages: List[str] = []

    try:
        app._load_data_once(source)
    except Exception as e:
        ok = False
        messages.append(f"[{source}] FAILED to load: {e}")
        return ok, messages

    dates = app.DATES_CACHE.get(source, [])
    if not dates:
        ok = False
        messages.append(f"[{source}] No dates available after loading.")
        return ok, messages

    messages.append(f"[{source}] Available dates: {', '.join(dates)}")

    # Check adjacent date pairs (same as UI typically uses).
    for i in range(len(dates) - 1):
        d0, d1 = dates[i], dates[i + 1]
        try:
            nim_info, *_ = app._build_figs_for_dates(source, d0, d1)
            nim_change = nim_info.get("nim_change", None)
            messages.append(
                f"[{source}] OK {d0} -> {d1} (nim_change={nim_change})"
            )
        except Exception as e:
            ok = False
            messages.append(f"[{source}] ERROR {d0} -> {d1}: {e}")

    return ok, messages


def main(argv: List[str]) -> int:
    overall_ok = True
    for source in app.DATA_SOURCES.keys():
        ok, msgs = _check_source(source)
        overall_ok = overall_ok and ok
        for line in msgs:
            print(line)
        print()

    if overall_ok:
        print("ALL SOURCES PASSED sanity checks.")
        return 0
    else:
        print("Some sources FAILED sanity checks. See messages above.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

