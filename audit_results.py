"""Summarize exported evidence without inventing missing pool configuration.

Usage: python audit_results.py /path/to/exports > overnight-audit.json
Uses standard library only. Does not connect to any service.
"""
import collections
import csv
import hashlib
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path


def audit(folder):
    folder = Path(folder)
    files = {}
    for name in ('raw_updates.jsonl', 'dislocations.csv', 'shocks.csv'):
        path = folder / name
        digest = hashlib.sha256()
        with path.open('rb') as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b''):
                digest.update(chunk)
        files[name] = {'bytes': path.stat().st_size, 'sha256': digest.hexdigest()}
    with (folder / 'dislocations.csv').open(newline='') as fh:
        rows = list(csv.DictReader(fh))
    with (folder / 'shocks.csv').open(newline='') as fh:
        shocks = list(csv.DictReader(fh))
    updates, malformed, controls = 0, 0, 0
    first = last = None
    accounts = set()
    with (folder / 'raw_updates.jsonl').open() as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                r = json.loads(line)
                if 'event' in r:
                    controls += 1
                    continue
                t, account = float(r['t']), r['v']
            except (ValueError, KeyError, TypeError):
                malformed += 1
                continue
            updates += 1
            first = t if first is None else min(first, t)
            last = t if last is None else max(last, t)
            accounts.add(account)
    positives = [r for r in rows if r['tradable'] == 'yes']
    tokens = []
    for token in sorted({r['watch'].strip() for r in rows}):
        subset = [r for r in rows if r['watch'].strip() == token]
        p = [r for r in subset if r['tradable'] == 'yes']
        tokens.append({'token': token, 'closed_gaps': len(subset),
                       'historical_model_positive': len(p),
                       'positive_gap_duration_over_1s': sum(float(r['seconds_open']) > 1 for r in p),
                       'depth_unknown': sum(r['tradable'] == 'unknown' for r in subset)})
    durations = [float(r['seconds_open']) for r in rows]
    return {'source_files': files,
            'observations': {'account_updates': updates, 'account_count': len(accounts),
                             'first_utc': datetime.fromtimestamp(first, timezone.utc).isoformat() if first is not None else None,
                             'last_utc': datetime.fromtimestamp(last, timezone.utc).isoformat() if last is not None else None,
                             'hours': (last - first) / 3600 if first is not None else 0,
                             'control_records': controls, 'malformed_lines': malformed},
            'results': {'shocks': len(shocks), 'closed_gaps': len(rows),
                        'historical_model_positive': len(positives),
                        'historical_model_negative': sum(r['tradable'] == 'no' for r in rows),
                        'depth_unknown': sum(r['tradable'] == 'unknown' for r in rows),
                        'closed_within_1_slot': sum(int(r['slots_open']) <= 1 for r in rows),
                        'median_gap_seconds': statistics.median(durations) if durations else None,
                        'positive_gap_duration_over_1s': sum(float(r['seconds_open']) > 1 for r in positives)},
            'tokens': tokens,
            'limitations': ['Historical classifications are preserved, not revalidated.',
                            'Gap duration does not establish that a positive depth estimate persisted.',
                            'Rows may overlap in time and share pools; counts are not independent trades.',
                            'No earnings or executable profitability can be derived from these exports.',
                            'Exact legacy replay requires deployed settings; evaluation timing was not recorded.']}


if __name__ == '__main__':
    print(json.dumps(audit(sys.argv[1]), indent=2))
