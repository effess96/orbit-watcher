# Overnight evidence review — 22 September 2026

The supplied exports contain 555,571 account updates across 25 account addresses,
from 21 September 2026 11:58:45 UTC to 22 September 2026 06:24:35 UTC (18.43 hours).
There are 190 shock rows and 116 closed gap rows. The screenshot's slightly larger
update/shock totals were captured at a different point from these exports.

| Token | Closed gaps | Historical model-positive | Positive gap records lasting over 1 second | Unknown depth |
|---|---:|---:|---:|---:|
| Bonk | 15 | 9 | 0 | 0 |
| Fartcoin | 35 | 13 | 6 | 15 |
| GOAT | 58 | 1 | 0 | 0 |
| POPCAT | 8 | 2 | 0 | 1 |
| Total | 116 | 25 | 6 | 16 |

75 records failed the original depth model. 52 of 116 gaps closed within one
observed slot; median gap duration was 0.572 seconds. These observations do not
identify another trader or establish how long an executable return persisted.

Every historical positive involves at least one concentrated-liquidity pool.
The old calculation extrapolates current active liquidity without verifying tick
crossings. Consequently none of these 25 classifications is execution evidence.
The original records are preserved, not retroactively changed or presented as a
successful replay of the upgraded model.

The six longer Fartcoin records are useful investigation cases. They are not six
independent trading opportunities: they can overlap, share pools, and contain only
a brief positive peak. First check their deployed pool mappings and state freshness,
then capture complete tick/bin state and test exact-size output and execution delay.

The deployed configuration was not included in the original ZIP. The JSONL also
lacks evaluation and connection boundaries. Those limitations prevent exact
reconstruction of this overnight run; no claim of such a replay is made here.

`overnight-audit.json` contains reproducible aggregates and SHA-256 hashes of the
three input files. `audit_results.py` regenerates it without network access. Raw
exports are not committed to this repository. No profits or fills were observed.
