"""Regression checks for research evidence, not trading profitability."""
import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import watcher as W
from test_watcher import config, START, VA_T, VA_Q, VB_T, VB_Q


class Integrity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.rec = W.Recorder(self.path)
        self.addCleanup(self.rec.close)
        self.engine = W.Engine(config(), self.rec)
        for addr, amount in START.items():
            self.engine.on_vault(addr, amount, 100, 1000)
        self.engine.evaluate(1000)

    def open_gap(self):
        self.engine.on_vault(VA_T, 1050000 * 10**6, 101, 1000.4)
        self.engine.on_vault(VA_Q, 952380952381, 101, 1000.4)
        self.engine.evaluate(1000.5)
        self.assertEqual(self.engine.open_count(), 1)

    def test_idle_state_expires_without_counting_market_closure(self):
        self.open_gap()
        self.engine.evaluate(1011)
        self.assertEqual(self.engine.open_count(), 0)
        self.assertEqual(self.engine.stats['dislocations'], 0)
        self.assertEqual(self.engine.stats['quality_interrupted'], 1)
        self.assertIn('state_age_exceeded', self.engine.blocked_pairs.values())

    def test_tip_lag_blocks_candidate(self):
        self.engine.note_tip(120)
        self.engine.on_vault(VA_T, 1100000 * 10**6, 101, 1000.4)
        self.engine.evaluate(1000.5)
        self.assertEqual(self.engine.open_count(), 0)
        self.assertIn('state_slot_lag_exceeded', self.engine.blocked_pairs.values())

    def test_missing_vault_cannot_survive_reconnect(self):
        self.open_gap()
        self.engine.interrupt()
        self.engine.on_vault(VA_T, 1100000 * 10**6, 102, 1001)
        self.engine.evaluate(1001)
        self.assertFalse(self.engine.watches[0].pools[0].ready())
        self.assertEqual(self.engine.open_count(), 0)

    def test_cross_currency_depth_is_unverified(self):
        w = self.engine.watches[0]
        self.assertIsNone(self.engine._depth(w, *w.pools, True))
        self.assertEqual(self.engine.depth_reason(*w.pools, True), 'conversion_depth_unmodeled')

    def test_direction_reversal_creates_separate_observations(self):
        self.open_gap()
        self.engine.on_vault(VA_T, START[VA_T], 102, 1000.8)
        self.engine.on_vault(VA_Q, int(START[VA_Q] * 1.1), 102, 1000.8)
        self.engine.evaluate(1000.9)
        self.assertEqual(self.engine.stats['dislocations'], 1)
        self.assertEqual(self.engine.open_count(), 1)
        current = next(iter(self.engine.watches[0].open.values()))
        self.assertEqual((current['buy'], current['sell']), ('B', 'A'))
        self.assertEqual(current['start_slot'], 102)

    def test_replay_preserves_intraslot_evaluation_timing(self):
        self.open_gap()
        # A close in the SAME slot must not disappear through slot grouping.
        self.engine.on_vault(VB_T, 1050000 * 10**6, 101, 1000.7)
        self.engine.on_vault(VB_Q, 952380952381, 101, 1000.7)
        self.engine.evaluate(1000.85)
        self.rec.raw_fh.flush()
        W.replay(config(), self.path / 'raw_updates.jsonl', self.path / 'replay')
        self.assertEqual((self.path / 'dislocations.csv').read_text(),
                         (self.path / 'replay/dislocations.csv').read_text())

    def test_replay_preserves_interruption_and_recorded_settings(self):
        self.open_gap()
        self.engine.interrupt()
        self.rec.raw_fh.flush()
        replayed = W.replay(config(cost=500), self.path / 'raw_updates.jsonl', self.path / 'replay')
        self.assertEqual(replayed.stats['interrupted'], 1)
        self.assertEqual(replayed.open_count(), 0)
        self.assertEqual(replayed.watches[0].cost_sol, 0.0005)

    def test_incomplete_new_recording_is_rejected(self):
        raw = self.path / 'truncated.jsonl'
        raw.write_text(json.dumps({'event': 'evaluate', 't': 1001, 'tip': 101}) + '\n')
        with self.assertRaisesRegex(ValueError, 'Incomplete recording'):
            W.replay(config(), raw, self.path / 'replay')

    def test_legacy_replay_explicitly_labels_approximation(self):
        self.rec.raw_fh.flush()
        raw = self.path / 'legacy.jsonl'
        raw.write_text('\n'.join(line for line in (self.path / 'raw_updates.jsonl').read_text().splitlines()
                                 if 'event' not in json.loads(line)) + '\n')
        with redirect_stdout(io.StringIO()) as output:
            W.replay(config(), raw, self.path / 'replay')
        self.assertIn('approximate', output.getvalue())

    def test_control_records_do_not_inflate_update_count(self):
        self.rec.raw_fh.flush()
        self.assertEqual(W.raw_span(self.path / 'raw_updates.jsonl')[0], 4)

    def test_export_excludes_credentials_at_each_level(self):
        cfg = config()
        cfg.update(rpc_http='https://secret.example/key', TELEGRAM_BOT_TOKEN='secret')
        cfg['watches'][0]['password'] = 'secret'
        cfg['watches'][0]['pools'][0]['api_key'] = 'secret'
        safe = W.research_config(cfg)
        self.assertNotIn('secret', json.dumps(safe))
        self.assertEqual(safe['watches'][0]['pools'][0]['base_vault'], VA_T)
        W.Watch(safe['watches'][0])

    def test_export_records_effective_defaults(self):
        safe = W.research_config(config())
        self.assertEqual(safe['max_state_age_s'], 10)
        self.assertEqual(safe['max_state_slot_lag'], 8)
        self.assertEqual(safe['watches'][0]['cost_sol'], 0.0005)

    def test_invalid_record_inside_new_session_is_rejected(self):
        self.rec.raw_fh.write('{truncated\n')
        self.rec.raw_fh.flush()
        with self.assertRaisesRegex(ValueError, 'Malformed line'):
            W.replay(config(), self.path / 'raw_updates.jsonl', self.path / 'replay')

    def test_rejected_comparison_is_removed_from_live_gauges(self):
        self.open_gap()
        self.assertIsNotNone(self.engine.best_net_gap_pct(self.engine.watches[0]))
        self.engine.evaluate(1011)
        self.assertIsNone(self.engine.best_net_gap_pct(self.engine.watches[0]))
        self.assertIsNone(self.engine.max_gap_pct(self.engine.watches[0]))

    def test_historical_csv_rows_preserved_without_revalidation(self):
        path = self.path / 'old' / 'dislocations.csv'
        path.parent.mkdir()
        old = W.DISLOCATION_FIELDS[:-3]
        with path.open('w') as fh:
            writer = csv.DictWriter(fh, fieldnames=old)
            writer.writeheader()
            writer.writerow({'watch': 'historical', 'tradable': 'yes'})
        W.migrate_csv(path, W.DISLOCATION_FIELDS)
        rows = W.read_csv(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['tradable'], 'yes')
        self.assertEqual(rows[0]['execution_verified'], '')

    def test_invalid_fee_and_thresholds_rejected(self):
        for value in (-1, float('nan'), float('inf')):
            cfg = config(cost=value)
            with self.assertRaises(ValueError):
                W.Watch(cfg['watches'][0])
            cfg = config()
            cfg['watches'][0]['pools'][0]['fee'] = value
            with self.assertRaises(ValueError):
                W.Watch(cfg['watches'][0])


if __name__ == '__main__':
    unittest.main()
