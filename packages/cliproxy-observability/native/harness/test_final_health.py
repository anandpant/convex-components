"""Qualification-oracle regressions; SQLite contains synthetic fixture data only."""
import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock

import proof


class FinalHealthTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        original = proof.ROOT
        self.addCleanup(setattr, proof, 'ROOT', original)
        proof.ROOT = Path(self.directory.name)
        (proof.ROOT / 'outbox').mkdir()
        self.db = sqlite3.connect(proof.ROOT / 'outbox/events.db')
        self.addCleanup(self.db.close)
        self.db.executescript('''
            CREATE TABLE events(destination, instance, boot, request_id, sequence, payload);
            CREATE TABLE health(payload);
        ''')
        self.boot = 'current-boot'
        self.instance = 'fixture-instance'
        self.config = {'instanceId': self.instance, 'bindings': [
            {'destinationId': 'harness-dev'}, {'destinationId': 'harness-prod'},
        ]}
        self.shutdown = {'pid': 123, 'startedAtNs': 1700000000000000000,
                         'completedAtNs': 1700000000100000000, 'returnCode': 0}
        body = b'{"stream":false}'
        request = {'pluginBootId': self.boot, 'instanceId': self.instance,
                   'destinationId': 'harness-dev', 'requestId': 'tail',
                   'sequence': 1, 'kind': 'request',
                   'observedAt': '2023-11-14T22:13:19.8Z',
                   'body': base64.b64encode(body).decode(),
                   'contentBytes': len(body), 'observedBodyBytes': len(body),
                   'contentSha256': hashlib.sha256(body).hexdigest()}
        self.tail = {**request, 'sequence': 2, 'kind': 'completion',
                     'observedAt': '2023-11-14T22:13:19.9Z',
                     'body': '', 'contentBytes': 0, 'observedBodyBytes': 0,
                     'contentSha256': hashlib.sha256(b'').hexdigest(),
                     'error': 'fixture error', 'observedErrorBytes': 13}
        self.ledger = [request, self.tail]
        self.content_bytes = len(body) + len(self.tail['error'].encode())
        self.health = {'pluginBootId': self.boot, 'instanceId': self.instance,
                       'destinationId': 'harness-dev',
                       'observedAt': '2023-11-14T22:13:20.050000123Z',
                       'observationsTotal': 2, 'activeCalls': 0,
                       'droppedObservationsTotal': 0, 'lostControlObservationsTotal': 0,
                       'precommitCoverage': 'unknown_before_local_commit',
                       'content': {'observedBytes': self.content_bytes,
                                   'committedBytes': self.content_bytes,
                                   **{k: 0 for k in ['lostBytes', 'unconfirmedBytes',
                                       'unknownLosses', 'missedCalls', 'incompleteCalls',
                                       'capacityRejections', 'retainedChargedBytes']}}}
        self.persist()

    def persist(self):
        self.db.execute('DELETE FROM events')
        self.db.execute('DELETE FROM health')
        for e in self.ledger:
            self.db.execute('INSERT INTO events VALUES (?,?,?,?,?,?)',
                            (e['destinationId'], e['instanceId'], e['pluginBootId'],
                             e['requestId'], e['sequence'], json.dumps(e)))
        for binding in self.config['bindings']:
            self.db.execute('INSERT INTO health VALUES (?)',
                            (json.dumps({**self.health, 'destinationId': binding['destinationId']}),))
        self.db.commit()

    def verify(self):
        return proof.final_health(self.boot, 1, self.tail, self.shutdown, self.config)

    def test_accepts_post_shutdown_publication_covering_tail_and_all_bytes(self):
        health, boundary = self.verify()
        self.assertEqual(len(health), 2)
        self.assertEqual(boundary['pluginBootId'], self.boot)
        self.assertEqual(boundary['observations'], 2)
        self.assertEqual(boundary['contentBytes'], self.content_bytes)
        self.assertEqual(boundary['tailSequence'], 2)
        self.assertEqual(boundary['shutdown'], self.shutdown)

    def test_failed_final_persistence_cannot_reuse_healthy_periodic_row(self):
        # The old row already covers the tail and all bytes. Only a successful
        # post-shutdown-start publication can qualify it as final evidence.
        self.health['observedAt'] = '2023-11-14T22:13:19.999999999Z'
        self.persist()
        self.db.executescript('''CREATE TRIGGER refuse_final_health
            BEFORE UPDATE ON health BEGIN
                SELECT RAISE(ABORT, 'simulated final health persistence failure');
            END;''')
        final = {**self.health, 'observedAt': '2023-11-14T22:13:20.050000123Z'}
        with self.assertRaisesRegex(sqlite3.DatabaseError, 'persistence failure'):
            self.db.execute('UPDATE health SET payload=?', (json.dumps(final),))
        self.db.rollback()
        retained = json.loads(self.db.execute('SELECT payload FROM health').fetchone()[0])
        self.assertEqual(retained['observedAt'], self.health['observedAt'])
        with self.assertRaisesRegex(AssertionError, 'publication missing or stale'):
            self.verify()

    def test_rejects_wrong_boot_even_when_all_counters_are_healthy(self):
        self.health['pluginBootId'] = 'previous-boot'
        self.persist()
        with self.assertRaisesRegex(AssertionError, 'current-boot health missing'):
            self.verify()

    def test_rejects_missing_destination_final_row(self):
        self.db.execute("DELETE FROM health WHERE json_extract(payload,'$.destinationId')='harness-prod'")
        self.db.commit()
        with self.assertRaisesRegex(AssertionError, 'current-boot health missing'):
            self.verify()

    def test_rejects_fresh_row_with_old_observation_count_or_byte_total(self):
        for field in ['observationsTotal', 'committedBytes']:
            with self.subTest(field=field):
                original = deepcopy(self.health)
                if field == 'observationsTotal':
                    self.health[field] -= 1
                else:
                    self.health['content'][field] -= 1
                self.persist()
                with self.assertRaisesRegex(AssertionError, 'complete ledger|byte accounting'):
                    self.verify()
                self.health = original

    def test_rechecks_gaps_sequences_hashes_and_terminal_after_tail(self):
        for change in [{'gap': 'capture_content_capacity'}, {'captureIncomplete': True},
                       {'sequence': 3}, {'contentSha256': '0'*64}, {'kind': 'response'}]:
            with self.subTest(change=change):
                original = deepcopy(self.ledger)
                self.ledger[-1] = {**self.ledger[-1], **change}
                self.persist()
                with self.assertRaises(AssertionError):
                    self.verify()
                self.ledger = original

    def test_rejects_missing_or_changed_expected_tail(self):
        self.tail = {**self.tail, 'requestId': 'different-tail'}
        with self.assertRaisesRegex(AssertionError, 'tail completion missing or changed'):
            self.verify()

    def test_rejects_post_stop_publication_outside_shutdown_window(self):
        self.health['observedAt'] = '2023-11-14T22:13:20.100000001Z'
        self.persist()
        with self.assertRaisesRegex(AssertionError, 'publication missing or stale'):
            self.verify()

    def test_timestamp_keeps_nanosecond_boundary(self):
        self.assertEqual(proof.timestamp_ns('2023-11-14T22:13:19.999999999Z'),
                         self.shutdown['startedAtNs']-1)


class StopProcessTest(unittest.TestCase):
    def test_nonzero_exit_cannot_be_labeled_graceful(self):
        for code in [1, -15]:
            with self.subTest(code=code):
                process = Mock(pid=123)
                process.poll.return_value = None
                process.wait.return_value = code
                with self.assertRaisesRegex(AssertionError, 'graceful shutdown failed'):
                    proof.stop_proc(process)

    def test_already_exited_process_is_not_controlled_shutdown(self):
        process = Mock(pid=123)
        process.poll.return_value = 0
        with self.assertRaisesRegex(AssertionError, 'before controlled shutdown'):
            proof.stop_proc(process)
        process.terminate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
