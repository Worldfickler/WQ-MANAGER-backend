import logging
import os
import unittest
from urllib.parse import parse_qs, urlsplit

os.environ.setdefault("SQL_ECHO", "False")

from app.core.logging import RedactAccessQueryFilter


class AccessLogPrivacyTests(unittest.TestCase):
    def record(self, path):
        return logging.LogRecord("uvicorn.access", logging.INFO, "", 0,
                                 '%s - "%s %s HTTP/%s" %d',
                                 ("127.0.0.1:8000", "GET", path, "1.1", 200), None)

    def test_redacts_repeated_and_case_insensitive_credentials_in_access_logs(self):
        record = self.record('/api/v1/submit-wait/stickers/1/image?ticket=private-value&TICKET=second-value&page=2&token=another-value')
        self.assertTrue(RedactAccessQueryFilter().filter(record))
        message = record.getMessage()
        for value in ("private-value", "second-value", "another-value"):
            self.assertNotIn(value, message)
        query = parse_qs(urlsplit(record.args[2]).query)
        self.assertEqual(query, {"ticket": ["***"], "TICKET": ["***"], "page": ["2"], "token": ["***"]})

    def test_preserves_normal_access_logs(self):
        record = self.record('/api/v1/submit-wait/posts?page=2&search=alpha')
        original = record.args
        self.assertTrue(RedactAccessQueryFilter().filter(record))
        self.assertEqual(record.args, original)

    def test_handles_records_without_access_log_arguments(self):
        record = logging.LogRecord("uvicorn.access", logging.INFO, "", 0, "Server started", (), None)
        self.assertTrue(RedactAccessQueryFilter().filter(record))
        self.assertEqual(record.getMessage(), "Server started")
