import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# Stub kafka before importing the poller
kafka_stub = types.ModuleType("kafka")
kafka_stub.KafkaProducer = MagicMock
sys.modules["kafka"] = kafka_stub

sys.path.insert(0, "producers")

from gdelt_poller import GDELTPoller  # noqa: E402


class TestGDELTPoller(unittest.TestCase):
    def setUp(self):
        self.poller = GDELTPoller.__new__(GDELTPoller)
        self.poller.producer = MagicMock()
        self.poller.topic_name = "geopolitical-events"
        self.poller.last_processed_url = None
        self.poller.relevant_countries = ["IND", "CHN", "USA"]

    def test_filter_relevant_keeps_actor1_match(self):
        rows = [
            {"actor1_country": "IND", "actor2_country": "GER"},
            {"actor1_country": "BRA", "actor2_country": "ARG"},
        ]
        result = self.poller.filter_relevant(rows)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["actor1_country"], "IND")

    def test_filter_relevant_keeps_actor2_match(self):
        rows = [{"actor1_country": "FRA", "actor2_country": "CHN"}]
        result = self.poller.filter_relevant(rows)
        self.assertEqual(len(result), 1)

    def test_filter_relevant_drops_unrelated(self):
        rows = [{"actor1_country": "BRA", "actor2_country": "ARG"}]
        result = self.poller.filter_relevant(rows)
        self.assertEqual(len(result), 0)

    def test_to_kafka_message_structure(self):
        row = {
            "event_id": "12345",
            "sql_date": "20240115",
            "actor1_country": "IND",
            "actor2_country": "PAK",
            "cameo_code": "190",
            "cameo_root_code": "19",
            "goldstein_score": "-8.0",
            "num_articles": "47",
            "avg_tone": "-6.3",
            "date_added": "20240115103000",
        }
        msg = self.poller.to_kafka_message(row)
        self.assertEqual(msg["event_id"], "12345")
        self.assertEqual(msg["event_date"], "2024-01-15")
        self.assertEqual(msg["event_timestamp"], "2024-01-15T10:30:00")
        self.assertAlmostEqual(msg["goldstein_score"], -8.0)
        self.assertEqual(msg["num_articles"], 47)

    def test_get_partition_for_country(self):
        self.assertEqual(self.poller.get_partition_for_country("IND"), 0)
        self.assertEqual(self.poller.get_partition_for_country("CHN"), 1)
        self.assertEqual(self.poller.get_partition_for_country("UAE"), 2)
        self.assertEqual(self.poller.get_partition_for_country("USA"), 3)
        self.assertEqual(self.poller.get_partition_for_country("BRA"), 4)

    def test_get_latest_file_url_returns_none_if_same(self):
        mock_text = "123 456 http://example.com/file.export.CSV.zip"
        self.poller.last_processed_url = "http://example.com/file.export.CSV.zip"
        with patch("gdelt_poller.requests.get") as mock_get:
            mock_get.return_value.text = mock_text
            mock_get.return_value.raise_for_status = MagicMock()
            result = self.poller.get_latest_file_url()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
