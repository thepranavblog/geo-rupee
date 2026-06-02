import sys
import types
import unittest
from unittest.mock import MagicMock

kafka_stub = types.ModuleType("kafka")
kafka_stub.KafkaProducer = MagicMock
sys.modules["kafka"] = kafka_stub

sys.path.insert(0, "producers")

from yahoo_finance_poller import YahooFinancePoller  # noqa: E402


class TestYahooFinancePoller(unittest.TestCase):
    def setUp(self):
        self.poller = YahooFinancePoller.__new__(YahooFinancePoller)
        self.poller.producer = MagicMock()
        self.poller.topic_name = "market-data"
        self.poller.symbol = "USDINR=X"
        self.poller.poll_interval_seconds = 120

    def test_to_kafka_message_passthrough(self):
        tick = {"symbol": "USDINR=X", "timestamp": "2024-01-15T10:00:00", "close": 83.22, "volume": 1000}
        result = self.poller.to_kafka_message(tick)
        self.assertEqual(result, tick)

    def test_push_to_kafka_calls_send(self):
        msg = {"symbol": "USDINR=X", "timestamp": "2024-01-15T10:00:00", "close": 83.22, "volume": 1000}
        self.poller.push_to_kafka(msg)
        self.poller.producer.send.assert_called_once_with("market-data", value=msg, partition=0)
        self.poller.producer.flush.assert_called_once()


if __name__ == "__main__":
    unittest.main()
