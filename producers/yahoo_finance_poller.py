import json
import logging
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_TOPIC,
    YAHOO_POLL_INTERVAL_SECONDS,
    YAHOO_SYMBOL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class YahooFinancePoller:
    def __init__(self, kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=MARKET_TOPIC):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic_name = topic_name
        self.symbol = YAHOO_SYMBOL
        self.poll_interval_seconds = YAHOO_POLL_INTERVAL_SECONDS

    def fetch_latest_tick(self) -> dict | None:
        try:
            url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{self.symbol}"
                "?interval=1m&range=1d"
            )
            resp = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data["chart"]["result"]
            if not result:
                log.info("No data returned (market may be closed)")
                return None
            meta = result[0]["meta"]
            ts = datetime.fromtimestamp(meta["regularMarketTime"], tz=timezone.utc)
            return {
                "symbol": self.symbol,
                "timestamp": ts.isoformat(),
                "close": float(meta["regularMarketPrice"]),
                "volume": int(meta.get("regularMarketVolume", 0)),
            }
        except Exception as e:
            log.error(f"Failed to fetch ticker data: {e}")
            return None

    def to_kafka_message(self, tick: dict) -> dict:
        return tick

    def push_to_kafka(self, message: dict):
        self.producer.send(self.topic_name, value=message, partition=0)
        self.producer.flush()
        log.info(f"Pushed market tick: {message['close']} at {message['timestamp']}")

    def run(self):
        log.info("Yahoo Finance poller started")
        while True:
            try:
                tick = self.fetch_latest_tick()
                if tick:
                    msg = self.to_kafka_message(tick)
                    self.push_to_kafka(msg)
            except Exception as e:
                log.error(f"Unhandled error in poll cycle: {e}")
            time.sleep(self.poll_interval_seconds)


if __name__ == "__main__":
    YahooFinancePoller().run()
