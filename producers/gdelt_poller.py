import io
import logging
import time
import zipfile
from datetime import datetime

import pandas as pd
import requests
from kafka import KafkaProducer
import json

from config import (
    GDELT_LAST_UPDATE_URL,
    GDELT_POLL_INTERVAL_SECONDS,
    GDELT_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    REGION_PARTITION_MAP,
    RELEVANT_COUNTRIES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# GDELT 2.0 column indices (tab-separated, no header, 61 columns total)
# Codebook: https://www.gdeltproject.org/data/documentation/GDELT-Event_Codebook-V2.0.pdf
GDELT_COLS = {
    0:  "event_id",
    1:  "sql_date",
    7:  "actor1_country",
    17: "actor2_country",
    26: "cameo_code",       # EventCode: full CAMEO code e.g. "0431"
    28: "cameo_root_code",  # EventRootCode: 2-digit root e.g. "04" (was wrongly col 27 = EventBaseCode)
    30: "goldstein_score",
    31: "num_mentions",
    33: "num_articles",
    34: "avg_tone",
    59: "date_added",       # DATEADDED: YYYYMMDDHHMMSS (col 59 in the live 61-column schema)
}


class GDELTPoller:
    def __init__(self, kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=GDELT_TOPIC):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic_name = topic_name
        self.last_processed_url = None
        self.relevant_countries = RELEVANT_COUNTRIES

    def get_latest_file_url(self) -> str | None:
        try:
            resp = requests.get(GDELT_LAST_UPDATE_URL, timeout=30)
            resp.raise_for_status()
            for line in resp.text.strip().splitlines():
                parts = line.strip().split()
                if len(parts) >= 3 and parts[2].endswith(".export.CSV.zip"):
                    url = parts[2]
                    if url == self.last_processed_url:
                        log.info("No new GDELT file since last poll")
                        return None
                    return url
        except Exception as e:
            log.error(f"Failed to fetch lastupdate.txt: {e}")
        return None

    def download_and_parse(self, url: str) -> list[dict]:
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                csv_name = z.namelist()[0]
                with z.open(csv_name) as f:
                    df = pd.read_csv(f, sep="\t", header=None, dtype=str, on_bad_lines="skip")
            usecols = [i for i in GDELT_COLS if i < len(df.columns)]
            df = df[usecols].rename(columns=GDELT_COLS)
            return df.to_dict(orient="records")
        except Exception as e:
            log.error(f"Failed to download/parse {url}: {e}")
            return []

    def filter_relevant(self, rows: list[dict]) -> list[dict]:
        return [
            r for r in rows
            if r.get("actor1_country") in self.relevant_countries
            or r.get("actor2_country") in self.relevant_countries
        ]

    def to_kafka_message(self, row: dict) -> dict:
        date_added = str(row.get("date_added", ""))
        try:
            ts = datetime.strptime(date_added, "%Y%m%d%H%M%S").isoformat()
        except Exception:
            ts = datetime.utcnow().isoformat()

        sql_date = str(row.get("sql_date", ""))
        try:
            event_date = datetime.strptime(sql_date, "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            event_date = sql_date

        def safe_float(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def safe_int(v):
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        return {
            "event_id": str(row.get("event_id", "")),
            "event_date": event_date,
            "event_timestamp": ts,
            "actor1_country": row.get("actor1_country") or "",
            "actor2_country": row.get("actor2_country") or "",
            "cameo_code": str(row.get("cameo_code", "")),
            "cameo_root_code": str(row.get("cameo_root_code", "")),
            "goldstein_score": safe_float(row.get("goldstein_score")),
            "num_articles": safe_int(row.get("num_articles")),
            "avg_tone": safe_float(row.get("avg_tone")),
        }

    def get_partition_for_country(self, country_code: str) -> int:
        for partition, countries in REGION_PARTITION_MAP.items():
            if country_code in countries:
                return partition
        return 4

    def push_to_kafka(self, messages: list[dict]):
        for msg in messages:
            partition = self.get_partition_for_country(msg.get("actor1_country", ""))
            self.producer.send(self.topic_name, value=msg, partition=partition)
        self.producer.flush()
        log.info(f"Pushed {len(messages)} messages to {self.topic_name}")

    def run(self):
        log.info("GDELT poller started")
        while True:
            try:
                url = self.get_latest_file_url()
                if url:
                    log.info(f"Processing {url}")
                    raw_rows = self.download_and_parse(url)
                    log.info(f"Raw rows: {len(raw_rows)}")
                    filtered = self.filter_relevant(raw_rows)
                    log.info(f"Filtered rows: {len(filtered)}")
                    messages = [self.to_kafka_message(r) for r in filtered]
                    self.push_to_kafka(messages)
                    self.last_processed_url = url
            except Exception as e:
                log.error(f"Unhandled error in poll cycle: {e}")
            time.sleep(GDELT_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    GDELTPoller().run()
