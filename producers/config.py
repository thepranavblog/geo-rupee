GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
GDELT_POLL_INTERVAL_SECONDS = 900  # 15 minutes

RELEVANT_COUNTRIES = ["IND", "CHN", "USA", "RUS", "UAE", "SAU", "PAK", "GBR", "JPN", "DEU"]

REGION_PARTITION_MAP = {
    0: ["IND", "PAK", "BGD", "LKA"],
    1: ["CHN", "JPN", "KOR"],
    2: ["UAE", "SAU", "IRN", "IRQ"],
    3: ["USA", "GBR", "DEU", "FRA"],
}
# All other countries -> partition 4

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GDELT_TOPIC = "geopolitical-events"
MARKET_TOPIC = "market-data"

YAHOO_POLL_INTERVAL_SECONDS = 120  # 2 minutes
YAHOO_SYMBOL = "USDINR=X"
