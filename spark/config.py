KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
EVENT_TOPIC = "geopolitical-events"
MARKET_TOPIC = "market-data"

EVENT_WATERMARK_THRESHOLD = "30 minutes"
MARKET_WATERMARK_THRESHOLD = "10 minutes"

JOIN_WINDOW_DURATION = "2 hours"
TRIGGER_INTERVAL = "15 minutes"

S3_BRONZE_GDELT_PATH = "s3a://geo-rupee-pipeline/bronze/gdelt-raw/"
S3_BRONZE_MARKET_PATH = "s3a://geo-rupee-pipeline/bronze/market-raw/"
S3_SILVER_CORRELATED_PATH = "s3a://geo-rupee-pipeline/silver/correlated/"
S3_CHECKPOINT_PATH = "s3a://geo-rupee-pipeline/checkpoints/spark/"

RELEVANT_COUNTRIES = ["IND", "CHN", "USA", "RUS", "UAE", "SAU", "PAK", "GBR", "JPN", "DEU"]
