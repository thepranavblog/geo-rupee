import unittest

# Spark tests require a running Spark context; these are lightweight schema/config checks.

class TestSparkSchemas(unittest.TestCase):
    def test_event_schema_import(self):
        """Schemas module should be importable if PySpark is installed."""
        try:
            import sys
            sys.path.insert(0, "spark")
            from schemas import EVENT_SCHEMA, MARKET_SCHEMA  # noqa: F401
            self.assertEqual(len(EVENT_SCHEMA.fields), 10)
            self.assertEqual(len(MARKET_SCHEMA.fields), 4)
        except ImportError:
            self.skipTest("PySpark not installed in test environment")

    def test_spark_config_values(self):
        import sys
        sys.path.insert(0, "spark")
        try:
            from config import (  # noqa: F401
                EVENT_TOPIC,
                KAFKA_BOOTSTRAP_SERVERS,
                MARKET_TOPIC,
                RELEVANT_COUNTRIES,
            )
            self.assertEqual(EVENT_TOPIC, "geopolitical-events")
            self.assertEqual(MARKET_TOPIC, "market-data")
            self.assertIn("IND", RELEVANT_COUNTRIES)
        except ImportError:
            self.skipTest("Spark config not importable")


if __name__ == "__main__":
    unittest.main()
