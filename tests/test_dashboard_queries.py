import unittest

from dashboard.queries import (
    CAMEO_QUERY,
    COUNTRY_QUERY,
    KPI_QUERY,
    SCATTER_QUERY,
    TIMELINE_QUERY,
)


class TestDashboardQueryTemplates(unittest.TestCase):
    """Sanity-check that all query strings format without error."""

    def _format(self, query: str, days: int = 30) -> str:
        return query.format(days=days)

    def test_kpi_query_formats(self):
        sql = self._format(KPI_QUERY)
        self.assertIn("fact_correlation", sql)

    def test_cameo_query_formats(self):
        sql = self._format(CAMEO_QUERY)
        self.assertIn("dim_event_type", sql)

    def test_country_query_formats(self):
        sql = self._format(COUNTRY_QUERY)
        self.assertIn("dim_country", sql)

    def test_scatter_query_formats(self):
        sql = self._format(SCATTER_QUERY)
        self.assertIn("goldstein_score", sql)

    def test_timeline_query_formats(self):
        sql = self._format(TIMELINE_QUERY)
        self.assertIn("event_date", sql)


if __name__ == "__main__":
    unittest.main()
