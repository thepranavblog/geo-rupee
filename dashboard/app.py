from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from config import DEFAULT_DATE_RANGE_DAYS, DUCKDB_PATH, PAGE_TITLE, REFRESH_INTERVAL_MS
from queries import CAMEO_QUERY, COUNTRY_QUERY, KPI_QUERY, SCATTER_QUERY, TIMELINE_QUERY


class GeopoliticalDashboard:
    def __init__(self):
        self.duckdb_path = DUCKDB_PATH
        self.connection = None
        self.date_range_days = DEFAULT_DATE_RANGE_DAYS

    def connect_duckdb(self):
        self.connection = duckdb.connect(self.duckdb_path, read_only=True)

    def _query(self, sql: str, days: int) -> pd.DataFrame:
        return self.connection.execute(sql.format(days=days)).df()

    def load_kpis(self, days: int) -> dict:
        df = self._query(KPI_QUERY, days)
        if df.empty:
            return {"total_events": 0, "avg_rupee_move": 0.0, "biggest_drop": 0.0, "most_active_country": "N/A"}
        row = df.iloc[0]
        return {
            "total_events": int(row.get("total_events", 0)),
            "avg_rupee_move": float(row.get("avg_rupee_move", 0.0) or 0.0),
            "biggest_drop": float(row.get("biggest_drop", 0.0) or 0.0),
            "most_active_country": str(row.get("most_active_country", "N/A") or "N/A"),
        }

    def load_cameo_data(self, days: int) -> pd.DataFrame:
        return self._query(CAMEO_QUERY, days)

    def load_country_data(self, days: int) -> pd.DataFrame:
        return self._query(COUNTRY_QUERY, days)

    def load_scatter_data(self, days: int) -> pd.DataFrame:
        return self._query(SCATTER_QUERY, days)

    def load_timeline_data(self, days: int) -> pd.DataFrame:
        return self._query(TIMELINE_QUERY, days)

    def render_kpis(self, data: dict):
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events Analyzed", f"{data['total_events']:,}")
        col2.metric("Avg Rupee Move at 1hr (%)", f"{data['avg_rupee_move']:.3f}%")
        col3.metric("Biggest Drop in 2hrs (%)", f"{data['biggest_drop']:.3f}%")
        col4.metric("Most Active Country", data["most_active_country"])

    def render_cameo_chart(self, df: pd.DataFrame):
        st.subheader("Rupee Movement by Event Category (CAMEO)")
        if df.empty:
            st.info("No data available for this date range.")
            return
        fig = px.bar(
            df,
            x="avg_rupee_change",
            y="category_label",
            orientation="h",
            color="avg_rupee_change",
            color_continuous_scale=["red", "white", "green"],
            color_continuous_midpoint=0,
            labels={"avg_rupee_change": "Avg 1hr Rupee Change (%)", "category_label": "Event Category"},
            text="event_count",
        )
        fig.update_traces(texttemplate="%{text} events", textposition="outside")
        fig.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    def render_country_section(self, df: pd.DataFrame):
        st.subheader("Rupee Movement by Country")
        if df.empty:
            st.info("No data available for this date range.")
            return
        col1, col2 = st.columns([1, 1])
        with col1:
            st.dataframe(df, use_container_width=True)
        with col2:
            fig = px.bar(
                df.sort_values("avg_1hr"),
                x="avg_1hr",
                y="country_name",
                orientation="h",
                color="avg_1hr",
                color_continuous_scale=["red", "white", "green"],
                color_continuous_midpoint=0,
                labels={"avg_1hr": "Avg 1hr Change (%)", "country_name": "Country"},
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    def render_scatter(self, df: pd.DataFrame):
        st.subheader("Goldstein Score vs Rupee Change (1hr)")
        if df.empty:
            st.info("No data available for this date range.")
            return
        fig = px.scatter(
            df,
            x="goldstein_score",
            y="change_1hr_pct",
            color="category_label",
            hover_data=["country_name", "event_timestamp"],
            labels={
                "goldstein_score": "Goldstein Score (-10 to +10)",
                "change_1hr_pct": "Rupee Change at 1hr (%)",
                "category_label": "Category",
            },
        )
        fig.add_vline(x=0, line_dash="dash", line_color="gray")
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    def render_timeline(self, df: pd.DataFrame):
        st.subheader("Event Timeline vs Rupee Rate")
        if df.empty:
            st.info("No data available for this date range.")
            return
        fig = px.line(
            df,
            x="event_date",
            y="avg_rupee_rate",
            labels={"event_date": "Date", "avg_rupee_rate": "Avg USD/INR Rate"},
        )
        fig.add_scatter(
            x=df["event_date"],
            y=df["min_goldstein"],
            mode="markers",
            name="Min Goldstein Score",
            yaxis="y2",
            marker=dict(color="red", size=6),
        )
        fig.update_layout(
            height=400,
            yaxis2=dict(title="Min Goldstein Score", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, use_container_width=True)

    def run(self):
        st.set_page_config(page_title=PAGE_TITLE, layout="wide")
        st_autorefresh(interval=REFRESH_INTERVAL_MS, key="dashboard_refresh")

        st.title(PAGE_TITLE)
        st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        with st.sidebar:
            st.header("Filters")
            days = st.slider("Date range (days)", min_value=1, max_value=365, value=DEFAULT_DATE_RANGE_DAYS)

        self.connect_duckdb()

        kpis = self.load_kpis(days)
        cameo_df = self.load_cameo_data(days)
        country_df = self.load_country_data(days)
        scatter_df = self.load_scatter_data(days)
        timeline_df = self.load_timeline_data(days)

        self.render_kpis(kpis)
        st.divider()
        self.render_cameo_chart(cameo_df)
        st.divider()
        self.render_country_section(country_df)
        st.divider()
        self.render_scatter(scatter_df)
        st.divider()
        self.render_timeline(timeline_df)


if __name__ == "__main__":
    GeopoliticalDashboard().run()
