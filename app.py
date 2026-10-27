"""Taxi / Ride Trip Analytics - Apache Spark (PySpark) + Streamlit dashboard."""
import hashlib
import os
import tempfile

import pandas as pd
import plotly.express as px
import streamlit as st

# ---- pick the engine: Spark if it can start (needs Java), else pandas fallback ----
ENGINE_NOTE = ""
try:
    import spark_engine as engine
    engine.get_spark()
except Exception as e:  # Java missing, low memory, etc.
    import pandas_engine as engine
    ENGINE_NOTE = f"Spark could not start here ({type(e).__name__}); running the pandas fallback."

st.set_page_config(page_title="Taxi Trip Analytics", page_icon="🚖", layout="wide")
HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE = os.path.join(HERE, "taxi_trips.csv")
DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


@st.cache_resource(show_spinner="Processing trips with Spark...")
def load(path, key):
    return engine.load_and_clean(path)


# ---------------- sidebar ----------------
st.sidebar.title("🚖 Controls")
up = st.sidebar.file_uploader("Upload trips CSV", type="csv")
if up is not None:
    content = up.getvalue()
    key = hashlib.md5(content).hexdigest()
    path = os.path.join(tempfile.gettempdir(), f"upload_{key}.csv")
    with open(path, "wb") as fh:
        fh.write(content)
    source = f"Uploaded: {up.name}"
else:
    path, key, source = SAMPLE, "sample", "Sample data (generated, not real ride records)"

try:
    df, stats = load(path, key)
except Exception as e:
    st.error(f"Could not read this file: {e}")
    st.stop()
if stats["rows_out"] == 0:
    st.error("No valid trips left after cleaning. Check your CSV columns and values.")
    st.stop()

zones, pays, dmin, dmax = engine.get_options(df)
date_range = st.sidebar.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax)
sel_zones = st.sidebar.multiselect("Pickup zone", zones, default=zones)
sel_pays = st.sidebar.multiselect("Payment type", pays, default=pays)
hmin, hmax = st.sidebar.slider("Pickup hour of day", 0, 23, (0, 23))

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start, end = date_range
else:
    start = end = date_range[0] if isinstance(date_range, (tuple, list)) else date_range
if not sel_zones or not sel_pays:
    st.warning("Select at least one pickup zone and one payment type.")
    st.stop()

res = engine.analyze(df, start, end, sel_zones, sel_pays, hmin, hmax)
k = res["kpi"]

# ---------------- header ----------------
st.title("🚖 Taxi / Ride Trip Analytics")
st.caption(f"Processing engine: **{engine.NAME}** · Source: {source}")
if ENGINE_NOTE:
    st.warning(ENGINE_NOTE)
st.caption(f"Cleaning: {stats['rows_in']:,} rows read, {stats['rows_dropped']:,} invalid trips removed, "
           f"{stats['rows_out']:,} kept.")
if k["trips"] == 0:
    st.info("No trips match the current filters.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total trips", f"{k['trips']:,}")
c2.metric("Total revenue", f"₹{k['revenue']:,.0f}")
c3.metric("Avg fare", f"₹{k['avg_fare']:,.0f}")
c4.metric("Avg distance", f"{k['avg_distance']:.1f} km")
c5.metric("Avg duration", f"{k['avg_duration']:.0f} min", f"{k['avg_speed']:.0f} km/h avg speed", delta_color="off")

t1, t2, t3, t4 = st.tabs(["Overview", "Zones & routes", "Peak hours", "Payments & fares"])

# ---------------- overview ----------------
with t1:
    st.plotly_chart(px.line(res["by_date"], x="date", y="trips", markers=True, title="Trips per day",
                            labels={"date": "Date", "trips": "Trips"}))
    a, b = st.columns(2)
    a.plotly_chart(px.bar(res["by_hour"], x="hour", y="trips", title="Trips by hour of day",
                          labels={"hour": "Hour (0-23)", "trips": "Trips"}))
    b.plotly_chart(px.line(res["by_date"], x="date", y="revenue", title="Revenue per day (₹)",
                           labels={"date": "Date", "revenue": "₹"}))

# ---------------- zones & routes ----------------
with t2:
    top = res["by_zone"].head(10)
    a, b = st.columns(2)
    a.plotly_chart(px.bar(top, x="trips", y="pickup_zone", orientation="h", title="Top pickup zones (trips)",
                          labels={"pickup_zone": "Zone", "trips": "Trips"}).update_yaxes(autorange="reversed"))
    b.plotly_chart(px.bar(res["routes"], x="trips", y="route", orientation="h", title="Top 10 routes",
                          labels={"route": "Route", "trips": "Trips"}).update_yaxes(autorange="reversed"))
    st.subheader("Zone summary")
    zt = res["by_zone"].copy()
    zt[["revenue", "avg_fare", "avg_distance"]] = zt[["revenue", "avg_fare", "avg_distance"]].round(1)
    st.dataframe(zt, hide_index=True)
    st.subheader("Route summary")
    rt = res["routes"].copy()
    rt[["avg_fare", "avg_distance"]] = rt[["avg_fare", "avg_distance"]].round(1)
    st.dataframe(rt, hide_index=True)

# ---------------- peak hours ----------------
with t3:
    heat = res["heat"]
    pivot = (heat.pivot_table(index="day", columns="hour", values="trips", aggfunc="sum", fill_value=0)
             .reindex(index=DAY_ORDER, columns=range(24), fill_value=0))
    st.plotly_chart(px.imshow(pivot, aspect="auto", title="Demand heatmap: trips by day and hour",
                              labels={"x": "Hour of day", "y": "Day", "color": "Trips"}))
    by_day = heat.groupby("day")["trips"].sum().reindex(DAY_ORDER, fill_value=0).reset_index()
    a, b = st.columns(2)
    a.plotly_chart(px.bar(by_day, x="day", y="trips", title="Trips by day of week",
                          labels={"day": "Day", "trips": "Trips"}))
    peak = res["by_hour"].sort_values("trips", ascending=False).head(3)
    b.subheader("Busiest hours")
    for _, r in peak.iterrows():
        b.write(f"**{int(r['hour']):02d}:00** – {int(r['trips']):,} trips, avg fare ₹{r['avg_fare']:.0f}")

# ---------------- payments & fares ----------------
with t4:
    a, b = st.columns(2)
    a.plotly_chart(px.pie(res["by_payment"], names="payment_type", values="trips", hole=0.45,
                          title="Trips by payment type"))
    b.plotly_chart(px.bar(res["by_payment"], x="payment_type", y="revenue", title="Revenue by payment type (₹)",
                          labels={"payment_type": "Payment", "revenue": "₹"}))
    st.plotly_chart(px.line(res["by_hour"], x="hour", y="avg_fare", markers=True,
                            title="Average fare by hour of day (₹) - look for peak-hour surge",
                            labels={"hour": "Hour (0-23)", "avg_fare": "Avg fare ₹"}))

# ---------------- export ----------------
st.divider()
st.subheader("Download reports")
d1, d2, d3 = st.columns(3)
d1.download_button("⬇️ Zone summary (CSV)", res["by_zone"].to_csv(index=False).encode(), "zone_summary.csv", "text/csv")
d2.download_button("⬇️ Hourly summary (CSV)", res["by_hour"].to_csv(index=False).encode(), "hourly_summary.csv", "text/csv")
d3.download_button("⬇️ Top routes (CSV)", res["routes"].to_csv(index=False).encode(), "top_routes.csv", "text/csv")
