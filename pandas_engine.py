"""Pandas fallback engine - used ONLY if Spark/Java cannot start on the host."""
import pandas as pd

NAME = "Pandas (fallback - Spark unavailable)"
REQUIRED = ["trip_id", "pickup_datetime", "dropoff_datetime", "pickup_zone", "dropoff_zone",
            "passenger_count", "distance_km", "fare_amount", "payment_type"]
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def load_and_clean(path):
    raw = pd.read_csv(path, dtype=str)
    raw.columns = [c.strip().lower() for c in raw.columns]
    missing = [c for c in REQUIRED if c not in raw.columns]
    if missing:
        raise ValueError("CSV is missing columns: " + ", ".join(missing))
    rows_in = len(raw)
    df = pd.DataFrame({
        "trip_id": raw["trip_id"],
        "pickup_ts": pd.to_datetime(raw["pickup_datetime"].str.strip(), errors="coerce"),
        "dropoff_ts": pd.to_datetime(raw["dropoff_datetime"].str.strip(), errors="coerce"),
        "pickup_zone": raw["pickup_zone"].fillna("").str.strip().str.title(),
        "dropoff_zone": raw["dropoff_zone"].fillna("").str.strip().str.title(),
        "passenger_count": pd.to_numeric(raw["passenger_count"], errors="coerce"),
        "distance_km": pd.to_numeric(raw["distance_km"], errors="coerce"),
        "fare_amount": pd.to_numeric(raw["fare_amount"], errors="coerce"),
        "payment_type": raw["payment_type"].fillna("").str.strip(),
    })
    df["duration_min"] = (df["dropoff_ts"] - df["pickup_ts"]).dt.total_seconds() / 60.0
    df["speed_kmph"] = df["distance_km"] / df["duration_min"] * 60.0
    df = df[df["pickup_ts"].notna() & df["dropoff_ts"].notna()
            & (df["pickup_zone"] != "") & (df["dropoff_zone"] != "") & (df["payment_type"] != "")
            & df["passenger_count"].between(1, 6)
            & (df["distance_km"] > 0) & (df["distance_km"] <= 200)
            & (df["fare_amount"] > 0) & (df["fare_amount"] <= 50000)
            & (df["duration_min"] >= 1) & (df["duration_min"] <= 300)
            & (df["speed_kmph"] <= 120)].copy()
    df["date"] = df["pickup_ts"].dt.date
    df["hour"] = df["pickup_ts"].dt.hour
    df["day"] = df["pickup_ts"].dt.dayofweek.map(dict(enumerate(DAYS)))
    return df.reset_index(drop=True), {"rows_in": rows_in, "rows_out": len(df), "rows_dropped": rows_in - len(df)}


def get_options(df):
    return (sorted(df["pickup_zone"].unique()), sorted(df["payment_type"].unique()),
            df["date"].min(), df["date"].max())


def analyze(df, start, end, zones, pays, hmin, hmax):
    f = df[(df["date"] >= start) & (df["date"] <= end) & df["pickup_zone"].isin(zones)
           & df["payment_type"].isin(pays) & df["hour"].between(hmin, hmax)]
    trips = len(f)
    kpi = {"trips": trips,
           "revenue": float(f["fare_amount"].sum()),
           "avg_fare": float(f["fare_amount"].mean()) if trips else 0.0,
           "avg_distance": float(f["distance_km"].mean()) if trips else 0.0,
           "avg_duration": float(f["duration_min"].mean()) if trips else 0.0,
           "avg_speed": float(f["distance_km"].sum() / f["duration_min"].sum() * 60) if trips else 0.0}
    by_hour = (f.groupby("hour").agg(trips=("trip_id", "count"), revenue=("fare_amount", "sum"),
                                     avg_fare=("fare_amount", "mean")).reset_index())
    by_zone = (f.groupby("pickup_zone").agg(trips=("trip_id", "count"), revenue=("fare_amount", "sum"),
                                            avg_fare=("fare_amount", "mean"), avg_distance=("distance_km", "mean"))
               .reset_index().sort_values("trips", ascending=False))
    routes = (f.groupby(["pickup_zone", "dropoff_zone"]).agg(trips=("trip_id", "count"), avg_fare=("fare_amount", "mean"),
                                                             avg_distance=("distance_km", "mean"))
              .reset_index().sort_values("trips", ascending=False).head(10))
    routes["route"] = routes["pickup_zone"] + " → " + routes["dropoff_zone"]
    by_payment = (f.groupby("payment_type").agg(trips=("trip_id", "count"), revenue=("fare_amount", "sum"))
                  .reset_index().sort_values("trips", ascending=False))
    by_date = f.groupby("date").agg(trips=("trip_id", "count"), revenue=("fare_amount", "sum")).reset_index().sort_values("date")
    heat = f.groupby(["day", "hour"]).size().reset_index(name="trips")
    return {"kpi": kpi, "by_hour": by_hour, "by_zone": by_zone, "routes": routes[["route", "trips", "avg_fare", "avg_distance"]],
            "by_payment": by_payment, "by_date": by_date, "heat": heat}
