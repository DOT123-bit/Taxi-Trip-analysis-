# Taxi / Ride Trip Analytics (Apache Spark + Streamlit)

Batch big-data analytics: PySpark reads and cleans ride-trip records, then computes demand by
hour/day, busiest zones and routes, fares, revenue and payment mix. Streamlit shows it as a
web dashboard with filters, CSV upload and report downloads.

**Data:** `taxi_trips.csv` is generated SAMPLE data (25,000 trips, `generate_sample_data.py`),
not real ride records. It has 8 intentionally bad rows to demonstrate cleaning.
For a bigger demo run `python generate_sample_data.py 500000` on a computer with Spark.

## Architecture
CSV -> PySpark (clean, derive hour/day/duration/speed, group, aggregate) -> small summary
tables -> Streamlit dashboard

## Files
- `app.py` - Streamlit dashboard
- `spark_engine.py` - PySpark processing (the core)
- `pandas_engine.py` - fallback used only if Spark cannot start on the host
- `generate_sample_data.py` - creates `taxi_trips.csv`
- `requirements.txt`, `packages.txt` (Java for hosting), `Dockerfile` (optional, Render)

## CSV columns
`trip_id, pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, passenger_count,
distance_km, fare_amount, payment_type` (timestamps as `YYYY-MM-DD HH:MM:SS`)

## Cleaning rules
Removes trips with invalid/missing times or zones, passengers outside 1-6, distance <= 0 or
> 200 km, fare <= 0, duration under 1 or over 300 minutes, or impossible speed (> 120 km/h).

## Run locally
    pip install -r requirements.txt   # also needs Java 17+
    streamlit run app.py
