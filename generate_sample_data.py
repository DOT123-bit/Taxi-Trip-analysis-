"""Generates taxi_trips.csv (SAMPLE data, not real ride-hailing records).
Run:  python generate_sample_data.py [number_of_rows]
Default is 25,000 trips. Use a bigger number (e.g. 500000) on a laptop to demo 'big data'.
"""
import csv
import math
import random
import sys
from datetime import datetime, timedelta

random.seed(11)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 25000

# zone: (x_km, y_km, popularity)  - rough Mumbai layout, only used to make distances believable
ZONES = {
    "Colaba": (0, 0, 2), "Fort": (1, 2, 3), "Worli": (0, 9, 2), "Dadar": (1, 12, 3),
    "Bandra": (0, 17, 4), "Juhu": (-2, 21, 2), "Airport": (1, 22, 3), "Andheri": (-1, 24, 5),
    "Powai": (4, 26, 2), "Borivali": (-1, 33, 2), "Vashi": (12, 30, 1.5), "Thane": (8, 36, 1.5),
}
NAMES = list(ZONES)
POP = [ZONES[z][2] for z in NAMES]
PEAK = set(range(8, 11)) | set(range(17, 21))
HOUR_W = [1, 1, 1, 1, 1, 2, 3, 5, 8, 8, 6, 5, 5, 5, 5, 5, 6, 8, 9, 9, 7, 5, 3, 2]
PAYMENTS = ["UPI", "Cash", "Card", "Wallet"]
PAY_W = [45, 30, 17, 8]
START = datetime(2026, 9, 1)
DAYS = 56

rows = []
for _ in range(N):
    day = START + timedelta(days=random.randrange(DAYS))
    hour = random.choices(range(24), weights=HOUR_W)[0]
    pick_w = [w * (2.5 if (z == "Airport" and (hour >= 22 or hour <= 5)) else 1) for z, w in zip(NAMES, POP)]
    pz = random.choices(NAMES, weights=pick_w)[0]
    dz = random.choices(NAMES, weights=POP)[0]
    (x1, y1, _), (x2, y2, _) = ZONES[pz], ZONES[dz]
    if pz == dz:
        km = random.uniform(1.0, 5.0)
    else:
        km = max(1.5, math.hypot(x2 - x1, y2 - y1) * 1.35 + random.uniform(-1, 2))
    if hour in PEAK:
        speed = random.uniform(13, 21)
    elif hour >= 23 or hour <= 5:
        speed = random.uniform(28, 38)
    else:
        speed = random.uniform(21, 30)
    minutes = km / speed * 60 + random.uniform(2, 6)
    fare = (50 + 15 * km + 1.2 * minutes) * (1.25 if hour in PEAK else 1.0)
    pickup = day.replace(hour=hour, minute=random.randrange(60), second=random.randrange(60))
    drop = pickup + timedelta(minutes=minutes)
    pax = random.choices([1, 2, 3, 4, 5, 6], weights=[60, 25, 8, 5, 1.5, 0.5])[0]
    rows.append([pickup, drop, pz, dz, pax, round(km, 2), round(fare, 1), random.choices(PAYMENTS, weights=PAY_W)[0]])

rows.sort(key=lambda r: r[0])
fmt = "%Y-%m-%d %H:%M:%S"
with open("taxi_trips.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["trip_id", "pickup_datetime", "dropoff_datetime", "pickup_zone", "dropoff_zone",
                "passenger_count", "distance_km", "fare_amount", "payment_type"])
    for i, r in enumerate(rows, 1):
        w.writerow([f"TR{i:07d}", r[0].strftime(fmt), r[1].strftime(fmt), r[2], r[3], r[4], r[5], r[6], r[7]])
    # deliberately bad rows so the cleaning step has something to do
    w.writerow(["TRX00001", "2026-10-05 10:00:00", "2026-10-05 10:25:00", "Andheri", "Bandra", 2, 8.5, -120, "UPI"])         # negative fare
    w.writerow(["TRX00002", "2026-10-05 11:00:00", "2026-10-05 11:20:00", "Dadar", "Worli", 1, 0, 90, "Cash"])              # zero distance
    w.writerow(["TRX00003", "2026-10-05 12:30:00", "2026-10-05 12:10:00", "Powai", "Andheri", 1, 6.2, 160, "Card"])         # drop before pickup
    w.writerow(["TRX00004", "2026-10-06 09:00:00", "2026-10-06 09:30:00", "", "Dadar", 1, 7.0, 140, "UPI"])                # missing zone
    w.writerow(["TRX00005", "not-a-time", "2026-10-06 10:30:00", "Bandra", "Juhu", 1, 5.0, 110, "Cash"])                   # invalid date
    w.writerow(["TRX00006", "2026-10-07 08:00:00", "2026-10-07 08:20:00", "Colaba", "Fort", 0, 3.0, 100, "UPI"])           # 0 passengers
    w.writerow(["TRX00007", "2026-10-07 14:00:00", "2026-10-07 14:05:00", "Thane", "Colaba", 2, 150, 900, "Card"])         # impossible speed
    w.writerow(["TRX00008", "2026-10-08 18:00:00", "2026-10-08 18:20:00", "Vashi", "Dadar", 2, "abc", 300, "Wallet"])      # invalid distance
print(f"taxi_trips.csv written with {len(rows) + 8} rows")
