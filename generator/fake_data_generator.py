# This script generates fake ride events for Maputo's transport network.


import random
import uuid
from datetime import datetime, timedelta
import json

# ── Real Maputo routes (chapa + bus) ──────────────────────────────────────────
ROUTES = [
    {"route_id": "R01", "name": "Museu → Baixa",         "type": "chapa", "base_fare": 15, "distance_km": 4.2},
    {"route_id": "R02", "name": "Matola → Junta",         "type": "chapa", "base_fare": 20, "distance_km": 12.5},
    {"route_id": "R03", "name": "Xipamanine → Baixa",    "type": "chapa", "base_fare": 12, "distance_km": 3.8},
    {"route_id": "R04", "name": "Costa do Sol → Baixa",  "type": "bus",   "base_fare": 10, "distance_km": 7.1},
    {"route_id": "R05", "name": "Julius Nyerere → Junta","type": "chapa", "base_fare": 17, "distance_km": 5.6},
    {"route_id": "R06", "name": "Benfica → Museu",        "type": "chapa", "base_fare": 22, "distance_km": 9.3},
    {"route_id": "R07", "name": "Maputo Central → Laulane","type":"bus",  "base_fare": 13, "distance_km": 8.0},
    {"route_id": "R08", "name": "Zimpeto → Junta",        "type": "chapa", "base_fare": 25, "distance_km": 14.2},
]

PAYMENT_METHODS = ["cash", "cash", "cash", "mpesa", "emola"]  # cash is dominant
VEHICLE_CONDITIONS = ["good", "good", "fair", "poor"]
PEAK_HOURS = [6, 7, 8, 12, 17, 18, 19]  # rush hours in Maputo


def get_congestion_multiplier(hour: int) -> float:
    """Fare goes up slightly during peak hours (driver behaviour)."""
    return 1.3 if hour in PEAK_HOURS else 1.0


def generate_ride_event(timestamp: datetime = None) -> dict:
    """Generate a single realistic ride event."""
    if timestamp is None:
        timestamp = datetime.utcnow()

    route = random.choice(ROUTES)
    hour = timestamp.hour
    congestion = get_congestion_multiplier(hour)

    # Passengers: chapas hold ~15, buses ~40
    max_capacity = 15 if route["type"] == "chapa" else 40
    passengers = random.randint(1, max_capacity)
    occupancy_pct = round((passengers / max_capacity) * 100, 1)

    # Fare: base + peak multiplier + slight randomness (driver discretion)
    fare_per_person = round(route["base_fare"] * congestion * random.uniform(0.95, 1.15))
    total_revenue = fare_per_person * passengers

    # Trip duration: distance / avg speed (lower during peak)
    avg_speed_kmh = random.uniform(15, 25) if hour in PEAK_HOURS else random.uniform(25, 40)
    duration_minutes = round((route["distance_km"] / avg_speed_kmh) * 60)

    return {
        "event_id":           str(uuid.uuid4()),
        "timestamp":          timestamp.isoformat() + "Z",
        "route_id":           route["route_id"],
        "route_name":         route["name"],
        "vehicle_type":       route["type"],
        "driver_id":          f"DRV-{random.randint(100, 999)}",
        "passengers":         passengers,
        "max_capacity":       max_capacity,
        "occupancy_pct":      occupancy_pct,
        "fare_per_person_mzn": fare_per_person,
        "total_revenue_mzn":  total_revenue,
        "payment_method":     random.choice(PAYMENT_METHODS),
        "duration_minutes":   duration_minutes,
        "distance_km":        route["distance_km"],
        "is_peak_hour":       hour in PEAK_HOURS,
        "vehicle_condition":  random.choice(VEHICLE_CONDITIONS),
        "hour_of_day":        hour,
        "day_of_week":        timestamp.strftime("%A"),
    }


def generate_batch(n: int = 10, start_time: datetime = None) -> list:
    """Generate a batch of n ride events spread over a time window."""
    if start_time is None:
        start_time = datetime.utcnow()

    events = []
    for i in range(n):
        # Spread events randomly within a 5-minute window
        jitter = timedelta(seconds=random.randint(0, 300))
        events.append(generate_ride_event(start_time + jitter))

    return sorted(events, key=lambda x: x["timestamp"])


if __name__ == "__main__":
    # Quick preview — print 5 sample events
    print("=== Maputo Transport — Sample Events ===\n")
    for event in generate_batch(5):
        print(json.dumps(event, indent=2))
        print("---")