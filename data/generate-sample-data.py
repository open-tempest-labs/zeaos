#!/usr/bin/env python3
"""
Generate sample Home Assistant energy sensor CSV files for the ZeaOS homelab tutorial.

Produces two files:
  home_assistant_2025_01.csv  — January 1, 2025 (3,600 rows)
  home_assistant_2025_02.csv  — January 2, 2025 (3,600 rows)

Each file simulates 5 energy sensors at 2-minute intervals over 24 hours,
matching the schema that Home Assistant uses for its history exports.
"""

import csv
import math
import os
import random

SENSORS = [
    "sensor.energy_total",
    "sensor.solar_production",
    "sensor.hvac_energy",
    "sensor.kitchen_energy",
    "sensor.washer_energy",
]

INTERVAL_MINUTES = 2
READINGS_PER_DAY = (24 * 60) // INTERVAL_MINUTES  # 720 per sensor


def solar_output(hour_fraction):
    """Rough bell curve peaking at solar noon, zero at night."""
    if hour_fraction < 6.5 or hour_fraction > 19.5:
        return 0.0
    peak = math.sin(math.pi * (hour_fraction - 6.5) / 13.0)
    return round(max(0.0, peak * 3.2 + random.uniform(-0.1, 0.1)), 3)


def hvac_draw(hour_fraction, day_seed):
    """HVAC cycles on/off throughout the day, higher in morning and evening."""
    random.seed(int(hour_fraction * 10) + day_seed * 1000)
    base = 0.4
    if 6 <= hour_fraction < 9 or 17 <= hour_fraction < 22:
        base = 1.2
    on = random.random() < 0.6
    return round(base + random.uniform(-0.05, 0.05) if on else 0.0, 3)


def kitchen_draw(hour_fraction, day_seed):
    """Kitchen spikes at breakfast, lunch, and dinner."""
    random.seed(int(hour_fraction * 30) + day_seed * 2000 + 7)
    if 7 <= hour_fraction < 8.5:     # breakfast
        return round(random.uniform(0.3, 1.8), 3)
    if 12 <= hour_fraction < 13.5:   # lunch
        return round(random.uniform(0.2, 1.2), 3)
    if 18 <= hour_fraction < 20:     # dinner
        return round(random.uniform(0.5, 2.5), 3)
    return round(random.uniform(0.0, 0.1), 3)


def washer_draw(hour_fraction, day_seed):
    """Washer runs for ~90 minutes mid-morning on day 1, afternoon on day 2."""
    if day_seed == 1 and 9.5 <= hour_fraction < 11.0:
        return round(random.uniform(0.8, 1.1), 3)
    if day_seed == 2 and 14.0 <= hour_fraction < 15.5:
        return round(random.uniform(0.8, 1.1), 3)
    return 0.0


def generate_day(date_str, day_seed, output_path):
    rows = []
    energy_total = round(random.uniform(12.0, 18.0), 3)  # starting cumulative kWh

    for i in range(READINGS_PER_DAY):
        minute_of_day = i * INTERVAL_MINUTES
        hour_fraction = minute_of_day / 60.0
        hh = minute_of_day // 60
        mm = minute_of_day % 60

        ts = f"{date_str}T{hh:02d}:{mm:02d}:00+00:00"

        random.seed(i + day_seed * 10000)
        solar = solar_output(hour_fraction)
        hvac  = hvac_draw(hour_fraction, day_seed)
        kitchen = kitchen_draw(hour_fraction, day_seed)
        washer  = washer_draw(hour_fraction, day_seed)

        interval_draw = hvac + kitchen + washer - solar * 0.7
        energy_total = round(energy_total + max(0, interval_draw * INTERVAL_MINUTES / 60), 3)

        sensor_values = {
            "sensor.energy_total":    round(energy_total, 3),
            "sensor.solar_production": solar,
            "sensor.hvac_energy":     hvac,
            "sensor.kitchen_energy":  kitchen,
            "sensor.washer_energy":   washer,
        }

        for entity_id in SENSORS:
            rows.append({
                "entity_id":   entity_id,
                "state":       str(sensor_values[entity_id]),
                "last_changed": ts,
                "last_updated": ts,
                "attributes":  "{}",
                "domain":      "sensor",
            })

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["entity_id", "state", "last_changed", "last_updated", "attributes", "domain"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows)} rows → {output_path}")


if __name__ == "__main__":
    out_dir = os.path.dirname(os.path.abspath(__file__))
    generate_day("2025-01-01", day_seed=1, output_path=os.path.join(out_dir, "home_assistant_2025_01.csv"))
    generate_day("2025-01-02", day_seed=2, output_path=os.path.join(out_dir, "home_assistant_2025_02.csv"))
    print("done — load these from /data/ inside the ZeaOS REPL")
