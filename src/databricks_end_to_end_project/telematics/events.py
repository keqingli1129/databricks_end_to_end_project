"""Fake car telematics events, used by the Kafka simulator."""

import json
import random
from datetime import datetime, timezone

# Rough bounding box around Houston, TX.
LAT_RANGE = (29.5, 30.1)
LON_RANGE = (-95.8, -95.0)
MAX_SPEED_KMH = 180.0

DEFAULT_CHASSIS_NUMBERS = [f"CHS{n:06d}" for n in range(1, 11)]


def generate_event(rng: random.Random, chassis_numbers: list[str], now: datetime) -> dict:
    """Return one telematics reading for a random car."""
    return {
        "chassis_number": rng.choice(chassis_numbers),
        "speed": round(rng.uniform(0, MAX_SPEED_KMH), 1),
        "latitude": round(rng.uniform(*LAT_RANGE), 6),
        "longitude": round(rng.uniform(*LON_RANGE), 6),
        "event_timestamp": now.astimezone(timezone.utc).isoformat(),
    }


def to_json(event: dict) -> bytes:
    """Serialize an event as the Kafka message value."""
    return json.dumps(event).encode("utf-8")
