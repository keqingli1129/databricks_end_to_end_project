import json
import random
from datetime import datetime, timezone

from databricks_end_to_end_project.telematics import events

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
CHASSIS = ["CHS000001", "CHS000002"]


def test_generate_event_has_all_fields_in_range():
    event = events.generate_event(random.Random(42), CHASSIS, NOW)

    assert set(event) == {"chassis_number", "speed", "latitude", "longitude", "event_timestamp"}
    assert event["chassis_number"] in CHASSIS
    assert 0 <= event["speed"] <= events.MAX_SPEED_KMH
    assert events.LAT_RANGE[0] <= event["latitude"] <= events.LAT_RANGE[1]
    assert events.LON_RANGE[0] <= event["longitude"] <= events.LON_RANGE[1]
    assert event["event_timestamp"] == "2026-09-27T12:00:00+00:00"


def test_same_seed_gives_same_event():
    first = events.generate_event(random.Random(7), CHASSIS, NOW)
    second = events.generate_event(random.Random(7), CHASSIS, NOW)
    assert first == second


def test_to_json_round_trips():
    event = events.generate_event(random.Random(1), events.DEFAULT_CHASSIS_NUMBERS, NOW)
    assert json.loads(events.to_json(event)) == event
