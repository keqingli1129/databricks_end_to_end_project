"""The claim rules (part 6): the single copy, used by the pipeline (gold.claim_checks, gold.policy_lookup) and the app.

Pure Python on purpose: the pipeline imports the constants (its root_path is src/), and the app, which deploys only
this folder, imports check_claim. A check is None when it can't run (no prediction, no telematics), which is not
a failure - the same as the NULLs in gold.claim_checks.
"""

from datetime import date

EXPECTED_DAMAGE = {"Trivial Damage": "ok", "Minor Damage": "minor", "Major Damage": "major", "Total Loss": "major"}
COVERAGE_LIMITS = {"COMPREHENSIVE": 20000, "COLLISION": 15000, "LIABILITY": 10000}
MAX_SPEED_KMH = 150
CHECKS = ["severity_match", "amount_within_limit", "policy_valid", "speed_ok"]


def check_claim(
    policy: dict, incident_date: date, incident_severity: str, claim_amount: float, predicted_damage: str | None
) -> dict:
    """Run the four checks on one claim.

    policy: a gold.policy_lookup row (coverage_limit, start_date, end_date, max_speed).
    Returns the four checks (True / False / None), expected_damage, failed_checks and claim_status.
    """
    expected = EXPECTED_DAMAGE.get(incident_severity)
    limit = policy["coverage_limit"]
    max_speed = policy["max_speed"]
    checks = {
        "severity_match": None if expected is None or predicted_damage is None else expected == predicted_damage,
        "amount_within_limit": None if limit is None else claim_amount <= limit,
        "policy_valid": policy["start_date"] <= incident_date <= policy["end_date"],
        "speed_ok": None if max_speed is None else max_speed <= MAX_SPEED_KMH,
    }
    failed = [name for name in CHECKS if checks[name] is False]
    return {
        **checks,
        "expected_damage": expected,
        "failed_checks": failed,
        "claim_status": "needs_review" if failed else "auto_approved",
    }
