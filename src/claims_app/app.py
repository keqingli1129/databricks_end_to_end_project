"""E2E claims app (part 6b): customer mode submits claims, admin mode reviews them (see docs/part6b_app_design.md)."""

import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

import claim_rules
import db
import services
import streamlit as st

st.set_page_config(page_title="E2E Claims", page_icon="🚗", layout="wide")


@st.cache_resource
def get_pool():
    pool = db.make_pool()
    try:
        db.init_app_tables(pool)
    except Exception:
        pool.close()  # failures aren't cached, so an open pool would keep retrying next to the next one
        raise
    return pool


def predicted_damage_for(image: bytes) -> str | None:
    """Call the model once per photo: Streamlit reruns the script on every click."""
    key = "prediction_" + hashlib.sha256(image).hexdigest()
    if key not in st.session_state:
        with st.spinner("Asking the damage model... (the first call after a while can take about a minute)"):
            st.session_state[key] = services.predict_damage(image)
    return st.session_state[key]


INCIDENT_TYPES = ["COLLISION", "THEFT", "WEATHER", "VANDALISM", "GLASS"]
SEVERITIES = list(claim_rules.EXPECTED_DAMAGE)  # Trivial Damage < Minor Damage < Major Damage < Total Loss
COLLISION_TYPES = ["Single vehicle", "Multi-vehicle", "Parked car", "Not a collision"]
CHECK_ICONS = {True: "✅", False: "❌", None: "➖"}
LOCAL_TZ = ZoneInfo("America/Chicago")  # the claims are in Houston; the app server runs on UTC


def customer_mode(pool) -> None:
    st.subheader("Submit a claim")
    upload = st.file_uploader("Photo of the damage", type=["jpg", "jpeg", "png"])
    if upload is None:
        st.info("Upload a photo of the damage to start.")
        return
    image = upload.getvalue()
    photo, prediction = st.columns([1, 2])
    photo.image(image, width="stretch")
    damage = predicted_damage_for(image)
    if damage is None:
        prediction.warning("The damage model didn't answer, so the photo check will be skipped.")
    else:
        prediction.metric("Damage according to the model", damage.upper())

    with st.form("claim"):
        left, right = st.columns(2)
        policy_no = left.text_input("Policy number", placeholder="POL0000003").strip().upper()
        today = datetime.now(LOCAL_TZ).date()
        incident_date = right.date_input("Incident date", value=today, max_value=today)
        incident_type = left.selectbox("Incident type", INCIDENT_TYPES)
        incident_severity = right.selectbox("Your assessment of the damage", SEVERITIES)
        claim_amount = left.number_input("Claim amount (USD)", min_value=0.0, step=100.0, format="%.2f")
        location = right.text_input("Location", placeholder="Houston")
        collision_type = left.selectbox("Collision type", COLLISION_TYPES)
        vehicles_involved = right.number_input("Vehicles involved", min_value=1, max_value=20, value=1)
        notes = st.text_area("Notes")
        submitted = st.form_submit_button("Submit claim", type="primary")
    if not submitted:
        return
    if not policy_no:
        st.error("Enter your policy number.")
        return

    policy = db.get_policy(pool, policy_no)
    if policy is None:
        st.error(f"Unknown policy number {policy_no}. Nothing was saved.")
        return
    result = claim_rules.check_claim(policy, incident_date, incident_severity, claim_amount, damage)
    claim_no = db.next_claim_no(pool)
    extension = "png" if upload.name.lower().endswith(".png") else "jpg"
    try:
        image_name = services.save_photo(claim_no, image, extension)  # the photo first: never a claim without one
    except Exception as error:  # noqa: BLE001 - tell the customer, save nothing
        st.error(f"The photo couldn't be stored, so the claim wasn't submitted: {error}")
        return
    db.insert_app_claim(
        pool,
        {
            **{key: policy[key] for key in ["policy_no", "customer_id", "full_name", "coverage", "coverage_limit"]},
            **{key: policy[key] for key in ["start_date", "end_date", "chassis_number", "max_speed"]},
            **{key: result[key] for key in [*claim_rules.CHECKS, "expected_damage", "failed_checks", "claim_status"]},
            "claim_no": claim_no,
            "incident_date": incident_date,
            "incident_type": incident_type,
            "incident_severity": incident_severity,
            "claim_amount": claim_amount,
            "image_name": image_name,
            "predicted_damage": damage,
            "location": location or None,
            "collision_type": collision_type,
            "vehicles_involved": vehicles_involved,
            "notes": notes or None,
        },
    )
    show_result(claim_no, policy, result, incident_date, incident_severity, claim_amount, damage)


def show_result(claim_no, policy, result, incident_date, incident_severity, claim_amount, damage) -> None:
    if result["claim_status"] == "auto_approved":
        st.success(f"Approved: claim {claim_no}. Your refund arrives within 3 to 5 business days.")
    else:
        st.warning(f"Claim {claim_no} needs a review by our team: {', '.join(result['failed_checks'])}.")
    speed = policy["max_speed"]
    explanations = {
        "severity_match": f"Your assessment {incident_severity} (= {result['expected_damage']}) "
        + (f"vs the photo: {damage}" if damage else "- the photo couldn't be checked"),
        # \$: Streamlit markdown reads text between two $ signs as a math formula.
        "amount_within_limit": f"\\${claim_amount:,.2f} vs the {policy['coverage']} limit of "
        f"\\${policy['coverage_limit']:,}",
        "policy_valid": f"{incident_date} vs the policy period {policy['start_date']} to {policy['end_date']}",
        "speed_ok": f"top speed {speed:.1f} km/h vs the {claim_rules.MAX_SPEED_KMH} km/h limit"
        if speed is not None
        else "no telematics for this car",
    }
    for check in claim_rules.CHECKS:
        st.write(f"{CHECK_ICONS[result[check]]} **{check}**: {explanations[check]}")


def admin_mode(pool) -> None:
    try:
        counts = db.table_counts(pool)
    except Exception as error:  # noqa: BLE001 - show any connection or permission problem on the page
        st.error(f"Can't read the claims database: {error}")
        st.code("\n".join(db.diagnose()))
        return
    left, middle, right = st.columns(3)
    left.metric("Claims (synced)", f"{counts['claim_checks']:,}")
    middle.metric("Policies (synced)", f"{counts['policy_lookup']:,}")
    right.metric("Claims submitted in the app", f"{counts['app_claims']:,}")


mode = st.sidebar.radio("Mode", ["Customer", "Admin"])
st.title("E2E Claims")
st.caption(f"{mode} mode")

try:
    pool = get_pool()
except Exception as error:  # noqa: BLE001 - show any connection or permission problem on the page
    st.error(f"Can't connect to the claims database: {error}")
    st.code("\n".join(db.diagnose()))
    st.stop()

if mode == "Customer":
    customer_mode(pool)
else:
    admin_mode(pool)
