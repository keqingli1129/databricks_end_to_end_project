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
MAX_ROWS = 500  # Streamlit gets slow rendering all ~13,000 claims
LIST_COLUMNS = [
    "claim_no", "source", "claim_status", "failed_checks", "full_name", "incident_date", "incident_type",
    "incident_severity", "predicted_damage", "claim_amount", "coverage", "coverage_limit", "submitted_at",
]  # fmt: skip
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
    claim = {
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
    }
    db.insert_app_claim(pool, claim)
    if claim["claim_status"] == "auto_approved":
        st.success(f"Approved: claim {claim_no}. Your refund arrives within 3 to 5 business days.")
    else:
        st.warning(f"Claim {claim_no} needs a review by our team: {', '.join(claim['failed_checks'])}.")
    show_checks(claim)


def show_checks(claim: dict) -> None:
    """Each check with its icon and the numbers behind it; claim is a row of claim_checks or app_claims."""
    speed, damage = claim["max_speed"], claim["predicted_damage"]
    explanations = {
        "severity_match": f"claimed {claim['incident_severity']} (= {claim['expected_damage']}) "
        + (f"vs the photo: {damage}" if damage else "- the photo couldn't be checked"),
        # \$: Streamlit markdown reads text between two $ signs as a math formula.
        "amount_within_limit": f"\\${claim['claim_amount']:,.2f} vs the {claim['coverage']} limit of "
        f"\\${claim['coverage_limit']:,}",
        "policy_valid": f"{claim['incident_date']} vs the policy period {claim['start_date']} to {claim['end_date']}",
        "speed_ok": f"top speed {speed:.1f} km/h vs the {claim_rules.MAX_SPEED_KMH} km/h limit"
        if speed is not None
        else "no telematics for this car",
    }
    for check in claim_rules.CHECKS:
        st.write(f"{CHECK_ICONS[claim[check]]} **{check}**: {explanations[check]}")


def admin_mode(pool) -> None:
    overview, analysis = st.tabs(["Overview", "Analysis"])
    with overview:
        admin_overview(pool)
    with analysis:
        admin_analysis(pool)


def admin_overview(pool) -> None:
    summary = db.claim_summary(pool)
    total, approved, review = st.columns(3)
    total.metric("Total claims", f"{summary['total']:,}", help=f"{summary['from_app']:,} submitted in the app")
    approved.metric("Auto-approved", f"{summary['auto_approved']:,}")
    review.metric("Needs review", f"{summary['needs_review']:,}")
    for column, severity in zip(st.columns(len(SEVERITIES)), SEVERITIES, strict=True):
        column.metric(severity, f"{summary['per_severity'].get(severity, 0):,}")

    severity_filter, status_filter, source_filter = st.columns(3)
    severity = severity_filter.selectbox("Claimed severity", ["All", *SEVERITIES])
    status = status_filter.selectbox("Status", ["All", "needs_review", "auto_approved"])
    source = source_filter.selectbox("Source", ["All", "app", "pipeline"])
    rows = db.list_claims(
        pool, *[None if choice == "All" else choice for choice in (severity, status, source)], limit=MAX_ROWS
    )
    st.caption(f"{len(rows):,} claims" + (f" (the first {MAX_ROWS})" if len(rows) == MAX_ROWS else ""))
    st.dataframe(
        [{**row, "failed_checks": ", ".join(row["failed_checks"])} for row in rows],
        hide_index=True,
        width="stretch",
        column_order=LIST_COLUMNS,
        column_config={
            "claim_amount": st.column_config.NumberColumn("claim_amount", format="dollar"),
            "coverage_limit": st.column_config.NumberColumn("coverage_limit", format="dollar"),
        },
    )


def admin_analysis(pool) -> None:
    claim_no = st.text_input("Claim number", value=db.latest_app_claim_no(pool) or "CLM00012832").strip().upper()
    claim = db.get_claim(pool, claim_no)
    if claim is None:
        st.warning(f"No claim {claim_no}.")
        return
    photo_column, details = st.columns([1, 2])
    photo = services.read_photo(claim["image_name"]) if claim["image_name"] else None
    if photo is None:
        photo_column.info("No photo found for this claim.")
    else:
        photo_column.image(photo, caption=claim["image_name"], width="stretch")
    with details:
        approved = claim["claim_status"] == "auto_approved"
        (st.success if approved else st.warning)(f"{claim_no}: {claim['claim_status']} ({claim['source']} claim)")
        st.write(
            f"**{claim['full_name']}** ({claim['customer_id']}), policy {claim['policy_no']}, "
            f"{claim['coverage']}, car {claim['chassis_number']}"
        )
        st.write(f"{claim['incident_type']} on {claim['incident_date']}")
        show_checks(claim)


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
