"""E2E claims app (part 6b): customer mode submits claims, admin mode reviews them (see docs/part6b_app_design.md)."""

import hashlib

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


def customer_mode() -> None:
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
    customer_mode()
else:
    admin_mode(pool)
