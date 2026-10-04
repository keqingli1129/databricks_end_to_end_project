"""E2E claims app (part 6b): customer mode submits claims, admin mode reviews them (see docs/part6b_app_design.md)."""

import db
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


mode = st.sidebar.radio("Mode", ["Customer", "Admin"])
st.title("E2E Claims")
st.caption(f"{mode} mode")

try:
    counts = db.table_counts(get_pool())
except Exception as error:  # noqa: BLE001 - show any connection or permission problem on the page
    st.error(f"Can't read the claims database: {error}")
    st.code("\n".join(db.diagnose()))
    st.stop()

left, middle, right = st.columns(3)
left.metric("Claims (synced)", f"{counts['claim_checks']:,}")
middle.metric("Policies (synced)", f"{counts['policy_lookup']:,}")
right.metric("Claims submitted in the app", f"{counts['app_claims']:,}")
