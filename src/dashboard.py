import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os
import duckdb

# --- CONFIGURATION ---
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")
WAREHOUSE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data/warehouse.duckdb')

st.set_page_config(page_title="Cloud Bill Hunter", page_icon="🛡️", layout="wide")

# --- CUSTOM CSS (FAANG Polish) ---
st.markdown("""
<style>
    /* -------- BASE LAYOUT -------- */
    .main { padding-top: 1rem; }
    
    /* -------- KPI CARD BASE -------- */
    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* -------- DARK MODE ADJUSTMENTS -------- */
    @media (prefers-color-scheme: dark) {
        div[data-testid="stMetric"] {
            background-color: #1e293b;
            border-color: #334155;
        }
    }
    
    /* -------- ALERTS -------- */
    .stAlert {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# --- HELPER: READ FROM WAREHOUSE ---
def get_warehouse_data():
    """
    Connects to the Gold Layer in Read-Only mode.
    Allows dashboard to read data even if the Watcher is active.
    """
    if not os.path.exists(WAREHOUSE_PATH):
        return None
    
    try:
        # read_only=True is CRITICAL to prevent locking conflicts
        con = duckdb.connect(database=WAREHOUSE_PATH, read_only=True)
        
        # Check tables safely
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        
        if 'gold_zombie_report' in tables:
            df = con.execute("SELECT * FROM gold_zombie_report").df()
            con.close()
            return df
        else:
            con.close()
            return None
    except Exception as e:
        # Fail silently/gracefully so UI doesn't crash
        return None

# --- SIDEBAR NAVIGATION ---
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/amazon-web-services.png", width=60)
    st.title("Cloud Bill Hunter")
    st.caption("v2.2.0 Enterprise Edition")
    
    st.markdown("---")
    page = st.radio("Navigation", ["Dashboard", "Upload Data", "API Status"])
    st.markdown("---")
    
    st.info("💡 **Tip:** Exports are CSV compatible with Excel/Tableau.")

# ==========================================
# PAGE 1: UPLOAD DATA (Manual Ingestion)
# ==========================================
if page == "Upload Data":
    st.header("📂 Data Ingestion")
    st.markdown("Upload your AWS Cost & Usage Report (CUR) to trigger the anomaly detection engine.")
    
    uploaded_file = st.file_uploader("Drop CSV Here", type="csv")
    
    if uploaded_file and st.button("🚀 Run Analysis"):
        with st.spinner("Processing in Microservice..."):
            try:
                files = {"file": (uploaded_file.name, uploaded_file, "text/csv")}
                response = requests.post(f"{API_URL}/analyze/upload", files=files)
                
                if response.status_code == 200:
                    data = response.json()
                    st.session_state['data'] = data # Save to session
                    st.success(f"Success! Processed {uploaded_file.name}")
                    st.balloons()
                else:
                    st.error(f"Error: {response.text}")
            except Exception as e:
                st.error(f"Connection Failed: {e}")

# ==========================================
# PAGE 2: DASHBOARD (The "Money" View)
# ==========================================
elif page == "Dashboard":
    st.header("📊 FinOps Observability")
    st.caption("Cloud cost anomalies detected from AWS billing and usage data")

    # --- DATA RESOLUTION STRATEGY ---
    # 1. Try Warehouse (Event-Driven)
    df_zombies = get_warehouse_data()
    source_type = "Event-Driven (Warehouse)"

    # 2. Fallback to Session State (Manual Upload)
    if df_zombies is None or df_zombies.empty:
        if 'data' in st.session_state:
            df_zombies = pd.DataFrame(st.session_state['data']['details'])
            source_type = "Manual Upload (Session)"

    # 3. Empty State Handler
    if df_zombies is None or df_zombies.empty:
        st.info("⏳ Waiting for data... Drop a file in `data/landing_zone` or use the Upload tab.")
        st.stop()

    # --- DATA NORMALIZATION ---
    # Handle API JSON vs SQL Column naming differences
    cost_col = "total_wasted_cost" if "total_wasted_cost" in df_zombies.columns else "wasted_cost"
    owner_col = "owner_team" if "owner_team" in df_zombies.columns else "owner"

    # Ensure numeric types
    df_zombies[cost_col] = pd.to_numeric(df_zombies[cost_col], errors="coerce").fillna(0)

    # --- CALCULATE METRICS ---
    total_monthly_waste = df_zombies[cost_col].sum()
    annualized_savings = total_monthly_waste * 12
    zombie_count = len(df_zombies)

    # --- KPI ROW ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Zombie Resources", zombie_count, delta="High Priority", delta_color="inverse")
    with col2:
        st.metric("Wasted Spend (Mo)", f"${total_monthly_waste:,.2f}", delta="-100%", delta_color="inverse")
    with col3:
        st.metric("Proj. Annual Savings", f"${annualized_savings:,.2f}", delta="ROI")
    with col4:
        st.metric("Data Source", source_type, delta="Active")

    st.markdown("---")

    # --- TABS FOR DETAIL ---
    tab1, tab2 = st.tabs(["🔥 Actionable Kill List", "📉 Waste Distribution"])

    with tab1:
        st.subheader("Top Cost Offenders")
        st.caption("Ranked by highest monthly waste — fix these first")

        df_ranked = df_zombies.sort_values(by=cost_col, ascending=False).reset_index(drop=True)

        st.dataframe(
            df_ranked,
            width='stretch',
            column_config={
                cost_col: st.column_config.NumberColumn("Monthly Waste ($)", format="$%.2f"),
                owner_col: st.column_config.TextColumn("Owner / Team"),
                "service": "AWS Service",
                "resource_id": "Resource ID",
            }
        )

        # Download Utility
        csv = df_ranked.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download Audit Report (CSV)",
            data=csv,
            file_name="cloud_zombie_audit.csv",
            mime="text/csv",
        )

    with tab2:
        st.subheader("Where Is the Money Leaking?")
        st.caption("Hierarchical breakdown by Team -> Service -> Resource")

        if zombie_count > 0:
            fig = px.sunburst(
                df_ranked,
                path=[owner_col, "service", 'resource_id'],
                values=cost_col,
                color=cost_col,
                color_continuous_scale="RdBu_r",
                title="Cost Leakage Hierarchy"
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.success("No anomalies to visualize!")

# ==========================================
# PAGE 3: API STATUS
# ==========================================
elif page == "API Status":
    st.header("🔌 System Health")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Backend Connection")
        try:
            r = requests.get(f"{API_URL}/")
            if r.status_code == 200:
                st.success(f"Online: {API_URL}")
                st.json(r.json())
            else:
                st.error(f"Error: {r.status_code}")
        except Exception as e:
            st.error(f"Offline: {e}")
            
    with col2:
        st.markdown("### Warehouse Status")
        if os.path.exists(WAREHOUSE_PATH):
            st.success(f"Found: {WAREHOUSE_PATH}")
            try:
                con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
                tables = con.execute("SHOW TABLES").fetchall()
                st.write("Tables found:", [t[0] for t in tables])
                con.close()
            except Exception as e:
                st.error(f"Read Error: {e}")
        else:
            st.warning("Not Found (Pipeline hasn't run yet)")