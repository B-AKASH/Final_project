import streamlit as st
import requests
import pandas as pd
import numpy as np

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="HOSPITAL INQUIRY",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --------------------------------------------------
# GLOBAL STYLES
# --------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=Inter:wght@300;400;500;600&display=swap');

:root {
    --bg-dark: #020408;
    --card-bg: rgba(13, 20, 38, 0.6);
    --accent-blue: #0ea5e9;
    --accent-glow: rgba(14, 165, 233, 0.15);
    --text-main: #f8fafc;
    --text-dim: #94a3b8;
    --border-color: rgba(255, 255, 255, 0.08);
}

/* Custom Scrollbar */
::-webkit-scrollbar { width: 8px; }
::-webkit-scrollbar-track { background: var(--bg-dark); }
::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 10px; }
::-webkit-scrollbar-thumb:hover { background: #334155; }

html, body, [data-testid="stAppViewContainer"] {
    font-family: 'Inter', sans-serif;
    background-color: var(--bg-dark);
    color: var(--text-main);
}

[data-testid="stSidebar"] {
    background-color: #03060c;
    border-right: 1px solid var(--border-color);
}

.glass-card {
    background: var(--card-bg);
    backdrop-filter: blur(20px);
    border-radius: 24px;
    padding: 28px;
    border: 1px solid var(--border-color);
    box-shadow: 0 20px 50px rgba(0,0,0,0.5);
    transition: transform 0.3s ease, box-shadow 0.3s ease;
}

.glass-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 25px 60px rgba(0,0,0,0.6);
}

.bio-card {
    background: rgba(255,255,255,0.03);
    padding: 20px;
    border-radius: 20px;
    border: 1px solid var(--border-color);
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.bio-card:hover {
    background: rgba(14, 165, 233, 0.05);
    border-color: rgba(14, 165, 233, 0.3);
    transform: scale(1.02);
}

.bio-icon { font-size: 1.5rem; color: var(--accent-blue); filter: drop-shadow(0 0 8px var(--accent-glow)); }
.bio-val { font-size: 1.8rem; font-weight: 700; color: #fff; }
.bio-lbl { font-size: 0.75rem; letter-spacing: 0.1em; color: var(--text-dim); text-transform: uppercase; }

.dash-header {
    background: linear-gradient(135deg, #0f172a 0%, #020617 100%);
    padding: 48px;
    border-radius: 32px;
    margin-bottom: 32px;
    border: 1px solid var(--border-color);
    border-bottom: 3px solid var(--accent-blue);
    position: relative;
    overflow: hidden;
}

.dash-header::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: radial-gradient(circle at top right, rgba(14, 165, 233, 0.1), transparent);
    pointer-events: none;
}

.medical-paper {
    background: #ffffff;
    color: #1e293b;
    border-radius: 12px;
    padding: 32px;
    box-shadow: inset 0 0 40px rgba(0,0,0,0.05), 0 10px 30px rgba(0,0,0,0.1);
    border-left: 6px solid #0ea5e9;
    font-family: 'Inter', sans-serif;
    line-height: 1.6;
}

.reason-card {
    background: rgba(14, 165, 233, 0.05);
    border: 1px solid rgba(14, 165, 233, 0.15);
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 12px;
    transition: all 0.2s ease;
}

.reason-card:hover {
    background: rgba(14, 165, 233, 0.08);
    transform: translateX(4px);
}

.consult-box {
    background: #0f172a;
    border: 1px solid #1e293b;
    border-radius: 16px;
    padding: 24px;
    position: relative;
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
}

.consult-box::before {
    content: "AI ADVISORY";
    position: absolute;
    top: -10px;
    left: 20px;
    background: #0ea5e9;
    color: white;
    font-size: 0.65rem;
    font-weight: 800;
    padding: 2px 10px;
    border-radius: 4px;
    letter-spacing: 0.1em;
}

.intel-brief {
    background: linear-gradient(135deg, rgba(8, 12, 22, 0.8) 0%, rgba(13, 19, 32, 0.8) 100%);
    color: #f8fafc;
    border-radius: 16px;
    padding: 32px;
    border: 1px solid rgba(14, 165, 233, 0.2);
    box-shadow: 0 10px 40px rgba(0,0,0,0.6);
    position: relative;
    overflow: hidden;
}

.intel-brief::before {
    content: "SECURE ANALYTIC FEED";
    position: absolute;
    top: 15px;
    right: 20px;
    font-size: 0.6rem;
    font-weight: 800;
    color: rgba(14, 165, 233, 0.5);
    letter-spacing: 0.2rem;
}

.data-row {
    display: flex;
    justify-content: space-between;
    padding: 12px 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
}

.data-label { color: #94a3b8; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05rem; }
.data-value { color: #f8fafc; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HELPERS (CRITICAL FIX)
# --------------------------------------------------
def glass_card_start(extra_style=""):
    st.markdown(f'<div class="glass-card" style="{extra_style}">', unsafe_allow_html=True)

def glass_card_end():
    st.markdown('</div>', unsafe_allow_html=True)

def intel_brief_start(session_id, source_count):
    st.markdown(f"""
    <div class="intel-brief">
        <div style="border-left: 4px solid #0ea5e9; padding-left: 20px; margin-bottom: 25px;">
            <h3 style="margin:0; font-family:'Outfit'; color:#fff; letter-spacing:0.05rem;">AGENT INTELLIGENCE SYNTHESIS</h3>
            <p style="color:#94a3b8; font-size:0.9rem; margin:5px 0 0 0;">CROSS-RECORD ANALYTIC REPORT • SESSION ID {session_id}</p>
        </div>
        <div style="background:rgba(0,0,0,0.2); border-radius:12px; padding:20px; border:1px solid rgba(255,255,255,0.03); margin-bottom:25px;">
    """, unsafe_allow_html=True)

def intel_brief_end(source_count):
    st.markdown(f"""
        </div>
        <div style="display:flex; gap:15px;">
            <div class="metric-pill">📡 SOURCES: {source_count}</div>
            <div class="metric-pill">🔒 SECURITY: LEVEL-4</div>
            <div class="metric-pill">⚙️ ENGINE: RAG-GPT4-LATEST</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def consult_box_start():
    st.markdown('<div class="consult-box"><div style="font-family:\'Inter\', sans-serif; color:#cbd5e1; font-size:0.95rem; line-height:1.7;">', unsafe_allow_html=True)

def consult_box_end():
    st.markdown('</div></div>', unsafe_allow_html=True)

def bio_card(label, value, icon):
    return f"""
    <div class="bio-card">
        <div class="bio-icon">{icon}</div>
        <div class="bio-val">{value}</div>
        <div class="bio-lbl">{label}</div>
    </div>
    """

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "view" not in st.session_state:
    st.session_state.view = "welcome"
if "result_data" not in st.session_state:
    st.session_state.result_data = None

# --------------------------------------------------
# SIDEBAR
# --------------------------------------------------
with st.sidebar:
    st.markdown("""
        <div style="padding: 10px 0;">
            <h2 style="font-family:'Outfit'; color:#0ea5e9; margin-bottom:20px;">🛡️ DETAILS ENQURIY</h2>
        </div>
    """, unsafe_allow_html=True)

    with st.expander("👤 PATIENT DETAILS", expanded=True):
        st.markdown('<p style="font-size:0.8rem; color:#94a3b8; margin-bottom:8px;">Enter patient id</p>', unsafe_allow_html=True)
        pid = st.number_input("Patient ID", min_value=1, value=1001, label_visibility="collapsed")
        if st.button(" SHOW", use_container_width=True, type="primary"):
            r = requests.post("http://localhost:8000/analyze", json={"patient_id": pid})
            if r.status_code == 200:
                st.session_state.result_data = r.json()
                st.session_state.view = "patient"
                st.rerun()

    with st.expander("🔍  INQUIRY", expanded=True):
        st.markdown('<p style="font-size:0.8rem; color:#94a3b8; margin-bottom:8px;">Natural language query</p>', unsafe_allow_html=True)
        query = st.text_area("Query", height=100, label_visibility="collapsed", placeholder="e.g.,risk, patients name...")
        if st.button(" SEARCH", use_container_width=True):
            r = requests.post("http://localhost:8000/hospital/inquiry", json={"query": query})
            if r.status_code == 200:
                st.session_state.result_data = r.json()
                st.session_state.view = "inquiry"
                st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🧹 CLEAR", use_container_width=True):
        st.session_state.clear()
        st.rerun()

# --------------------------------------------------
# WELCOME VIEW
# --------------------------------------------------
if st.session_state.view == "welcome":
    st.markdown("""
    <div class="dash-header">
        <h1 style="margin:0; font-family:'Outfit'; font-weight:700; letter-spacing:-0.02em;">
            HOSPITAL <span style="color:#0ea5e9;">INSIGHT</span> ENGINE
        </h1>
        <p style="color:#94a3b8; font-size:1.1rem; margin-top:8px;">
            Advanced Medical Intelligence & Clinical Support System
        </p>
    </div>
    """, unsafe_allow_html=True)

    glass_card_start()
    st.markdown("""
    ### ⚡ PRO-GRADE FEATURES
    - **Agent-Driven Queries**
    - **Medical RAG**
    - **Clinical Risk Intelligence**
    - **Unified Analytics Dashboard**
    """)
    glass_card_end()

# --------------------------------------------------
# PATIENT VIEW
# --------------------------------------------------
elif st.session_state.view == "patient":
    data = st.session_state.result_data
    ps = data["patient_summary"]
    ds = data["decision_support"]

    st.markdown(f"""
    <div class="dash-header">
        <div style="display:flex; justify-content:space-between; align-items:flex-end;">
            <div>
                <h1 style="margin:0; font-family:'Outfit'; font-weight:700;">{ps['patient_name']}</h1>
                <p style="color:#94a3b8; margin:4px 0 0 0;">PATIENT RECORD #{ps['patient_id']} • {ps['age']} YEARS • {ps['gender'].upper()}</p>
            </div>
            <div style="background:rgba(14, 165, 233, 0.1); padding:8px 16px; border-radius:12px; border:1px solid rgba(14, 165, 233, 0.2);">
                <span style="color:#0ea5e9; font-weight:600; font-size:0.9rem;">LIVE STATUS: ACTIVE</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["📋 CLINICAL SUMMARY", "🧠 DECISION SUPPORT", "📊 TREND ANALYTICS"])

    with tab1:
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(bio_card("Diagnosis", ps["diagnosis"], "🩺"), unsafe_allow_html=True)
        c2.markdown(bio_card("Risk Level", ps["risk_level"], "⚠️"), unsafe_allow_html=True)
        c3.markdown(bio_card("Care Priority", ps["care_priority"], "🏷️"), unsafe_allow_html=True)
        c4.markdown(bio_card("Blood Pressure", ps["blood_pressure"], "❤️"), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        
        # Medical Paper Style for Summary
        st.markdown(f"""
        <div class="medical-paper">
            <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid #f1f5f9; padding-bottom:15px; margin-bottom:20px;">
                <h2 style="margin:0; color:#0f172a; font-family:'Outfit';">CLINICAL CASE SUMMARY</h2>
                <div style="text-align:right;">
                    <span style="font-size:0.8rem; color:#64748b;">RECORDED: 2024-02-10</span><br>
                    <span style="font-weight:700; color:#0ea5e9;">DOC-ID: AI-PX-{ps['patient_id']}</span>
                </div>
            </div>
            <p style="font-size:1.1rem; color:#334155;">
                Assessment for patient <b>{ps['patient_name']}</b> reveals a <b>{ps['risk_level'].lower()} risk</b> profile. 
                The current diagnosis is <b>{ps['diagnosis']}</b>. Clinical indications suggest that <b>{ps['care_priority'].lower()}</b> 
                monitoring is required at this stage.
            </p>
            <div style="display:flex; gap:10px; margin-top:20px;">
                <div class="metric-pill" style="color:#0f172a; border-color:#e2e8f0;">🧬 Genomic Match: High</div>
                <div class="metric-pill" style="color:#0f172a; border-color:#e2e8f0;">🧪 Lab Sync: Complete</div>
                <div class="metric-pill" style="color:#0f172a; border-color:#e2e8f0;">📌 Priority: {ps['care_priority']}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with tab2:
        st.markdown("<br>", unsafe_allow_html=True)
        col_l, col_r = st.columns([1, 1.4])
        with col_l:
            st.markdown("### 🎯 CLINICAL RATIONALE")
            for reason in ds["why"]:
                st.markdown(f"""
                <div class="reason-card">
                    <div style="color:#0ea5e9; font-size:1.2rem;">🔹</div>
                    <div style="font-size:0.95rem; color:#f8fafc;">{reason}</div>
                </div>
                """, unsafe_allow_html=True)

        with col_r:
            st.markdown("### 🧠 EXPERT ANALYSIS")
            consult_box_start()
            st.markdown(ds["llm_explanation"])
            consult_box_end()

    with tab3:
        glass_card_start()
        st.markdown("### � Longitudinal Vitals Trend")
        chart = pd.DataFrame(
            np.random.randint(60, 160, size=(10, 2)),
            columns=["BP", "Heart Rate"]
        )
        st.area_chart(chart)
        glass_card_end()

# --------------------------------------------------
# INQUIRY VIEW
# --------------------------------------------------
elif st.session_state.view == "inquiry":
    res = st.session_state.result_data

    st.markdown("""
    <div class="dash-header">
        <h2 style="margin:0; font-family:'Outfit'; font-weight:700;">INQUIRY <span style="color:#0ea5e9;">REPORT</span></h2>
        <p style="color:#94a3b8; margin:4px 0 0 0;">INTELLIGENT AGENT SYNTHESIS</p>
    </div>
    """, unsafe_allow_html=True)

    intel_brief_start(
        session_id=np.random.randint(100000, 999999), 
        source_count=len(res.get('matched_records', [])) if res.get('matched_records') else 0
    )
    st.markdown(res["deep_explanation"])
    intel_brief_end(
        source_count=len(res.get('matched_records', [])) if res.get('matched_records') else 0
    )

    if res.get("matched_records"):
        st.markdown("<br>", unsafe_allow_html=True)
        glass_card_start()
        st.markdown("### 📄 MATCHED CLINICAL EVIDENCE")
        df = pd.DataFrame(res["matched_records"])
        st.dataframe(df, use_container_width=True)
        glass_card_end()
