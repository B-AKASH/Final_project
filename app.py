import sqlite3
import csv
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# 🔗 Your existing modules (UNCHANGED)
from rag import retrieve_evidence
from llm import explain_decision, parse_inquiry

# =================================================
# APP CONFIG (ONE APP ONLY)
# =================================================
app = FastAPI(title="Professional Hospital Inquiry System")

# =================================================
# PATH CONFIG (ABSOLUTE – CRITICAL)
# =================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "database.db")
CSV_FILE = os.path.join(BASE_DIR, "data.csv")

# =================================================
# DATABASE INITIALIZATION
# =================================================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            patient_id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER,
            symptoms TEXT,
            diagnosis TEXT
        )
    """)

    conn.commit()
    conn.close()

def load_csv_to_db():
    if not os.path.exists(CSV_FILE):
        print("⚠️ patients.csv not found")
        return

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    with open(CSV_FILE, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            cur.execute("""
                INSERT OR IGNORE INTO patients
                (patient_id, name, age, symptoms, diagnosis)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row["patient_id"],
                row["patient_name"],
                int(row["age"]),
                row["risk_level"],
                row["diagnosis"]
            ))

    conn.commit()
    conn.close()

# =================================================
# STARTUP (CRITICAL)
# =================================================
@app.on_event("startup")
def startup_event():
    init_db()
    load_csv_to_db()
    print("✅ Database initialized at:", DB_NAME)

# =================================================
# REQUEST MODELS
# =================================================
class PatientQuery(BaseModel):
    patient_id: str   # STRING (P001)

class InquiryQuery(BaseModel):
    query: str

# =================================================
# DATABASE HELPERS
# =================================================
def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def get_patient(patient_id: str):
    conn = get_db_connection()
    cur = conn.cursor()

    # SAFETY NET (prevents reload issues)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            patient_id TEXT PRIMARY KEY,
            name TEXT,
            age INTEGER,
            symptoms TEXT,
            diagnosis TEXT
        )
    """)

    cur.execute(
        "SELECT * FROM patients WHERE patient_id = ?",
        (patient_id,)
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return dict(row)

def search_patients(where_clause: str = ""):
    conn = get_db_connection()
    cur = conn.cursor()

    query = "SELECT * FROM patients"
    if where_clause:
        query += f" WHERE {where_clause}"

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    return [dict(r) for r in rows]

# =================================================
# HEALTH CHECK
# =================================================
@app.get("/")
def health():
    return {"status": "ok"}

# =================================================
# 🔘 BUTTON 1 — PATIENT ID ANALYSIS
# =================================================
@app.post("/analyze")
def analyze_patient(q: PatientQuery):
    patient = get_patient(q.patient_id)
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")

    # ---------- CLINICAL REASONS ----------
    reasons = []

    if patient.get("diabetes") == "Yes":
        reasons.append("Patient has diabetes")
    if patient.get("smoking_status") == "Smoker":
        reasons.append("Patient is an active smoker")
    if patient.get("cholesterol", 0) and patient.get("cholesterol", 0) >= 200:
        reasons.append("Elevated cholesterol level")
    if patient.get("obesity") == "Yes":
        reasons.append("Patient is obese")
    if patient.get("chronic_kidney_disease") == "Yes":
        reasons.append("Chronic kidney disease present")

    if not reasons:
        reasons.append("Risk derived from combined clinical indicators")

    # ---------- PDF RAG ----------
    pdf_evidence = retrieve_evidence(patient)

    # ---------- LLM EXPLANATION ----------
    explanation = explain_decision(
        patient,
        reasons,
        pdf_evidence.get("clinical_evidence", []) +
        pdf_evidence.get("insurance_evidence", [])
    )

    return {
        "patient_summary": patient,
        "decision_support": {
            "decision": f"{patient.get('risk_level', 'Unknown')} Risk",
            "why": reasons,
            "llm_explanation": explanation
        },
        "pdf_evidence": pdf_evidence
    }

# =================================================
# 🔘 BUTTON 2 — HOSPITAL INQUIRY
# =================================================
@app.post("/hospital/inquiry")
def hospital_inquiry(q: InquiryQuery):

    # ---------- LLM → NLU ----------
    nlu_data = parse_inquiry(q.query)

    where_clause = " AND ".join(nlu_data.get("sql_conditions", []))

    if nlu_data.get("specific_name"):
        name_clause = f"name LIKE '%{nlu_data['specific_name']}%'"
        where_clause = f"{where_clause} AND {name_clause}" if where_clause else name_clause

    results = search_patients(where_clause)

    total_count = len(results)
    patient_names = [r.get("name") for r in results]

    context_patient = results[0] if results else {
        "diabetes": "No",
        "smoking_status": "No",
        "obesity": "No",
        "chronic_kidney_disease": "No",
        "cholesterol": 0,
        "diagnosis": ""
    }

    pdf_evidence = retrieve_evidence(context_patient, q.query)

    explanation = explain_decision(
        context_patient,
        [nlu_data.get("summary", "Hospital inquiry analysis")],
        pdf_evidence.get("clinical_evidence", []) +
        pdf_evidence.get("insurance_evidence", [])
    )

    return {
        "query": q.query,
        "total_count": total_count,
        "patient_names": patient_names,
        "matched_records": results[:10],
        "pdf_evidence": pdf_evidence,
        "deep_explanation": explanation,
        "nlu_summary": nlu_data.get("summary"),
        "display_mode": nlu_data.get("display_mode", "ANALYTICS_GRID")
    }
