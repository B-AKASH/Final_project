from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
from rag import retrieve_evidence
from llm import explain_decision, parse_inquiry

# =================================================
# APP CONFIG
# =================================================
app = FastAPI(title="Professional Hospital Inquiry System")

import sqlite3

DB_PATH = "a.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS patients (
        patient_id INTEGER PRIMARY KEY,
        patient_name TEXT,
        age INTEGER,
        gender TEXT,
        diagnosis TEXT,
        risk_level TEXT,
        care_priority TEXT,
        blood_pressure TEXT
    )
    """)

    conn.commit()
    conn.close()



# =================================================
# REQUEST MODELS
# =================================================
class PatientQuery(BaseModel):
    patient_id: int

class InquiryQuery(BaseModel):
    query: str

# =================================================
# DATABASE HELPERS (SQLite3)
# =================================================
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_patient(patient_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM patients WHERE patient_id = ?",
        (patient_id,)
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

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
    if patient.get("cholesterol", 0) >= 200:
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
        pdf_evidence["clinical_evidence"] + pdf_evidence["insurance_evidence"]
    )

    return {
        "patient_summary": patient,
        "decision_support": {
            "decision": f"{patient['risk_level']} Risk",
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
    # ---------- INTELLIGENT NLU → SQL ----------
    # The LLM parses the inquiry and returns structured conditions
    nlu_data = parse_inquiry(q.query)
    
    where_clause = " AND ".join(nlu_data["sql_conditions"])
    
    # Fallback for name search if the LLM identifies a name but no conditions
    if nlu_data.get("specific_name") and not where_clause:
        where_clause = f"patient_name LIKE '%{nlu_data['specific_name']}%'"
    elif nlu_data.get("specific_name") and where_clause:
        where_clause += f" AND patient_name LIKE '%{nlu_data['specific_name']}%'"

    results = search_patients(where_clause)

    total_count = len(results)
    patient_names = [r["patient_name"] for r in results]

    # ---------- CONTEXT FOR RAG + LLM ----------
    # We use the first matched patient as context for guidelines, or a generic mock if none
    context_patient = results[0] if results else {
        "diabetes": "No", "smoking_status": "No", "obesity": "No", 
        "chronic_kidney_disease": "No", "cholesterol": 0, "diagnosis": ""
    }

    pdf_evidence = retrieve_evidence(context_patient, q.query)

    # Generate the professional matrix explanation
    explanation = explain_decision(
        results[0] if results else {
            "patient_name": "N/A", "age": "N/A", "gender": "N/A",
            "diagnosis": "N/A", "risk_level": "N/A"
        },
        [nlu_data["summary"]],
        pdf_evidence["clinical_evidence"] + pdf_evidence["insurance_evidence"]
    )

    return {
        "query": q.query,
        "total_count": total_count,
        "patient_names": patient_names,
        "matched_records": results[:10],
        "pdf_evidence": pdf_evidence,
        "deep_explanation": explanation,
        "nlu_summary": nlu_data["summary"],
        "display_mode": nlu_data.get("display_mode", "ANALYTICS_GRID")
    }
