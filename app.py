
# app.py
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
import os

API_KEY = os.environ.get("API_KEY")
NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASS = os.environ.get("NEO4J_PASS")

if not all([API_KEY, NEO4J_URI, NEO4J_USER, NEO4J_PASS]):
    raise RuntimeError("Missing required environment variables: API_KEY, NEO4J_URI, NEO4J_USER, NEO4J_PASS")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

app = FastAPI(title="Graph Ingest API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

def run_tx(query: str, params: dict | None = None):
    with driver.session() as s:
        res = s.run(query, params or {})
        try:
            return [r.data() for r in res]
        except:
            return []

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/cypher")
def run_cypher(body: dict, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "unauthorized")
    q = body.get("query")
    params = body.get("params", {})
    if not q:
        raise HTTPException(400, "query is required")
    return run_tx(q, params)

@app.post("/ingest")
def ingest(payload: dict, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "unauthorized")
    # Load Cypher from file
    qpath = os.path.join(os.path.dirname(__file__), "ingest.cypher")
    with open(qpath, "r", encoding="utf-8") as f:
        q = f.read()
    return run_tx(q, payload)
