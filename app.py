
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

INGEST_CYPHER = r"""
// Carry all payload parts up-front
WITH $doc AS d, $entities AS entities, $mentions AS mentions, $relations AS relations

// Upsert the Doc
MERGE (doc:Doc {id:d.id})
SET doc.url = d.url,
    doc.title = d.title,
    doc.source = d.source,
    doc.published_at = d.published_at,
    doc.fetched_at = d.fetched_at,
    doc.lang = d.lang,
    doc.summary = d.summary

// Upsert Entities
WITH doc, entities, mentions, relations
UNWIND entities AS e
MERGE (ent:Entity {id:e.id})
SET ent.name = e.name,
    ent.type = e.type,
    ent.domain = e.domain,
    ent.country = e.country,
    ent.aliases = coalesce(e.aliases, []),
    ent.updated_at = timestamp()

// Mentions (link this doc to entities)
WITH doc, mentions, relations
UNWIND mentions AS m
MATCH (ent:Entity {id:m.entity_id})
MERGE (doc)-[rm:MENTIONS]->(ent)
SET rm.sentences = coalesce(m.sentences, []),
    rm.confidence = coalesce(m.confidence, 0.0),
    rm.created_at = coalesce(rm.created_at, timestamp())

// Relations between entities (typed if allowed, else RELATES_TO with predicate)
WITH relations
UNWIND relations AS r0
MATCH (s:Entity {id:r0.start_id}), (t:Entity {id:r0.end_id})
FOREACH (_ IN CASE WHEN r0.type = 'IMPACTS' THEN [1] ELSE [] END |
  MERGE (s)-[rel:IMPACTS]->(t)
  SET rel.predicate = coalesce(r0.predicate, null),
      rel.evidence_doc = r0.evidence_doc,
      rel.confidence = coalesce(r0.confidence, 0.0),
      rel.created_at = coalesce(rel.created_at, timestamp())
)
FOREACH (_ IN CASE WHEN r0.type = 'SUPPLIES' THEN [1] ELSE [] END |
  MERGE (s)-[rel:SUPPLIES]->(t)
  SET rel.predicate = coalesce(r0.predicate, null),
      rel.evidence_doc = r0.evidence_doc,
      rel.confidence = coalesce(r0.confidence, 0.0),
      rel.created_at = coalesce(rel.created_at, timestamp())
)
FOREACH (_ IN CASE WHEN r0.type = 'PART_OF' THEN [1] ELSE [] END |
  MERGE (s)-[rel:PART_OF]->(t)
  SET rel.predicate = coalesce(r0.predicate, null),
      rel.evidence_doc = r0.evidence_doc,
      rel.confidence = coalesce(r0.confidence, 0.0),
      rel.created_at = coalesce(rel.created_at, timestamp())
)
FOREACH (_ IN CASE WHEN r0.type = 'LOCATED_IN' THEN [1] ELSE [] END |
  MERGE (s)-[rel:LOCATED_IN]->(t)
  SET rel.predicate = coalesce(r0.predicate, null),
      rel.evidence_doc = r0.evidence_doc,
      rel.confidence = coalesce(r0.confidence, 0.0),
      rel.created_at = coalesce(rel.created_at, timestamp())
)
FOREACH (_ IN CASE WHEN r0.type IS NULL OR r0.type IN ['IMPACTS','SUPPLIES','PART_OF','LOCATED_IN'] THEN [] ELSE [1] END |
  MERGE (s)-[rel:RELATES_TO]->(t)
  SET rel.predicate = coalesce(r0.predicate, r0.type),
      rel.evidence_doc = r0.evidence_doc,
      rel.confidence = coalesce(r0.confidence, 0.0),
      rel.created_at = coalesce(rel.created_at, timestamp())
)
"""

@app.post("/ingest")
def ingest(payload: dict, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "unauthorized")
    try:
        return run_tx(INGEST_CYPHER, payload)
    except Exception as e:
        raise HTTPException(500, f"ingest error: {type(e).__name__}: {e}")


@app.get("/diag")
def diag(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "unauthorized")
    try:
        res = run_tx("RETURN 1 AS ok")
        return {"neo4j_ok": True, "result": res}
    except Exception as e:
        return {"neo4j_ok": False, "error": f"{type(e).__name__}: {e}"}
