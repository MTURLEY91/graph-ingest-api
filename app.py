
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

QUERIES = {
  "bridges_7d": """
  WITH timestamp() AS now
  MATCH (a:Entity)-[r]->(b:Entity)
  WHERE a.domain IS NOT NULL AND b.domain IS NOT NULL
    AND a.domain <> b.domain
    AND coalesce(r.created_at,0) >= now - 7*24*3600*1000
  WITH a,b,r,
       coalesce(r.evidence_doc, head(coalesce(r.evidence_docs, []))) AS doc_id
  OPTIONAL MATCH (d:Doc {id:doc_id})
  RETURN a.name AS A, a.domain AS A_dom,
         type(r) AS Rel, coalesce(r.predicate,'') AS Predicate,
         b.name AS B, b.domain AS B_dom,
         round(coalesce(r.confidence,0),2) AS Conf,
         coalesce(d.title, doc_id) AS SourceTitle, d.url AS SourceURL
  ORDER BY Conf DESC, A, B LIMIT 20;
  """,

  "storage_7d": """
  WITH timestamp() AS now
  MATCH (x:Entity)-[r]->(y:Entity)
  WHERE (y.id='process_battery_storage' OR x.id STARTS WITH 'tech_batt_' OR y.id='loss_storage')
    AND coalesce(r.created_at,0) >= now - 7*24*3600*1000
  WITH x,y,r,
       coalesce(r.evidence_doc, head(coalesce(r.evidence_docs, []))) AS doc_id
  OPTIONAL MATCH (d:Doc {id:doc_id})
  RETURN x.name AS From, type(r) AS Rel, coalesce(r.predicate,'') AS Pred,
         y.name AS To, round(coalesce(r.confidence,0),2) AS Conf,
         coalesce(d.title, doc_id) AS Source, d.url AS URL
  ORDER BY Conf DESC LIMIT 20;
  """,

  "dc_mix": """
  MATCH (src)-[r:FLOWS_TO]->(:Entity {id:'sub_digital_datacenters'})
  RETURN src.name AS Input, src.type AS Type, r.value AS Value, r.unit AS Unit, r.year AS Year, r.scenario AS Scenario
  ORDER BY coalesce(r.value,0) DESC, src.name;
  """,

  "efficiency_7d": """
  WITH timestamp() AS now
  MATCH (n:Entity)-[r:RELATES_TO {predicate:'reduces_losses'}]->(l:Entity {type:'Loss'})
  WHERE coalesce(r.created_at,0) >= now - 7*24*3600*1000
  WITH n,l,r,
       coalesce(r.evidence_doc, head(coalesce(r.evidence_docs, []))) AS doc_id
  OPTIONAL MATCH (d:Doc {id:doc_id})
  RETURN n.name AS Actor, l.name AS LossBucket, round(coalesce(r.confidence,0),2) AS Conf,
         coalesce(d.title, doc_id) AS Source, d.url AS URL
  ORDER BY Conf DESC;
  """,

  "sources_7d": """
  WITH datetime() AS now
  MATCH (d:Doc)
  WITH d, coalesce(datetime(d.published_at), datetime(d.fetched_at), now) AS dt
  WHERE dt >= now - duration('P7D')
  RETURN coalesce(d.source,'(unknown)') AS Source, count(*) AS Docs
  ORDER BY Docs DESC LIMIT 15;
  """
}


from fastapi import Body

@app.post("/named")
def run_named(name: str = Body(..., embed=True), x_api_key: str = Header(None)):
  if x_api_key != API_KEY:
    raise HTTPException(401, "unauthorized")
  q = QUERIES.get(name)
  if not q:
    raise HTTPException(400, f"unknown query: {name}")
  return run_tx(q, {})


INGEST_CYPHER = r"""
WITH
  $doc AS d,
  coalesce($entities, [])  AS entities,
  coalesce($mentions, [])  AS mentions,
  coalesce($relations, []) AS relations

// ---------- Doc ----------
MERGE (doc:Doc {id:d.id})
SET doc.url          = d.url,
    doc.title        = d.title,
    doc.source       = d.source,        // kept for compatibility
    doc.published_at = d.published_at,
    doc.fetched_at   = d.fetched_at,    // retrieval timestamp
    doc.lang         = d.lang,
    doc.summary      = d.summary,
    doc.type         = coalesce(d.type, doc.type)  // "news" | "report" | "filing" | ...

// ---------- Source node (optional but useful) ----------
WITH doc, d, entities, mentions, relations
FOREACH (_ IN CASE WHEN d.source IS NULL THEN [] ELSE [1] END |
  MERGE (src:Source {name:d.source})
    ON CREATE SET src.type = 'publisher'
  MERGE (doc)-[:PUBLISHED_BY]->(src)
)

// ---------- Entities (new ones are grey via :Imported) ----------
WITH doc, entities, mentions, relations
UNWIND entities AS e
MERGE (ent:Entity {id:e.id})
  ON CREATE SET ent:Imported, ent.created_at = timestamp()
SET ent.name       = e.name,
    ent.type       = e.type,
    ent.domain     = e.domain,
    ent.country    = e.country,
    ent.aliases    = coalesce(e.aliases, []),
    ent.updated_at = timestamp()

// ---------- Mentions (Doc -> Entity) ----------
WITH doc, mentions, relations
UNWIND mentions AS m
MATCH (ent:Entity {id:m.entity_id})
MERGE (doc)-[rm:MENTIONS]->(ent)
SET rm.sentences  = coalesce(m.sentences, []),
    rm.confidence = coalesce(m.confidence, 0.0),
    rm.created_at = coalesce(rm.created_at, timestamp())

// ---------- Relations (typed + fallback) ----------
WITH doc, relations
UNWIND relations AS r0
MATCH (s:Entity {id:r0.start_id}), (t:Entity {id:r0.end_id})

// ---- IMPACTS ----
FOREACH (_ IN CASE WHEN r0.type = 'IMPACTS' THEN [1] ELSE [] END |
  MERGE (s)-[rel:IMPACTS]->(t)
  SET rel.predicate       = coalesce(r0.predicate, null),
      rel.evidence_doc    = coalesce(r0.evidence_doc, rel.evidence_doc),
      rel.evidence_docs   = CASE
                               WHEN doc.id IS NULL THEN rel.evidence_docs
                               WHEN rel.evidence_docs IS NULL THEN [doc.id]
                               WHEN NOT doc.id IN rel.evidence_docs THEN rel.evidence_docs + doc.id
                               ELSE rel.evidence_docs
                            END,
      rel.prev_confidence = rel.confidence,
      rel.confidence      = coalesce(r0.confidence, rel.confidence, 0.0),
      rel.provenance      = coalesce(rel.provenance, 'imported'),
      rel.created_at      = coalesce(rel.created_at, timestamp()),
      rel.updated_at      = timestamp()
)
// ---- SUPPLIES ----
FOREACH (_ IN CASE WHEN r0.type = 'SUPPLIES' THEN [1] ELSE [] END |
  MERGE (s)-[rel:SUPPLIES]->(t)
  SET rel.predicate       = coalesce(r0.predicate, null),
      rel.evidence_doc    = coalesce(r0.evidence_doc, rel.evidence_doc),
      rel.evidence_docs   = CASE
                               WHEN doc.id IS NULL THEN rel.evidence_docs
                               WHEN rel.evidence_docs IS NULL THEN [doc.id]
                               WHEN NOT doc.id IN rel.evidence_docs THEN rel.evidence_docs + doc.id
                               ELSE rel.evidence_docs
                            END,
      rel.prev_confidence = rel.confidence,
      rel.confidence      = coalesce(r0.confidence, rel.confidence, 0.0),
      rel.provenance      = coalesce(rel.provenance, 'imported'),
      rel.created_at      = coalesce(rel.created_at, timestamp()),
      rel.updated_at      = timestamp()
)
// ---- PART_OF ----
FOREACH (_ IN CASE WHEN r0.type = 'PART_OF' THEN [1] ELSE [] END |
  MERGE (s)-[rel:PART_OF]->(t)
  SET rel.predicate       = coalesce(r0.predicate, null),
      rel.evidence_doc    = coalesce(r0.evidence_doc, rel.evidence_doc),
      rel.evidence_docs   = CASE
                               WHEN doc.id IS NULL THEN rel.evidence_docs
                               WHEN rel.evidence_docs IS NULL THEN [doc.id]
                               WHEN NOT doc.id IN rel.evidence_docs THEN rel.evidence_docs + doc.id
                               ELSE rel.evidence_docs
                            END,
      rel.prev_confidence = rel.confidence,
      rel.confidence      = coalesce(r0.confidence, rel.confidence, 0.0),
      rel.provenance      = coalesce(rel.provenance, 'imported'),
      rel.created_at      = coalesce(rel.created_at, timestamp()),
      rel.updated_at      = timestamp()
)
// ---- LOCATED_IN ----
FOREACH (_ IN CASE WHEN r0.type = 'LOCATED_IN' THEN [1] ELSE [] END |
  MERGE (s)-[rel:LOCATED_IN]->(t)
  SET rel.predicate       = coalesce(r0.predicate, null),
      rel.evidence_doc    = coalesce(r0.evidence_doc, rel.evidence_doc),
      rel.evidence_docs   = CASE
                               WHEN doc.id IS NULL THEN rel.evidence_docs
                               WHEN rel.evidence_docs IS NULL THEN [doc.id]
                               WHEN NOT doc.id IN rel.evidence_docs THEN rel.evidence_docs + doc.id
                               ELSE rel.evidence_docs
                            END,
      rel.prev_confidence = rel.confidence,
      rel.confidence      = coalesce(r0.confidence, rel.confidence, 0.0),
      rel.provenance      = coalesce(rel.provenance, 'imported'),
      rel.created_at      = coalesce(rel.created_at, timestamp()),
      rel.updated_at      = timestamp()
)
// ---- Fallback RELATES_TO (uses r0.type as predicate) ----
FOREACH (_ IN CASE WHEN r0.type IS NULL OR r0.type IN ['IMPACTS','SUPPLIES','PART_OF','LOCATED_IN'] THEN [] ELSE [1] END |
  MERGE (s)-[rel:RELATES_TO {predicate:r0.type}]->(t)
  SET rel.predicate       = coalesce(r0.predicate, r0.type),
      rel.evidence_doc    = coalesce(r0.evidence_doc, rel.evidence_doc),
      rel.evidence_docs   = CASE
                               WHEN doc.id IS NULL THEN rel.evidence_docs
                               WHEN rel.evidence_docs IS NULL THEN [doc.id]
                               WHEN NOT doc.id IN rel.evidence_docs THEN rel.evidence_docs + doc.id
                               ELSE rel.evidence_docs
                            END,
      rel.prev_confidence = rel.confidence,
      rel.confidence      = coalesce(r0.confidence, rel.confidence, 0.0),
      rel.provenance      = coalesce(rel.provenance, 'imported'),
      rel.created_at      = coalesce(rel.created_at, timestamp()),
      rel.updated_at      = timestamp()
)
"""

@app.post("/ingest")
def ingest(payload: dict, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "unauthorized")
    safe = {
        "doc":       payload.get("doc", {}),
        "entities":  payload.get("entities", []),
        "mentions":  payload.get("mentions", []),
        "relations": payload.get("relations", [])
    }
    try:
        return run_tx(INGEST_CYPHER, safe)
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
