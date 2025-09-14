// ingest.cypher — adds provenance + :Imported while keeping your typed rels

// ---------- 1) Doc ----------
WITH $doc AS d
MERGE (doc:Doc {id:d.id})
SET doc.url          = d.url,
    doc.title        = d.title,
    doc.source       = d.source,        // kept for backward compatibility
    doc.published_at = d.published_at,
    doc.fetched_at   = d.fetched_at,    // retrieval date
    doc.lang         = d.lang,
    doc.summary      = d.summary,
    doc.type         = coalesce(d.type, doc.type);   // e.g. "news" | "report" | "filing"

// ---------- 2) Source node (optional but useful) ----------
WITH doc, d
FOREACH (_ IN CASE WHEN d.source IS NULL THEN [] ELSE [1] END |
  MERGE (src:Source {name:d.source})
    ON CREATE SET src.type = 'publisher'
  MERGE (doc)-[:PUBLISHED_BY]->(src)
)

// ---------- 3) Entities (new ones are grey via :Imported) ----------
UNWIND $entities AS e
MERGE (ent:Entity {id:e.id})
  ON CREATE SET ent:Imported, ent.created_at = timestamp()
SET ent.name = e.name,
    ent.type = e.type,
    ent.domain = CASE
                   WHEN toLower(e.domain) IN ['tech','physical'] THEN 'Physical'
                   ELSE e.domain
                 END,
    ent.country   = e.country,
    ent.aliases   = coalesce(e.aliases, []),
    ent.updated_at= timestamp();

// ---------- 4) Mentions (Doc -> Entity) ----------
UNWIND $mentions AS m
MATCH (doc:Doc {id:m.doc_id})
MATCH (ent:Entity {id:m.entity_id})
MERGE (doc)-[rm:MENTIONS]->(ent)
SET rm.sentences  = coalesce(m.sentences, []),
    rm.confidence = coalesce(m.confidence, 0.0),
    rm.created_at = coalesce(rm.created_at, timestamp());

// ---------- 5) Entity↔Entity relations (typed + fallback) ----------
UNWIND $relations AS r0
MATCH (s:Entity {id:r0.start_id}), (t:Entity {id:r0.end_id})

// ---- IMPACTS ----
FOREACH (_ IN CASE WHEN r0.type = 'IMPACTS' THEN [1] ELSE [] END |
  MERGE (s)-[rel:IMPACTS]->(t)
  SET rel.predicate      = coalesce(r0.predicate, null),
      rel.evidence_doc   = coalesce(r0.evidence_doc, rel.evidence_doc),
      rel.evidence_docs  = CASE
                              WHEN $doc.id IS NULL THEN rel.evidence_docs
                              WHEN rel.evidence_docs IS NULL THEN [$doc.id]
                              WHEN NOT $doc.id IN rel.evidence_docs THEN rel.evidence_docs + $doc.id
                              ELSE rel.evidence_docs
                           END,
      rel.prev_confidence= rel.confidence,
      rel.confidence     = coalesce(r0.confidence, rel.confidence, 0.0),
      rel.provenance     = coalesce(rel.provenance, 'import
