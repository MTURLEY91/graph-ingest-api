
// ingest.cypher (no-APOC variant; supports limited typed rels + fallback)
WITH $doc AS d
MERGE (doc:Doc {id:d.id})
SET doc.url = d.url,
    doc.title = d.title,
    doc.source = d.source,
    doc.published_at = d.published_at,
    doc.fetched_at = d.fetched_at,
    doc.lang = d.lang,
    doc.summary = d.summary;

UNWIND $entities AS e
MERGE (ent:Entity {id:e.id})
SET ent.name = e.name,
    ent.type = e.type,
    ent.domain = e.domain,
    ent.country = e.country,
    ent.aliases = coalesce(e.aliases, []),
    ent.updated_at = timestamp();

UNWIND $mentions AS m
MATCH (doc:Doc {id:m.doc_id})
MATCH (ent:Entity {id:m.entity_id})
MERGE (doc)-[rm:MENTIONS]->(ent)
SET rm.sentences = coalesce(m.sentences, []),
    rm.confidence = coalesce(m.confidence, 0.0),
    rm.created_at = coalesce(rm.created_at, timestamp());

// Relations with allowed explicit types; otherwise fallback to RELATES_TO
UNWIND $relations AS r0
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
// fallback
FOREACH (_ IN CASE WHEN r0.type IS NULL OR r0.type IN ['IMPACTS','SUPPLIES','PART_OF','LOCATED_IN'] THEN [] ELSE [1] END |
  MERGE (s)-[rel:RELATES_TO]->(t)
  SET rel.predicate = coalesce(r0.predicate, r0.type),
      rel.evidence_doc = r0.evidence_doc,
      rel.confidence = coalesce(r0.confidence, 0.0),
      rel.created_at = coalesce(rel.created_at, timestamp())
);
