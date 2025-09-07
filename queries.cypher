
// --- Save these queries in Neo4j Browser ---

// 0) Constraints & indexes (run once on a fresh DB)
CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (d:Doc) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE;
CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name);
CREATE INDEX doc_url IF NOT EXISTS FOR (d:Doc) ON (d.url);

// 1) New cross-domain bridges (last 7 days by relationship creation time)
MATCH (a:Entity)-[r]->(b:Entity)
WHERE r.created_at >= timestamp() - 7*24*3600*1000
  AND a.domain <> b.domain
RETURN a.name AS A, type(r) AS Rel, b.name AS B, r.evidence_doc AS doc_id, r.confidence AS conf
ORDER BY conf DESC LIMIT 20;

// 2) Rising hubs (simple WoW change using mentions count, no GDS)
WITH timestamp() AS now
MATCH (e:Entity)<-[m:MENTIONS]-()
WITH e, count(m) AS mentions_total
OPTIONAL MATCH (e)<-[m7:MENTIONS]-()
WHERE m7.created_at >= now - 7*24*3600*1000
WITH e, mentions_total, count(m7) AS mentions_7d
OPTIONAL MATCH (e)<-[m14:MENTIONS]-()
WHERE m14.created_at >= now - 14*24*3600*1000 AND m14.created_at < now - 7*24*3600*1000
WITH e, mentions_total, mentions_7d, count(m14) AS mentions_prev_7d
RETURN e.name AS entity, (mentions_7d - mentions_prev_7d) AS delta
ORDER BY delta DESC LIMIT 20;

// 3) Policy → Company impact chains (≤3 hops)
MATCH p:Entity {type:'Policy'}
MATCH path = (p)-[:IMPACTS|RELATES_TO*1..3]->(c:Entity {type:'Company'})
RETURN p.name AS policy, c.name AS company, relationships(path) AS rels
ORDER BY length(path) ASC, company LIMIT 20;

// 4) Sources (last 7 days)
MATCH (d:Doc)<-[:MENTIONS]-()
WHERE d.published_at >= date() - duration('P7D') OR d.fetched_at >= date() - duration('P7D')
RETURN d.title, d.source, d.url, d.published_at
ORDER BY d.published_at DESC LIMIT 40;
