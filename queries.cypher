// ==============================
// queries.cypher  (beginner-safe)
// ==============================

// 0) Constraints & indexes (run once in a fresh DB)
//    Tip: run these in Neo4j Browser; they’re idempotent.
CREATE CONSTRAINT doc_id IF NOT EXISTS     FOR (d:Doc)    REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT entity_id IF NOT EXISTS  FOR (e:Entity) REQUIRE e.id IS UNIQUE;
CREATE CONSTRAINT source_name IF NOT EXISTS FOR (s:Source) REQUIRE s.name IS UNIQUE;
CREATE INDEX entity_name IF NOT EXISTS     FOR (e:Entity) ON (e.name);
CREATE INDEX doc_url IF NOT EXISTS         FOR (d:Doc)    ON (d.url);

// -----------------------------------------------------
// A) PIPELINE / PROVENANCE METRICS (new, uses fetched_at)
// -----------------------------------------------------

// A1) Sources scanned in the last 7 days (by doc)
//     Uses fetched_at if present, otherwise published_at.
WITH datetime() AS now
MATCH (d:Doc)
WITH d, coalesce(datetime(d.published_at), datetime(d.fetched_at), now) AS dt
WHERE dt >= now - duration('P7D')
RETURN count(d) AS docs_7d;

// A2) Docs by type (news/report/filing…) in last 7 days
WITH datetime() AS now
MATCH (d:Doc)
WITH d, coalesce(datetime(d.published_at), datetime(d.fetched_at), now) AS dt
WHERE dt >= now - duration('P7D')
RETURN coalesce(d.type,'unknown') AS doc_type, count(*) AS docs
ORDER BY docs DESC;

// A3) New cross-domain bridges (7d, by relationship creation time)
//     Works with typed rels + fallback; shows either evidence_doc or evidence_docs.
WITH timestamp() AS now
MATCH (a:Entity)-[r]->(b:Entity)
WHERE a.domain IS NOT NULL AND b.domain IS NOT NULL
  AND a.domain <> b.domain
  AND coalesce(r.created_at,0) >= now - 7*24*3600*1000
RETURN a.name AS A,
       type(r) AS Rel,
       coalesce(r.predicate,'') AS Pred,
       b.name AS B,
       round(coalesce(r.confidence,0),2) AS Conf,
       coalesce(r.evidence_doc, head(coalesce(r.evidence_docs,[]))) AS doc_id
ORDER BY Conf DESC
LIMIT 20;

// A4) Strengthened connections (Δconfidence ≥ 0.05 in last 7 days)
WITH timestamp() AS now
MATCH ()-[r]->()
WHERE coalesce(r.updated_at,0) >= now - 7*24*3600*1000
  AND r.prev_confidence IS NOT NULL
  AND (coalesce(r.confidence,0) - coalesce(r.prev_confidence,0)) >= 0.05
RETURN type(r) AS Rel,
       coalesce(r.predicate,'') AS Pred,
       round(r.confidence - r.prev_confidence,3) AS d_conf,
       coalesce(r.evidence_doc, head(coalesce(r.evidence_docs,[]))) AS doc_id
ORDER BY d_conf DESC
LIMIT 25;

// A5) “Novel pairs” seen this week (no pair older than 30 days)
//     First-time or rare connections signal non-obviousness.
WITH timestamp() AS now
MATCH (a:Entity)-[r]->(b:Entity)
WHERE coalesce(r.created_at,0) >= now - 7*24*3600*1000
WITH a,b,r,now
OPTIONAL MATCH (a)-[old]->(b)
WHERE coalesce(old.created_at,0) < now - 30*24*3600*1000
WITH a,b,r, count(old) AS older_than_30d
WHERE older_than_30d = 0
RETURN a.name AS A,
       coalesce(r.predicate, type(r)) AS Pred,
       b.name AS B,
       coalesce(r.evidence_doc, head(coalesce(r.evidence_docs,[]))) AS doc_id
LIMIT 25;

// A6) Promotions this week (grey → colored)
WITH timestamp() AS now
MATCH (e:Entity)
WHERE coalesce(e.promoted_at,0) >= now - 7*24*3600*1000
RETURN count(e) AS promoted_7d;

// A7) Grey share (how much of graph is still :Imported)
MATCH (e:Entity)
RETURN count{ (e:Imported) } AS grey,
       count(*)              AS total,
       toFloat(count{ (e:Imported) })/count(*) AS grey_share;

// A8) Docs list (last 7 days) with source + link (handy table)
WITH datetime() AS now
MATCH (d:Doc)
WITH d, coalesce(datetime(d.published_at), datetime(d.fetched_at), now) AS dt
WHERE dt >= now - duration('P7D')
RETURN coalesce(d.title,'(untitled)') AS title,
       coalesce(d.source,'(unknown)') AS source,
       d.url AS url,
       d.published_at AS published_at,
       d.fetched_at   AS fetched_at
ORDER BY dt DESC
LIMIT 40;

// -----------------------------------------------------
// B) YOUR ORIGINAL ANALYTICS (kept, minor safety tweaks)
// -----------------------------------------------------

// B1) Rising hubs (simple WoW change using mentions count, no GDS)
WITH timestamp() AS now
MATCH (e:Entity)<-[m:MENTIONS]-()
WITH e, count(m) AS mentions_total
OPTIONAL MATCH (e)<-[m7:MENTIONS]-()
WHERE coalesce(m7.created_at,0) >= now - 7*24*3600*1000
WITH e, mentions_total, count(m7) AS mentions_7d
OPTIONAL MATCH (e)<-[m14:MENTIONS]-()
WHERE coalesce(m14.created_at,0) >= now - 14*24*3600*1000
  AND coalesce(m14.created_at,0) <  now - 7*24*3600*1000
WITH e, mentions
