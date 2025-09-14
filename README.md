
# Neo4j Newsletter Starter

This is a tiny FastAPI service that lets an AI agent (or you) insert documents, entities, mentions, and relations into Neo4j, and run Cypher queries.

## Files
- `app.py` — FastAPI app with `/ingest` and `/cypher`.
- `ingest.cypher` — Idempotent upserts (no APOC).
- `openapi.json` — Import into ChatGPT Actions. Replace the server URL after you deploy.
- `requirements.txt` — Python deps.

## Run locally (optional)
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export API_KEY=your_key
export NEO4J_URI=neo4j+s://<your-hosted-db>  # Aura URI
export NEO4J_USER=neo4j
export NEO4J_PASS=<password>
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Health check
`GET /health` → `{"status":"ok"}`

## Provenance & Imported nodes
- Ingest fills Doc.fetched_at (retrieval date), source_name, doc_type.
- New Entities get label :Imported (grey). New Relations get provenance='imported'.
- Run the promotion query in queries.cypher once a day to turn grey → colored.

### Quick start
python app.py
# then in Neo4j Browser:
#  - run the metrics queries
#  - run the promotion job when you want

