# SqlCatalog
Set of tools to cataloging SQL script creating a database and make query on it.
Cataloging result:
- File catalog.json
- Export of each sql create statement of tables, views, function and procedures.

# VectorizeCatalog


## vectorize explicitly (optional – query/RAG will auto-build if missing)
python vectorize_catalog.py

## semantic retrieval

### One time query
python query_catalog.py "procedures that update client cash table" --kind procedure --schema dbo

### RAG answer with Qwen
python rag_chat.py "Which views read from Order tables?" --kind view

### deterministic utility inside RAG script
python rag_chat.py "what table that is not accessed by any procedure" --op unaccessed --schema dbo --no-chat

## client server mode

### query client
python cli.py  "what table that is not accessed by any procedure" --kind table

### using server
python webapp.py

Runs a server and give a html page to perform queries.
