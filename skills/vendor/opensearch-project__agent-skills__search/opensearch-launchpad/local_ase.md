# Local Ingestion Guide

Indexes locally-processed chunks into a local OpenSearch cluster with sparse encoding.
This replicates what AOSS Automatic Semantic Enrichment (ASE) does in the cloud.

- **Input:** `.opensearch/chunks/<index>/<file>.jsonl` (from [document_processing_guide](../../ingest/document-processing/document_processing_guide.md))
- **Output:** a searchable index with sparse-encoded chunks

## Steps

### 1. Deploy the sparse encoding model

```bash
uv run python scripts/opensearch_ops.py deploy-model \
  --name amazon/neural-sparse/opensearch-neural-sparse-encoding-doc-v2-mini
```

Note the returned `model_id`.

### 2. Create the index

```bash
uv run python scripts/opensearch_ops.py create-index \
  --name <index> \
  --body '{
    "mappings": {
      "properties": {
        "text": {"type": "text"},
        "text_sparse": {"type": "rank_features"},
        "headings": {"type": "keyword"},
        "source_file": {"type": "keyword"},
        "chunk_id": {"type": "integer"}
      }
    },
    "settings": {"index": {"default_pipeline": "sparse-ingest-pipeline"}}
  }'
```

### 3. Create the sparse ingest pipeline

```bash
uv run python scripts/opensearch_ops.py create-pipeline \
  --name sparse-ingest-pipeline \
  --type ingest \
  --body '{"processors": [{"sparse_encoding": {"model_id": "<model_id>", "field_map": {"text": "text_sparse"}}}]}' \
  --index <index>
```

### 4. Bulk-index the chunks

```bash
uv run python scripts/opensearch_ops.py index-bulk \
  --source-file .opensearch/chunks/<index>/<filename>.jsonl \
  --index <index> \
  --count 1000
```

After this, set up the flow agent (see [cli-reference](../../cli-reference.md) — agentic search setup with `--sparse`).
