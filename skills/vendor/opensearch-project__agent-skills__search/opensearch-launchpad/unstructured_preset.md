# Unstructured Data Preset

When the source is **unstructured** (PDF/DOCX/PPTX/XLSX), apply this bundle without
asking the strategy question. Confirm with the user in one sentence, then proceed.

| Element | Value |
|---|---|
| **Search strategy** | `agentic` (flow agent) |
| **Ingest** | `neural_sparse` (sparse encoding on ingest) |
| **Cloud sink** | `semantic_enrichment` (AOSS ASE) |

## Target: local

1. [document_processing_guide](../../ingest/document-processing/document_processing_guide.md) — process source → chunks.
2. [local_ase.md](local_ase.md) — deploy sparse model, create index, pipeline, bulk-index chunks (manual ASE).
3. Set up flow agent: deploy Bedrock agentic model, create a sparse flow agent (`--sparse`), and wire the pipeline. See [cli-reference](../../cli-reference.md) for full commands.
4. Launch UI.

## Target: cloud

1. Provision AOSS collection with [aws-setup](../../cloud/aws-setup/SKILL.md) if needed.
2. [managed-ingestion-service](../../cloud/managed-ingestion-service/SKILL.md) — handles ingestion (it decides local-vs-cloud processing, uploads to S3, OSIS pipeline with `semantic_enrichment`).
3. Set up flow agent on the collection — follow [serverless-04-agentic-setup](../../cloud/aws-setup/aoss/serverless-04-agentic-setup.md).
4. Launch UI with `--endpoint`.

## Caveats

- The preset uses **flow** agent. Flow agents run on Serverless NextGen. If the user
  explicitly asks for **conversational** agentic search, that requires a managed domain —
  confirm before deviating.
- The preset is a default, not a lock. If the user explicitly requests a different strategy,
  fall through to the normal structured-data path in Phase 2.
