# PDF Parser Service

Async PDF parsing service (HTTP API + queue + background workers) with a Python client SDK. Built on PyMuPDF, using ports-and-adapters (hexagonal) architecture, with a first implementation targeting AWS.

- Extracts text and layout, tables, and a Markdown rendering suited to LLM/RAG pipelines.
- Hybrid parsing: native text for born-digital pages, OCR for scanned pages, decided per page.
- Azure Document Intelligence-style long-running operations: submit a PDF, poll or receive a webhook.
- Python SDK with sync and async clients.

## Status

Design phase. See [docs/DESIGN.md](docs/DESIGN.md).
