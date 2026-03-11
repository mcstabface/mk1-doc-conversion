[README.md](https://github.com/user-attachments/files/25904043/README.md)
# MK1 Document Conversion Pipeline

Deterministic document ingestion and conversion pipeline designed to support downstream LLM ingestion workflows.

The system scans a directory tree, fingerprints documents, determines whether they require conversion, and converts eligible files to PDF using LibreOffice in headless mode.

The pipeline is **idempotent** and **delta-aware**, meaning previously converted files are skipped automatically unless their contents change.

---

# Key Features

Deterministic ingestion pipeline:

inventory → fingerprint → registry decision → conversion → audit

Capabilities:

- Recursive document discovery
- SHA256 fingerprinting
- Delta-aware conversion
- Automatic skip of unchanged files
- Parallel document conversion
- SQLite audit history
- Deterministic run manifests
- Clean CLI output
- LibreOffice-based document normalization

---

# Supported Input Types

LibreOffice enables conversion of multiple document types:

Word:
- .doc
- .docx
- .odt
- .rtf

Spreadsheets:
- .xls
- .xlsx
- .ods

Presentations:
- .ppt
- .pptx
- .odp

Additional formats may also work depending on LibreOffice support.

---

# Requirements

Required software:

LibreOffice  
https://www.libreoffice.org/download/

LibreOffice is used in **headless mode** to perform document conversion.

Python is **not required** if using the packaged executable.

---

# Usage

Example:


mk1-doc-conversion.exe --source C:\documents


Optional parameters:


--source Directory to recursively scan
--pdf-output Directory where PDFs are written
--db-path SQLite database for pipeline memory
--recent-runs Number of runs to show in audit output


Example:


mk1-doc-conversion.exe
--source C:\contracts
--pdf-output C:\contracts\pdf


---

# Example Output


RUN RESULT
run_id: 57
status: CONVERSION_RUN_COMPLETE
planned_total_count: 5
planned_convert_count: 0
planned_skip_count: 5

SKIPPED
SKIP | contract1.docx | reason=unchanged_already_converted
SKIP | contract2.docx | reason=unchanged_already_converted


---

# Architecture

Pipeline stages:

1. Inventory
   - Recursive file discovery

2. Fingerprinting
   - SHA256 hash calculation

3. Registry Decision Engine
   - Determines convert vs skip

4. Conversion
   - LibreOffice headless conversion

5. Audit + Manifest
   - SQLite run history
   - Run manifest emitted per execution

---

# Idempotent Behavior

Files that have already been converted are skipped automatically unless their content changes.

Example:


first run → converts files
second run → skips unchanged files


This prevents unnecessary reprocessing and provides deterministic ingestion behavior.

---

# Future Direction

The current pipeline converts documents to PDF because the downstream system requires PDF ingestion.

Future enhancements may include:

- conversion directly to LLM search context
- structured document extraction
- document chunking pipelines
- vector indexing

The ingestion pipeline architecture already supports these future stages.

---

# Repository

https://github.com/mcstabface/mk1-doc-conversion
