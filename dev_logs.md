# Development Logs - Adobe to BigQuery ETL

## Phase 1: Project Initialization & Structure
- [x] Initialize Git repository.
- [x] Define project directory structure.
- [x] Create `requirements.txt` with essential dependencies (`aanalytics2`, `google-cloud-bigquery`, `pandas`, `pyarrow`).

## Phase 2: Extraction (Adobe Analytics)
- [x] Refactor `AdobeAnalyticsConnector` to be fully standalone.
- [x] Ensure robust OAuth2 token management and pagination for large datasets.
- [x] Implement a method to extract data directly into a Pandas DataFrame.

## Phase 3: Loading (Google BigQuery)
- [x] Create a `BigQueryHandler` class in `tools/bigquery/loader.py`.
- [x] Implement `df_to_bq` logic using the `google-cloud-bigquery` library.
- [x] Handle schema auto-detection and table creation.

## Phase 4: Integration & CLI
- [x] Refactor `main.py` into a CLI tool (removing GCF/Firestore dependencies).
- [ ] Implement configuration management via environment variables or `.env` file.
- [ ] Add logging and error handling.

## Phase 5: Deployment & Documentation
- [x] Finalize `README.md` with setup and usage instructions.
- [ ] Link and push to GitHub repository.
