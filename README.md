# Adobe Analytics to BigQuery ETL

This repository contains a standalone Python-based ETL pipeline designed to extract reporting data from Adobe Analytics API 2.0 and load it directly into Google BigQuery.

## 📂 Repository Structure

```text
standalone_adobe_etl/
├── tools/
│   ├── adobe/
│   │   └── connector.py       # Adobe Analytics API 2.0 Wrapper
│   ├── bigquery/
│   │   └── loader.py          # BigQuery Client and Data Loader
│   ├── main.py                # Main entry point (CLI/Job Runner)
│   └── requirements.txt       # Project dependencies
├── dev_logs.md                # Development progress and roadmap
└── README.md                  # Project documentation (this file)
```

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Adobe Analytics API Credentials (Client ID, Secret, Org ID)
- Google Cloud Service Account with BigQuery Data Editor permissions

### Installation
1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r tools/requirements.txt
   ```

### Usage
Run the ETL process using:
```bash
python tools/main.py --config config.json
```

## 🛠 Features
- **Adobe API 2.0 Integration:** Full support for modern Adobe Analytics reporting.
- **Auto-Pagination:** Automatically handles large reports by paginating through result sets.
- **BigQuery Integration:** Efficiently loads DataFrames into BigQuery using the `google-cloud-bigquery` library.
- **Flexible Schema:** Supports schema auto-detection for BigQuery tables.
