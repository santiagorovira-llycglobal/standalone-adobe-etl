import os
import argparse
import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

from adobe.connector import AdobeAnalyticsConnector
from bigquery.loader import BigQueryLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("adobe_etl")

def run_etl(config: Dict[str, Any]):
    """
    Executes the Adobe to BigQuery ETL process.
    """
    # 1. Adobe Credentials & Connection
    adobe_creds = {
        "client_id": os.getenv("ADOBE_CLIENT_ID"),
        "client_secret": os.getenv("ADOBE_CLIENT_SECRET"),
        "org_id": os.getenv("ADOBE_ORG_ID")
    }
    
    if not all(adobe_creds.values()):
        raise ValueError("Missing Adobe credentials in environment variables. Ensure ADOBE_CLIENT_ID, ADOBE_CLIENT_SECRET, and ADOBE_ORG_ID are set in your .env file.")
    
    logger.info("Initializing Adobe Analytics Connector...")
    connector = AdobeAnalyticsConnector(adobe_creds)

    # 2. Extract Data
    report_config = config.get("report_config", {})
    rsid = report_config.get("rsid")
    dimension = report_config.get("dimension")
    metrics = report_config.get("metrics")
    date_range = report_config.get("date_range")

    if not all([rsid, dimension, metrics, date_range]):
        raise ValueError("Missing required report configuration (rsid, dimension, metrics, or date_range).")

    logger.info(f"Fetching report from Adobe for RSID: {rsid}...")
    df = connector.fetch_report_with_pagination(
        rsid=rsid,
        metrics=metrics,
        dimension=dimension,
        date_range=date_range,
        initial_limit=report_config.get("limit", 50000)
    )

    if df.empty:
        logger.info("No data returned from Adobe Analytics. ETL process finished.")
        return

    logger.info(f"Extracted {len(df)} rows from Adobe.")

    # 3. Load to BigQuery
    bq_config = config.get("bigquery_config", {})
    project_id = bq_config.get("project_id") or os.getenv("GCP_PROJECT_ID")
    dataset_id = bq_config.get("dataset_id")
    table_id = bq_config.get("table_id") or f"adobe_analytics_{rsid.lower().replace('-', '_')}"

    if not all([project_id, dataset_id]):
        raise ValueError("Missing BigQuery project_id or dataset_id.")

    logger.info(f"Initializing BigQuery Loader for project {project_id}...")
    loader = BigQueryLoader(project_id=project_id, credentials_path=bq_config.get("credentials_path"))
    
    logger.info(f"Loading data into BigQuery: {dataset_id}.{table_id}...")
    loader.load_dataframe(
        df=df,
        dataset_id=dataset_id,
        table_id=table_id,
        write_disposition=bq_config.get("write_disposition", "WRITE_APPEND")
    )
    logger.info("ETL process completed successfully.")

def main():
    parser = argparse.ArgumentParser(description="Adobe Analytics to BigQuery ETL Tool")
    parser.add_argument("--config", help="Path to JSON configuration file", required=True)
    args = parser.parse_args()

    load_dotenv()

    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
        run_etl(config)
    except Exception as e:
        logger.error(f"ETL Job failed: {e}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()
