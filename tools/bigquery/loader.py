import logging
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class BigQueryLoader:
    """
    Handles data loading into Google BigQuery.
    """
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        """
        Initializes the BigQuery client.
        :param project_id: Google Cloud Project ID.
        :param credentials_path: Path to the service account JSON key file.
        """
        self.project_id = project_id
        if credentials_path:
            self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(project=project_id, credentials=self.credentials)
        else:
            # Fallback to default environment credentials
            self.client = bigquery.Client(project=project_id)
        
        logger.info(f"BigQueryLoader initialized for project: {project_id}")

    def load_dataframe(
        self, 
        df: pd.DataFrame, 
        dataset_id: str, 
        table_id: str, 
        write_disposition: str = "WRITE_APPEND"
    ) -> bool:
        """
        Loads a Pandas DataFrame into a BigQuery table.
        :param df: DataFrame to load.
        :param dataset_id: Target dataset ID.
        :param table_id: Target table ID.
        :param write_disposition: Specifies action if table exists (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY).
        """
        if df.empty:
            logger.warning("DataFrame is empty. Skipping load to BigQuery.")
            return False

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,
        )

        try:
            logger.info(f"Starting load job for table {table_ref}...")
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for the job to complete.
            logger.info(f"Successfully loaded {len(df)} rows into {table_ref}.")
            return True
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise
