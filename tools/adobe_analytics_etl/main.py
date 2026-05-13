import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import functions_framework
from cloudevents.http import CloudEvent

from shared.auth_utils import get_adobe_credentials
from shared.connectors.adobe.connector import AdobeAnalyticsConnector
from shared.models.adobe_analytics import ReportRequest
from shared.bq_sync_utils import BQSyncManager
from shared.firestor_client import FirestoreClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@functions_framework.http
async def master_router(request: CloudEvent) -> Dict[str, Any]:
    """
    Main entry point for the Adobe Analytics ETL Cloud Function.
    Handles report fetching, pagination, and BigQuery synchronization.
    """
    logger.info("Received request for Adobe Analytics ETL GCF.")
    
    job_id = None
    user_email = "system" # Default user for GCF if not explicitly passed
    try:
        # Initialize Firestore client
        firestore_client = FirestoreClient()
        # Parse the request body
        request_json = request.get_json()
        
        job_id = request_json.get("job_id")
        user_email = request_json.get("user_email", user_email) # Get user email from payload
        report_request_data = ReportRequest(**request_json)

        if not job_id:
            job_id = f"adobe-etl-{datetime.utcnow().timestamp()}" # Generate if not provided
            logger.warning(f"No job_id provided, generated: {job_id}")

        # Update job status to 'in_progress'
        await firestore_client.update_document(
            collection_name="adobe_analytics_jobs",
            document_id=job_id,
            updates={"status": "in_progress", "started_at": datetime.utcnow().isoformat()}
        )

        logger.info(f"Processing Adobe Analytics report job {job_id} for connection {report_request_data.connection_id}.")

        # 1. Retrieve Adobe Credentials
        connection_data = await get_adobe_credentials(user_email, report_request_data.connection_id)
        if not connection_data:
            raise ValueError(f"Adobe connection '{report_request_data.connection_id}' not found for user '{user_email}'.")
        
        # 2. Initialize Adobe Analytics Connector
        connector = AdobeAnalyticsConnector(connection_data)

        # 3. Fetch Report Data with Pagination
        logger.info(f"Fetching report for RSID: {report_request_data.rsid}, Dimension: {report_request_data.dimension}, Metrics: {report_request_data.metrics}")
        report_df = connector.fetch_report_with_pagination(
            rsid=report_request_data.rsid,
            metrics=report_request_data.metrics,
            dimension=report_request_data.dimension,
            date_range=report_request_data.date_range.model_dump(),
            filters=report_request_data.filters,
            initial_limit=report_request_data.limit
        )
        
        if report_df.empty:
            message = "Report fetched successfully, but returned no data."
            logger.info(message)
            await firestore_client.update_document(
                collection_name="adobe_analytics_jobs",
                document_id=job_id,
                updates={"status": "completed", "message": message, "completed_at": datetime.utcnow().isoformat()}
            )
            return {"status": "completed", "message": message}

        logger.info(f"Report fetched. Total rows: {len(report_df)}")

        # 4. BigQuery Synchronization
        bq_sync_util = BQSyncManager()
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable not set.")
        
        # Construct BQ table name
        table_name = f"adobe_analytics_{report_request_data.rsid.lower().replace('-', '_')}_data"
        full_table_path = f"{project_id}.adobe_analytics_reports.{table_name}" # Assuming dataset 'adobe_analytics_reports'
        
        logger.info(f"Syncing data to BigQuery table: {full_table_path}")
        await bq_sync_util.df_to_bq(
            df=report_df,
            project_id=project_id,
            dataset_id="adobe_analytics_reports", # Define a consistent dataset
            table_id=table_name,
            if_exists="append", # Append new data
            schema_mapping=None # Let BQ infer schema, or define explicitly if needed
        )
        logger.info("Data successfully synced to BigQuery.")

        # 5. Update Job Status to 'completed'
        await firestore_client.update_document(
            collection_name="adobe_analytics_jobs",
            document_id=job_id,
            updates={
                "status": "completed",
                "message": "Report generated and data synced to BigQuery.",
                "bq_table_path": full_table_path,
                "completed_at": datetime.utcnow().isoformat(),
                "row_count": len(report_df)
            }
        )

        return {"status": "completed", "bq_table_path": full_table_path}, 200

    except Exception as e:
        logger.error(f"Error processing Adobe Analytics ETL job {job_id}: {e}", exc_info=True)
        # Update job status to 'failed'
        await firestore_client.update_document(
            collection_name="adobe_analytics_jobs",
            document_id=job_id,
            updates={"status": "failed", "error": str(e), "failed_at": datetime.utcnow().isoformat()}
        )
        return {"status": "failed", "error": str(e)}, 500 # Return 500 status code for errors
