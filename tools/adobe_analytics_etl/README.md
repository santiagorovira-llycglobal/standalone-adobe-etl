# Adobe Analytics ETL Google Cloud Function

## Overview (English)
This Google Cloud Function (GCF) is responsible for the Extract, Transform, Load (ETL) process for Adobe Analytics reports. It is triggered by an HTTP request, typically from the AIMA 2.0 Streamlit frontend via the `AdobeAnalyticsService`. The function fetches report data from the Adobe Analytics API (handling pagination and rate limits), and then loads the processed data into Google BigQuery. It also updates job status in Firestore.

## Descripción General (Español)
Esta Google Cloud Function (GCF) es responsable del proceso de Extracción, Transformación y Carga (ETL) para los informes de Adobe Analytics. Se activa mediante una solicitud HTTP, típicamente desde el frontend de Streamlit de AIMA 2.0 a través del `AdobeAnalyticsService`. La función obtiene datos de informes de la API de Adobe Analytics (manejando la paginación y los límites de tasa), y luego carga los datos procesados en Google BigQuery. También actualiza el estado del trabajo en Firestore.

## Functionality / Funcionalidad
-   **HTTP Trigger**: Activated by an incoming HTTP POST request containing report parameters and connection details.
-   **Credential Management**: Retrieves and decrypts Adobe Analytics credentials securely from Firestore.
-   **Report Fetching**: Utilizes the `AdobeAnalyticsConnector` to fetch data from the Adobe Analytics API, automatically handling pagination for large datasets.
-   **BigQuery Synchronization**: Loads the fetched data into a specified BigQuery table, including schema inference and data appending.
-   **Job Status Tracking**: Updates job status (pending, in_progress, completed, failed) in Firestore, allowing the frontend to monitor progress.
-   **Error Handling**: Catches and logs errors during the ETL process, updating job status accordingly.

## Deployment / Despliegue
This function is designed to be deployed as a Google Cloud Function (2nd Gen) with an HTTP trigger.

### Required Environment Variables / Variables de Entorno Requeridas
-   `GCP_PROJECT_ID`: The Google Cloud Project ID where BigQuery and Firestore are located.
-   `ADOBE_ANALYTICS_ETL_GCF_URL`: (This GCF's own URL, used by calling services).

### Required `requirements.txt` / `requirements.txt` Requerido
See `functions/adobe_analytics_etl/requirements.txt` for the full list of Python dependencies.

## Usage / Uso
The `master_router` function is the entry point for HTTP requests. It expects a JSON payload conforming to the `ReportRequest` Pydantic model, along with a `job_id` and `user_email`.
