"""
Pydantic models for Adobe Analytics integration.
Modelos Pydantic para la integración de Adobe Analytics.
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class DateRange(BaseModel):
    """
    Represents a date range for reports.
    Representa un rango de fechas para los informes.
    """
    start_date: str = Field(..., description="Start date in 'YYYY-MM-DD' format or 'NdaysAgo'. / Fecha de inicio en formato 'YYYY-MM-DD' o 'NdaysAgo'.")
    end_date: str = Field(..., description="End date in 'YYYY-MM-DD' format or 'today'. / Fecha de fin en formato 'YYYY-MM-DD' o 'today'.")


class AdobeConnection(BaseModel):
    """
    Model for storing Adobe Analytics connection details in Firestore.
    Modelo para almacenar los detalles de conexión de Adobe Analytics en Firestore.
    """
    connection_id: str
    user_email: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    auth_type: str # e.g., "oauth_server_to_server"
    
    # Encrypted credentials
    client_id: str
    client_secret: str # Encrypted
    org_id: str
    access_token: Optional[str] = None # Encrypted
    refresh_token: Optional[str] = None # Encrypted
    token_expiry: Optional[datetime] = None # ISO format string or datetime
    
    selected_company: Optional[Dict[str, str]] = None # {"global_company_id": "...", "company_name": "..."}
    selected_report_suites: List[Dict[str, str]] = Field(default_factory=list) # [{"rsid": "...", "name": "..."}]
    
    permissions: Dict[str, List[str]] = Field(default_factory=lambda: {"admins": [], "viewers": []})


class Metric(BaseModel):
    """
    Represents an Adobe Analytics metric.
    Representa una métrica de Adobe Analytics.
    """
    id: str = Field(..., description="Adobe Analytics metric ID (e.g., 'metrics/visits'). / ID de métrica de Adobe Analytics.")
    name: str = Field(..., description="Display name of the metric (e.g., 'Visits'). / Nombre de visualización de la métrica.")
    category: Optional[str] = None


class Dimension(BaseModel):
    """
    Represents an Adobe Analytics dimension.
    Representa una dimensión de Adobe Analytics.
    """
    id: str = Field(..., description="Adobe Analytics dimension ID (e.g., 'variables/daterangeday'). / ID de dimensión de Adobe Analytics.")
    name: str = Field(..., description="Display name of the dimension (e.g., 'Day'). / Nombre de visualización de la dimensión.")
    category: Optional[str] = None


class ReportRequest(BaseModel):
    """
    Request payload for running an Adobe Analytics report.
    Payload de solicitud para ejecutar un informe de Adobe Analytics.
    """
    connection_id: str = Field(..., description="ID of the stored Adobe connection to use. / ID de la conexión de Adobe almacenada a utilizar.")
    rsid: str = Field(..., description="Adobe Analytics Report Suite ID. / ID de Report Suite de Adobe Analytics.")
    metrics: List[str] = Field(..., description="List of Adobe Analytics metric IDs. / Lista de IDs de métricas de Adobe Analytics.")
    dimension: str = Field(..., description="Adobe Analytics dimension ID. / ID de dimensión de Adobe Analytics.")
    date_range: DateRange = Field(..., description="Date range for the report. / Rango de fechas para el informe.")
    filters: Optional[List[Dict]] = Field(None, description="Optional filters for the report. / Filtros opcionales para el informe.")
    limit: int = Field(50000, description="Max number of rows per page. / Número máximo de filas por página.")


class ReportResponse(BaseModel):
    """
    Response structure for an Adobe Analytics report, typically from GCF.
    Estructura de respuesta para un informe de Adobe Analytics, típicamente desde GCF.
    """
    status: str = Field(..., description="Status of the report generation (e.g., 'pending', 'completed', 'failed'). / Estado de la generación del informe.")
    report_id: Optional[str] = Field(None, description="ID for tracking the asynchronous report generation. / ID para rastrear la generación asíncrona del informe.")
    message: Optional[str] = Field(None, description="Descriptive message about the report status. / Mensaje descriptivo sobre el estado del informe.")
    data: Optional[List[Dict[str, Any]]] = Field(None, description="The report data, if status is 'completed'. / Los datos del informe, si el estado es 'completed'.")
    bq_table_path: Optional[str] = Field(None, description="Path to BigQuery table where data is stored, if applicable. / Ruta a la tabla de BigQuery donde se almacenan los datos.")
    error: Optional[str] = Field(None, description="Error message, if status is 'failed'. / Mensaje de error, si el estado es 'failed'.")
