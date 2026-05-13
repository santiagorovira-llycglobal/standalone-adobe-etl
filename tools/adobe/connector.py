import json
import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

import aanalytics2 as api2
import pandas as pd
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)

class AdobeAnalyticsConnector:
    """
    Connector for Adobe Analytics API 2.0.
    Handles authentication, token management, and basic API interactions
    using the aanalytics2 library.
    """
    
    def __init__(self, credentials_dict: Dict[str, Any]):
        """
        Initializes the connector with OAuth credentials.
        Credentials should contain 'client_id', 'client_secret', 'org_id'.
        Optionally, 'access_token', 'refresh_token', 'token_expiry' can be provided
        for existing sessions. These are assumed to be already decrypted.
        """
        self._credentials_dict = credentials_dict.copy() # Store a mutable copy
        self.client_id = self._credentials_dict.get("client_id")
        self.client_secret = self._credentials_dict.get("client_secret")
        self.org_id = self._credentials_dict.get("org_id")
        
        if not all([self.client_id, self.client_secret, self.org_id]):
            raise ValueError("Missing Adobe Analytics client_id, client_secret, or org_id in credentials.")

        # Initialize aanalytics2 Login object
        self._login_instance: Optional[api2.Login] = None
        self._analytics_instance: Optional[api2.Analytics] = None

        # Set aanalytics2's internal config for the current session
        SCOPES = "openid,read_organizations,additional_info.projectedProductContext,additional_info.job_function,analytics.api_2.0"
        api2.configure(
            client_id=self.client_id,
            secret=self.client_secret,
            org_id=self.org_id,
            tech_id=self.client_id, # Add tech_id here
            scopes=SCOPES # Add scopes here
        )

        # Initialize Login instance
        # If tokens are provided, pass them to Login()
        if "access_token" in self._credentials_dict and "refresh_token" in self._credentials_dict:
            token_expiry = None
            if "token_expiry" in self._credentials_dict and self._credentials_dict["token_expiry"]:
                try:
                    token_expiry = datetime.fromisoformat(self._credentials_dict["token_expiry"])
                except ValueError:
                    logger.warning("Invalid token_expiry format, proceeding without it.")

            self._login_instance = api2.Login(
                client_id=self.client_id,
                secret=self.client_secret,
                org_id=self.org_id,
                token=self._credentials_dict["access_token"],
                refresh_token=self._credentials_dict["refresh_token"],
                token_expiry=token_expiry
            )
            logger.info("Adobe Analytics connector initialized with existing tokens.")
        else:
            # No tokens provided, perform fresh login
            self._login_instance = api2.Login(
                client_id=self.client_id,
                secret=self.client_secret,
                org_id=self.org_id
            )
            logger.info("Adobe Analytics connector initialized for fresh login (OAuth flow).")

        
        # Ensure login is successful and token is current
        self._update_tokens_from_login()
        
        # Initialize Analytics instance after successful login
        self._analytics_instance = api2.Analytics(self._login_instance.getGlobalCompanyId())

    def _update_tokens_from_login(self) -> None:
        """
        Retrieves the latest token info from aanalytics2.Login and updates
        the internal credentials dictionary. Assumes aanalytics2 has handled refresh.
        """
        if self._login_instance is None:
            raise RuntimeError("Adobe Analytics Login instance not initialized.")

        try:
            token_info = self._login_instance.get_token_info()
            self._credentials_dict["access_token"] = token_info.get("access_token")
            self._credentials_dict["refresh_token"] = token_info.get("refresh_token")
            self._credentials_dict["token_expiry"] = token_info.get("expires_at", datetime.utcnow()).isoformat()
            logger.debug("Adobe Analytics internal credentials_dict updated with latest token info.")
        except Exception as e:
            logger.error(f"Error retrieving latest Adobe Analytics token info: {e}")
            raise

    @property
    def updated_credentials(self) -> Dict[str, Any]:
        """
        Returns the current internal credentials dictionary,
        including potentially refreshed tokens. These are decrypted.
        """
        return self._credentials_dict.copy()

    @property
    def analytics_client(self) -> api2.Analytics:
        """Returns the aanalytics2 Analytics client instance, ensuring token is fresh."""
        # aanalytics2 internally handles token refresh when API calls are made if Login()
        # was initialized with refresh_token. We just need to update our stored tokens after.
        # This will be handled by the service layer after calling a connector method.
        if self._analytics_instance is None:
            raise RuntimeError("Adobe Analytics instance not initialized.")
        return self._analytics_instance

    def list_companies(self) -> List[Dict[str, Any]]:
        """
        Lists accessible companies.
        Returns a list of dictionaries with 'globalCompanyId' and 'companyName'.
        """
        companies = self.analytics_client.getCompanyId()
        self._update_tokens_from_login() # Update tokens after successful API call
        return [
            {"global_company_id": c.get('globalCompanyId'), "company_name": c.get('companyName')}
            for c in companies
        ]

    def list_report_suites(self, global_company_id: str) -> List[Dict[str, Any]]:
        """
        Lists report suites for a given global company ID.
        Returns a list of dictionaries with 'rsid' and 'name'.
        """
        # aanalytics2.Analytics is initialized with a global_company_id, so this method implicitly uses it
        report_suites = self.analytics_client.getReportSuites()
        self._update_tokens_from_login() # Update tokens after successful API call
        return [
            {"rsid": rs.get('rsid'), "name": rs.get('name')}
            for rs in report_suites
        ]

    def get_dimensions(self, rsid: str) -> List[Dict[str, Any]]:
        """
        Get available dimensions for a report suite.
        Returns a list of dictionaries with 'id' and 'name'.
        """
        dimensions = self.analytics_client.getDimensions(reportSuiteID=rsid)
        self._update_tokens_from_login() # Update tokens after successful API call
        return [
            {"id": d.get('id'), "name": d.get('name'), "category": d.get('category')}
            for d in dimensions
        ]
    
    def get_metrics(self, rsid: str) -> List[Dict[str, Any]]:
        """
        Get available metrics for a report suite.
        Returns a list of dictionaries with 'id' and 'name'.
        """
        metrics = self.analytics_client.getMetrics(reportSuiteID=rsid)
        self._update_tokens_from_login() # Update tokens after successful API call
        return [
            {"id": m.get('id'), "name": m.get('name'), "category": m.get('category')}
            for m in metrics
        ]

    def run_report(
        self,
        rsid: str,
        metrics: List[str],
        dimension: str,
        date_range: Dict[str, str],
        filters: Optional[List[Dict]] = None,
        limit: int = 50000
    ) -> pd.DataFrame:
        """
        Run a report and return as DataFrame.
        This method is intended to be called by the GCF for heavy lifting.
        """
        report_request_payload = {
            "rsid": rsid,
            "globalFilters": [
                {
                    "type": "dateRange",
                    "dateRange": f"{date_range['start_date']}T00:00:00/{date_range['end_date']}T23:59:59"
                }
            ],
            "metricContainer": {
                "metrics": [{"id": m} for m in metrics]
            },
            "dimension": dimension,
            "settings": {
                "limit": limit,
                "page": 0,
                "dimensionSort": "asc"
            }
        }
        
        # Add filters if provided
        if filters:
            # Assuming filters are in a format compatible with aanalytics2/Adobe API
            # This part might need further refinement based on actual filter structure
            report_request_payload["dimensionFilter"] = {
                "type": "segment",
                "segmentId": "s123_abc", # Placeholder, needs actual segment ID or inline definition
            }
            logger.warning("Filtering not fully implemented yet for Adobe Analytics report generation.")

        # aanalytics2 handles pagination internally for getReport if needed, but we'll use a wrapper
        # in GCF for explicit control and progress tracking for large reports.
        # This run_report here primarily prepares and executes a single API call for a given payload.
        report_response = self.analytics_client.getReport(report_request_payload)
        self._update_tokens_from_login() # Update tokens after successful API call
        
        if report_response and 'data' in report_response:
            return report_response['data']
        else:
            return pd.DataFrame()

    def fetch_report_with_pagination(
        self,
        rsid: str,
        metrics: List[str],
        dimension: str,
        date_range: Dict[str, str],
        filters: Optional[List[Dict]] = None,
        initial_limit: int = 50000
    ) -> pd.DataFrame:
        """
        Fetch complete report with automatic pagination.
        This method is designed to be called by the GCF.
        """
        all_data = pd.DataFrame()
        page = 0
        
        while True:
            report_request_payload = {
                "rsid": rsid,
                "globalFilters": [
                    {
                        "type": "dateRange",
                        "dateRange": f"{date_range['start_date']}T00:00:00/{date_range['end_date']}T23:59:59"
                    }
                ],
                "metricContainer": {
                    "metrics": [{"id": m} for m in metrics]
                },
                "dimension": dimension,
                "settings": {
                    "limit": initial_limit,
                    "page": page,
                    "dimensionSort": "asc"
                }
            }
            
            if filters:
                # Placeholder for filter integration
                report_request_payload["dimensionFilter"] = {
                    "type": "segment",
                    "segmentId": "s123_abc",
                }

            try:
                # Use aanalytics2._makeRequest for low-level control to handle pagination manually
                response = self.analytics_client._makeRequest(
                    method="POST",
                    endpoint=f"{self.analytics_client.endpoint}/{self.analytics_client.company_id}/reports",
                    data=report_request_payload
                )
                
                # Check for empty data or no rows
                if not response or 'rows' not in response or not response['rows']:
                    break
                
                # Directly convert response to DataFrame for concatenation
                current_page_df = pd.json_normalize(response['rows'])
                all_data = pd.concat([all_data, current_page_df], ignore_index=True)
                
                # Adobe API 2.0 uses 'lastPage' flag in response for pagination termination
                if response.get('lastPage', False):
                    break
                
                page += 1
                
                # Implement delay for rate limiting if needed, though aanalytics2 has some internal handling
                time.sleep(1) # Small delay between page fetches
                
            except HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit. aanalytics2 should handle this internally, but GCF might need broader retry logic.")
                    time.sleep(10) # Manual backoff for demonstration
                    continue
                else:
                    logger.error(f"HTTPError fetching paginated report: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error fetching paginated report: {e}")
                raise
            
            self._update_tokens_from_login() # Update tokens after successful API call
                
        return all_data

    # Placeholder for BigQuery sync, to be implemented in GCF
    def sync_to_bq(
        self,
        data: pd.DataFrame,
        project_id: str,
        table_name: str,
        credentials: Optional[Any] = None
    ) -> bool:
        """
        Placeholder for syncing data to BigQuery.
        This function will be primarily executed within a GCF.
        """
        logger.info(f"Syncing {len(data)} rows to BigQuery table {table_name} in project {project_id}.")
        # Actual implementation will use google-cloud-bigquery client
        return True # Simulate success for now
