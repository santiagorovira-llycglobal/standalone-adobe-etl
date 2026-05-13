import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock, MagicMock
import sys
import os

# Ensure app is importable
sys.path.insert(0, os.path.abspath("backend"))

from app.main import app
from app.routers.mcp_analytics import get_current_user

client = TestClient(app)

# Force override
app.dependency_overrides[get_current_user] = lambda: "prod_tester@llyc.global"

AUTH_HEADERS = {"Authorization": "Bearer dummy_token"}

@pytest.fixture
def adobe_session():
    session_id = "prod_test_adobe_session"
    adobe_creds = {
        "client_id": "prod_id",
        "client_secret": "prod_secret",
        "org_id": "prod_org"
    }
    
    with patch("app.routers.mcp_analytics.session_service.get_session") as mock_get:
        mock_get.return_value = {
            "provider": "adobe",
            "adobe_credentials": adobe_creds,
            "user_email": "prod_tester@llyc.global"
        }
        yield session_id

@pytest.mark.asyncio
async def test_prod_adobe_routing_traffic_ia(adobe_session):
    with patch("app.routers.mcp_analytics.AdobeAnalyticsService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.provider = "adobe"
        mock_instance.analyze_traffic_ia = AsyncMock(return_value={
            "battle_of_ais": [],
            "behavioral_clusters": {"methodology": "test", "definitions": {}, "distribution": {}},
            "inferred_traffic": {"total_sessions": 0, "confidence_index": "Low", "breakdown_by_channel": {}, "avg_duration": 0, "pages_per_session": 0, "conversion_rate": 0, "engagement_score": 0, "top_sources": []},
            "content_affinity": [],
            "total_sessions": 0,
            "non_ia_sessions": 0,
            "daily_trend": [],
            "raw_data_summary": {"source": "Adobe Real Logic"}
        })
        
        response = client.post(
            f"/api/v1/mcp-analytics/traffic-ia?session_id={adobe_session}", 
            json={"property_id": "rsid123", "start_date": "7daysAgo", "end_date": "today", "session_id": adobe_session},
            headers=AUTH_HEADERS
        )
        
        assert response.status_code == 200
        assert response.json()["raw_data_summary"]["source"] == "Adobe Real Logic"

@pytest.mark.asyncio
async def test_prod_adobe_routing_risk(adobe_session):
    with patch("app.routers.mcp_analytics.AdobeAnalyticsService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.provider = "adobe"
        mock_instance.analyze_risk = AsyncMock(return_value={"risk_score": 42})
        
        response = client.post(
            f"/api/v1/mcp-analytics/risk-analysis?session_id={adobe_session}", 
            json={"property_id": "rsid123", "start_date": "7daysAgo", "end_date": "today", "break_even_roas": 3.0, "session_id": adobe_session},
            headers=AUTH_HEADERS
        )
        
        assert response.status_code == 200
        assert response.json()["risk_score"] == 42

@pytest.mark.asyncio
async def test_prod_adobe_routing_deep_dive(adobe_session):
    with patch("app.routers.mcp_analytics.AdobeAnalyticsService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.provider = "adobe"
        mock_instance.execute_deep_dive = AsyncMock(return_value={"summary": "Adobe Deep Dive"})
        
        response = client.post(
            f"/api/v1/mcp-analytics/deep-dive", 
            json={"property_id": "rsid123", "start_date": "7daysAgo", "end_date": "today", "session_id": adobe_session},
            headers=AUTH_HEADERS
        )
        
        assert response.status_code == 200
        assert response.json()["summary"] == "Adobe Deep Dive"

@pytest.mark.asyncio
async def test_prod_adobe_routing_url_analysis(adobe_session):
    with patch("app.routers.mcp_analytics.AdobeAnalyticsService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.provider = "adobe"
        mock_instance.analyze_url_performance = AsyncMock(return_value={
            "daily_trend": [],
            "url_performance": [],
            "traffic_sources_analysis": [],
            "summary": {"total_sessions": 100, "urls_analyzed": ["/test"]}
        })
        
        response = client.post(
            f"/api/v1/mcp-analytics/traffic-ia/url-analysis?session_id={adobe_session}", 
            json={"property_id": "rsid123", "start_date": "7daysAgo", "end_date": "today", "urls": ["/test"]},
            headers=AUTH_HEADERS
        )
        
        assert response.status_code == 200
        assert response.json()["summary"]["total_sessions"] == 100
