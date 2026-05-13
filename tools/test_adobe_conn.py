import os
import asyncio
import aiohttp
import json
import logging
import argparse
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AdobeTest")

class AdobeTester:
    def __init__(self, credentials: Dict[str, str]):
        self.client_id = credentials.get("ADOBE_CLIENT_ID")
        self.client_secret = credentials.get("ADOBE_CLIENT_SECRET")
        self.refresh_token = credentials.get("ADOBE_REFRESH_TOKEN")
        self.org_id = credentials.get("ADOBE_ORG_ID")
        self.company_id = credentials.get("ADOBE_COMPANY_ID")
        self.access_token = None
        self.base_url = "https://analytics.adobe.io/api"

    async def get_access_token(self):
        """Attempts to get a token via refresh_token or client_credentials."""
        url = "https://ims-na1.adobelogin.com/ims/token/v3"
        
        # Flow 1: Refresh Token
        if self.refresh_token and self.client_id and self.client_secret:
            logger.info("Attempting Refresh Token Flow...")
            payload = {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.access_token = data.get("access_token")
                        logger.info("Access token obtained successfully via refresh_token.")
                        return self.access_token
                    else:
                        txt = await resp.text()
                        logger.error(f"Refresh token flow failed ({resp.status}): {txt}")

        # Flow 2: Client Credentials
        if self.client_id and self.client_secret:
            logger.info("Attempting Client Credentials Flow...")
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "openid,AdobeID,read_organizations,additional_info.projectedProductContext,https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk"
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.access_token = data.get("access_token")
                        logger.info("Access token obtained successfully via client_credentials.")
                        return self.access_token
                    else:
                        txt = await resp.text()
                        logger.error(f"Client credentials flow failed ({resp.status}): {txt}")
        
        raise Exception("Authentication failed. Check your credentials.")

    def get_headers(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "x-api-key": self.client_id
        }
        if self.org_id:
            headers["x-gw-ims-org-id"] = self.org_id
        return headers

    async def test_discovery(self):
        """Tests the Discovery API (me)."""
        if not self.access_token: await self.get_access_token()
        url = "https://analytics.adobe.io/discovery/me"
        headers = self.get_headers()
        
        logger.info(f"Testing Discovery API: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
                if resp.status == 200:
                    logger.info("Discovery API SUCCESS.")
                    print(json.dumps(data, indent=2))
                    return data
                else:
                    logger.error(f"Discovery API Error ({resp.status}): {await resp.text()}")
                    return None

    async def test_suites(self, company_id: str = None):
        """Tests the Collections API (Suites)."""
        if not self.access_token: await self.get_access_token()
        c_id = company_id or self.company_id
        if not c_id:
            logger.error("No Global Company ID (ADOBE_COMPANY_ID) provided.")
            return None
            
        url = f"{self.base_url}/{c_id}/collections/suites"
        headers = self.get_headers()
        
        logger.info(f"Testing Suites API: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
                if resp.status == 200:
                    logger.info("Suites API SUCCESS.")
                    suites = data.get("content", [])
                    print(f"Found {len(suites)} suites.")
                    for suite in suites:
                        print(f" - {suite.get('name')} ({suite.get('rsid')})")
                    return suites
                else:
                    logger.error(f"Suites API Error ({resp.status}): {await resp.text()}")
                    return None

    async def test_report(self, rsid: str, start: str = "2024-03-01T00:00:00", end: str = "2024-03-07T23:59:59"):
        """Tests the Reports API."""
        if not self.access_token: await self.get_access_token()
        if not self.company_id:
            logger.error("No Global Company ID (ADOBE_COMPANY_ID) provided.")
            return None
            
        url = f"{self.base_url}/{self.company_id}/reports"
        headers = self.get_headers()
        headers["Content-Type"] = "application/json"
        
        # Simple report payload
        payload = {
            "rsid": rsid,
            "globalFilters": [{"type": "dateRange", "dateRange": f"{start}/{end}"}],
            "metricContainer": {
                "metrics": [{"id": "metrics/visits"}, {"id": "metrics/pageviews"}]
            },
            "dimension": "variables/daterangeday",
            "settings": {"limit": 10, "page": 0}
        }
        
        logger.info(f"Testing Report API (RSID: {rsid}): {url}")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status == 200:
                    logger.info("Report API SUCCESS.")
                    print(json.dumps(data, indent=2))
                    return data
                else:
                    logger.error(f"Report API Error ({resp.status}): {await resp.text()}")
                    return None

async def main():
    parser = argparse.ArgumentParser(description="Adobe Analytics Connection Tester")
    parser.add_argument("--test", choices=["discovery", "suites", "report", "all"], default="all")
    parser.add_argument("--company", help="Global Company ID (overrides .env)")
    parser.add_argument("--rsid", help="Report Suite ID (overrides .env)")
    args = parser.parse_args()

    load_dotenv()
    creds = {
        "ADOBE_CLIENT_ID": os.getenv("ADOBE_CLIENT_ID"),
        "ADOBE_CLIENT_SECRET": os.getenv("ADOBE_CLIENT_SECRET"),
        "ADOBE_REFRESH_TOKEN": os.getenv("ADOBE_REFRESH_TOKEN"),
        "ADOBE_ORG_ID": os.getenv("ADOBE_ORG_ID"),
        "ADOBE_COMPANY_ID": args.company or os.getenv("ADOBE_COMPANY_ID")
    }
    
    tester = AdobeTester(creds)
    
    try:
        if args.test in ["discovery", "all"]:
            discovery_data = await tester.test_discovery()
            # Try to infer company_id if not provided
            if not tester.company_id and discovery_data:
                try:
                    ims_orgs = discovery_data.get("imsOrgs") or []
                    for org in ims_orgs:
                        companies = org.get("companies") or []
                        if companies:
                            tester.company_id = companies[0].get("globalCompanyId")
                            logger.info(f"Auto-inferred Company ID: {tester.company_id}")
                            break
                except: pass

        if args.test in ["suites", "all"]:
            await tester.test_suites()

        if args.test in ["report", "all"]:
            rsid = args.rsid or os.getenv("ADOBE_RSID")
            if rsid:
                await tester.test_report(rsid)
            else:
                logger.warning("Skipping report test: No RSID provided.")

    except Exception as e:
        logger.error(f"Test execution failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
