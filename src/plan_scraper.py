"""
Fast Plan Data Scraper

Ultra-aggressive Plan entity scraper for EOZ portal.
Uses id field from pages (not externalId) and includes authentication cookies.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

import httpx
import structlog
from tqdm.asyncio import tqdm

logger = structlog.get_logger()


@dataclass
class PlanScraperConfig:
    """Ultra-aggressive Plan scraping configuration."""

    base_url: str = "https://www.eoz.kz/api/uicommand"
    cookies: str = "_fbp=fb.1.1750532640213.905462364716617920; JSESSIONID=D50DB6CD4666BFBD253E406E9046DF6E"
    max_concurrent: int = 150  # High concurrency for speed
    retry_delays: List[float] = field(default_factory=lambda: [0.1, 0.3, 0.8])
    timeout: int = 30


class PlanScraper:
    """Fast Plan scraper with correct UUID mapping and cookie authentication."""

    def __init__(self, config: Optional[PlanScraperConfig] = None):
        self.config = config or PlanScraperConfig()
        self.session: Optional[httpx.AsyncClient] = None
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._consecutive_errors = 0

        # Ensure directories exist
        Path("raw/pages/Plan").mkdir(parents=True, exist_ok=True)
        Path("raw/objects/Plan").mkdir(parents=True, exist_ok=True)

    def build_plan_payload(self, page: int = 0, length: int = 1000) -> Dict[str, Any]:
        """Build Plan-specific API request payload."""
        return {
            "page": page,
            "entity": "Plan",
            "length": length,
            "filter": {"includeMyTru": 0}
        }

    def get_headers(self) -> Dict[str, str]:
        """Get headers with authentication cookies."""
        return {
            "Cookie": self.config.cookies,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    async def make_request(
        self, path: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Make HTTP request with retries and error handling."""
        async with self.semaphore:
            for attempt, delay in enumerate(self.config.retry_delays):
                try:
                    if self.session is None:
                        self.session = httpx.AsyncClient(
                            timeout=self.config.timeout,
                            limits=httpx.Limits(
                                max_keepalive_connections=50, max_connections=200
                            ),
                        )

                    # Adaptive rate limiting based on error rate
                    if self._consecutive_errors > 10:
                        await asyncio.sleep(0.5)
                    elif self._consecutive_errors > 5:
                        await asyncio.sleep(0.2)
                    else:
                        await asyncio.sleep(0.05)  # Aggressive rate

                    response = await self.session.post(
                        f"{self.config.base_url}{path}",
                        json=payload,
                        headers=self.get_headers(),
                    )
                    response.raise_for_status()

                    # Parse JSON response
                    try:
                        data = response.json()
                        self._consecutive_errors = 0  # Reset on success
                        return data
                    except json.JSONDecodeError:
                        if not response.text.strip():
                            logger.debug(f"Empty response from {path}")
                            self._consecutive_errors += 1
                            raise ValueError("Empty response from server")
                        else:
                            logger.debug(f"Non-JSON response: {response.text[:100]}...")
                            self._consecutive_errors += 1
                            raise ValueError("Invalid JSON response")

                except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as e:
                    self._consecutive_errors += 1
                    if attempt < len(self.config.retry_delays) - 1:
                        logger.debug(
                            f"Request failed, retrying in {delay}s (consecutive errors: {self._consecutive_errors})",
                            error=str(e),
                            attempt=attempt,
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Request failed after all retries", error=str(e))
                        raise

        raise RuntimeError("Request failed unexpectedly")

    async def fetch_plan_pages(self) -> List[Dict[str, Any]]:
        """Fetch all Plan pages with authentication."""
        logger.info("Starting Plan page fetch with authentication")

        pages_data = []
        page = 0

        while True:
            payload = self.build_plan_payload(page)

            try:
                data = await self.make_request("/get/page", payload)

                if not data.get("content"):
                    logger.info(f"No more Plan content at page {page}")
                    break

                # Save raw page data
                page_file = Path(f"raw/pages/Plan/{page}.json")
                page_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))

                pages_data.append(data)
                logger.info(f"Fetched Plan page {page}: {len(data['content'])} records")
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch Plan page {page}", error=str(e))
                break

        total_records = sum(len(p.get("content", [])) for p in pages_data)
        logger.info(f"Completed Plan page fetch: {page} pages, {total_records} total records")
        return pages_data

    async def fetch_plan_object(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """Fetch Plan object using id field (not externalId)."""
        # Check if already exists
        obj_file = Path(f"raw/objects/Plan/{plan_id}.json")
        if obj_file.exists():
            logger.debug(f"Plan object {plan_id} already exists, skipping")
            return None

        for attempt in range(3):
            try:
                payload = {"entity": "Plan", "uuid": plan_id}
                data = await self.make_request("/get/object", payload)

                if not data or not isinstance(data, dict):
                    if attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    else:
                        logger.debug(f"Empty response for Plan object {plan_id}")
                        return None

                # Save raw object data
                obj_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))
                return data

            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.warning(f"Failed to fetch Plan object {plan_id}", error=str(e))
                    return None

        return None

    async def fetch_all_plan_objects(self, pages_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch all Plan objects using id field from pages."""
        # Collect all Plan IDs from pages using 'id' field
        plan_ids = []
        for page_data in pages_data:
            for item in page_data.get("content", []):
                # Use 'id' field as specified, not 'externalId'
                plan_id = item.get("id")
                if plan_id:
                    plan_ids.append(str(plan_id))

        if not plan_ids:
            logger.warning("No Plan IDs found in pages")
            return []

        logger.info(f"Fetching {len(plan_ids)} Plan objects using id field")

        # Adaptive concurrency based on error rate
        if self._consecutive_errors > 20:
            concurrency = 50  # Reduce concurrency
            logger.info(f"Reducing concurrency to {concurrency} due to high error rate")
        else:
            concurrency = 100  # Normal aggressive mode

        obj_semaphore = asyncio.Semaphore(concurrency)

        # Process in batches for memory efficiency
        batch_size = 1000
        all_objects = []

        for i in range(0, len(plan_ids), batch_size):
            batch_ids = plan_ids[i : i + batch_size]
            logger.info(f"Processing Plan batch {i//batch_size + 1}/{(len(plan_ids) + batch_size - 1)//batch_size}")

            # Create semaphore-wrapped tasks
            async def fetch_with_semaphore(plan_id: str):
                async with obj_semaphore:
                    return await self.fetch_plan_object(plan_id)

            tasks = [fetch_with_semaphore(plan_id) for plan_id in batch_ids]

            # Execute batch with progress bar
            batch_objects = []
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Fetching Plan objects batch {i//batch_size + 1}",
            ):
                obj_data = await coro
                if obj_data:
                    batch_objects.append(obj_data)

            all_objects.extend(batch_objects)
            logger.info(f"Batch {i//batch_size + 1} completed: {len(batch_objects)}/{len(batch_ids)} objects fetched")

        logger.info(f"Successfully fetched {len(all_objects)}/{len(plan_ids)} Plan objects")
        return all_objects

    def load_existing_plan_pages(self) -> List[Dict[str, Any]]:
        """Load existing Plan page files from disk."""
        logger.info("Loading existing Plan pages from raw/pages/Plan/")
        
        pages_data = []
        pages_dir = Path("raw/pages/Plan")
        
        if not pages_dir.exists():
            logger.error("Plan pages directory not found: raw/pages/Plan/")
            return []
        
        # Find all page JSON files (0.json, 1.json, etc.)
        page_files = sorted(pages_dir.glob("*.json"), key=lambda x: int(x.stem))
        
        for page_file in page_files:
            try:
                with open(page_file, 'r', encoding='utf-8') as f:
                    page_data = json.load(f)
                    pages_data.append(page_data)
                    logger.debug(f"Loaded {page_file.name}: {len(page_data.get('content', []))} records")
            except Exception as e:
                logger.warning(f"Failed to load {page_file.name}", error=str(e))
        
        total_records = sum(len(p.get("content", [])) for p in pages_data)
        logger.info(f"Loaded {len(pages_data)} Plan pages with {total_records} total records")
        return pages_data

    async def scrape_plans(self) -> Dict[str, Any]:
        """Plan object scraping workflow using existing pages."""
        logger.info("Starting Plan object scraping from existing pages")

        try:
            # Phase 1: Load existing Plan pages
            pages_data = self.load_existing_plan_pages()
            
            if not pages_data:
                raise ValueError("No existing Plan pages found. Please run page scraping first.")

            # Phase 2: Fetch all Plan objects using id field
            objects_data = await self.fetch_all_plan_objects(pages_data)

            result = {
                "entity": "Plan",
                "pages": len(pages_data),
                "objects": len(objects_data),
                "total_records": sum(len(p.get("content", [])) for p in pages_data),
            }

            logger.info("Completed Plan object scraping", **result)
            return result

        except Exception as e:
            logger.error("Failed to scrape Plan objects", error=str(e))
            raise

        finally:
            # Close session
            if self.session:
                await self.session.aclose()


async def main():
    """Main entry point for Plan object scraping."""
    logger.info("Starting Plan object scraper (pages already exist)")
    
    scraper = PlanScraper()
    result = await scraper.scrape_plans()
    
    print("\n=== Plan Object Scraping Results ===")
    print(f"✅ Pages loaded: {result['pages']}")
    print(f"✅ Objects fetched: {result['objects']}")
    print(f"✅ Total records: {result['total_records']:,}")
    print(f"📁 Objects saved to: raw/objects/Plan/")
    
    if result['objects'] > 0:
        success_rate = (result['objects'] / result['total_records']) * 100
        print(f"📊 Success rate: {success_rate:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
