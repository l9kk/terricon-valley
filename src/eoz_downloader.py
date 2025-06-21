"""
EOZ API Downloader

Handles async scraping of EOZ procurement data with rate limiting, retries, and progress tracking.
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
class EOZConfig:
    """Configuration for EOZ API client."""

    base_url: str = os.getenv("EOZ_BASE_URL", "https://www.eoz.kz/api/uicommand")
    cookies: str = os.getenv("EOZ_COOKIES", "")
    max_concurrent: int = int(
        os.getenv("MAX_CONCURRENT_REQUESTS", "100")
    )  # More aggressive
    requests_per_second: int = int(
        os.getenv("REQUESTS_PER_SECOND", "10")
    )  # Higher rate
    retry_delays: List[float] = field(
        default_factory=lambda: [
            float(x)
            for x in os.getenv("RETRY_DELAYS", "0.5,1,2").split(",")  # Faster retries
        ]
    )
    timeout: int = 60  # Longer timeout for stability


class EOZDownloader:
    """Async downloader for EOZ procurement data."""

    def __init__(self, config: Optional[EOZConfig] = None):
        self.config = config or EOZConfig()
        self.session: Optional[httpx.AsyncClient] = None
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._consecutive_errors = (
            0  # Track consecutive errors for adaptive rate limiting
        )

        # Entity configurations - Plan removed since already scraped
        self.entities = {
            "_Lot": {"filter": {"tru": None, "includeMyTru": 0}},
            "OrderDetail": {"filter": {}},
        }

        # Ensure directories exist
        for entity in self.entities:
            Path(f"raw/pages/{entity}").mkdir(parents=True, exist_ok=True)
            Path(f"raw/objects/{entity}").mkdir(parents=True, exist_ok=True)

    def build_payload(
        self, entity: str, page: int = 0, length: int = 1000, **kwargs
    ) -> Dict[str, Any]:
        """Build API request payload."""
        payload = {
            "page": page,
            "entity": entity,
            "length": length,
            "filter": self.entities[entity]["filter"].copy(),
        }
        payload["filter"].update(kwargs)
        return payload

    async def make_request(
        self,
        path: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request with retries, rate limiting, and enhanced error handling."""
        async with self.semaphore:
            for attempt, delay in enumerate(self.config.retry_delays):
                try:
                    if self.session is None:
                        self.session = httpx.AsyncClient(
                            timeout=self.config.timeout,
                            limits=httpx.Limits(
                                max_keepalive_connections=20, max_connections=100
                            ),
                        )

                    # Adaptive rate limiting - slow down when hitting errors
                    if self._consecutive_errors > 10:
                        await asyncio.sleep(0.5)  # Slow down significantly
                    elif self._consecutive_errors > 5:
                        await asyncio.sleep(0.2)  # Moderate slowdown
                    else:
                        await asyncio.sleep(0.1)  # Normal aggressive rate

                    response = await self.session.post(
                        f"{self.config.base_url}{path}",
                        json=payload,
                        headers=headers or {},
                    )
                    response.raise_for_status()

                    # Try to parse JSON
                    try:
                        data = response.json()
                        # Reset error counter on success
                        self._consecutive_errors = 0
                        return data
                    except json.JSONDecodeError:
                        # Check if response is empty or not JSON
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

        # This should never be reached due to the raise above, but needed for type checking
        raise RuntimeError("Request failed unexpectedly")

    async def fetch_pages(self, entity: str) -> List[Dict[str, Any]]:
        """Fetch all pages for an entity."""
        logger.info(f"Starting aggressive page fetch for {entity}")

        headers = {}
        if self.config.cookies:  # Use cookies for all entities if available
            headers["Cookie"] = self.config.cookies

        pages_data = []
        page = 0

        while True:
            payload = self.build_payload(entity, page)

            try:
                data = await self.make_request("/get/page", payload, headers)

                if not data.get("content"):
                    logger.info(f"No more content for {entity} at page {page}")
                    break

                # Save raw page data
                page_file = Path(f"raw/pages/{entity}/{page}.json")
                page_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))

                pages_data.append(data)

                logger.info(
                    f"Fetched {entity} page {page}: {len(data['content'])} records"
                )
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch page {page} for {entity}", error=str(e))
                break

        logger.info(
            f"Completed page fetch for {entity}: {page} pages, {sum(len(p['content']) for p in pages_data)} total records"
        )
        return pages_data

    async def fetch_object(
        self, entity: str, uuid: str, semaphore: asyncio.Semaphore
    ) -> Optional[Dict[str, Any]]:
        """Fetch full object data for a specific UUID with enhanced error handling."""
        async with semaphore:
            for attempt in range(3):  # Add retry logic for objects
                try:
                    payload = {"entity": entity, "uuid": uuid}
                    data = await self.make_request("/get/object", payload)

                    # Check if we got valid data
                    if not data or not isinstance(data, dict):
                        if attempt < 2:
                            await asyncio.sleep(
                                0.5 * (attempt + 1)
                            )  # Progressive backoff
                            continue
                        else:
                            logger.debug(
                                f"Empty response for object {uuid} after retries"
                            )
                            return None

                    # Save raw object data
                    obj_file = Path(f"raw/objects/{entity}/{uuid}.json")
                    obj_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))

                    return data

                except json.JSONDecodeError as e:
                    if attempt < 2:
                        logger.debug(
                            f"JSON decode error for {uuid}, retrying (attempt {attempt + 1})"
                        )
                        await asyncio.sleep(
                            1.0 * (attempt + 1)
                        )  # Longer wait for JSON errors
                        continue
                    else:
                        logger.warning(
                            f"Failed to decode JSON for object {uuid} after retries",
                            error=str(e),
                        )
                        return None
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    else:
                        logger.warning(
                            f"Failed to fetch object {uuid} for {entity}", error=str(e)
                        )
                        return None

            return None

    async def fetch_all_objects(
        self, entity: str, pages_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Fetch all object details for an entity with aggressive batching."""
        # Collect all UUIDs from pages with entity-specific field mapping
        uuids = []
        for page_data in pages_data:
            for item in page_data.get("content", []):
                # All entities use "id" field for object fetching API
                uuid = item.get("id")

                if uuid:
                    uuids.append(str(uuid))  # Ensure it's a string

        if not uuids:
            logger.warning(f"No UUIDs found for {entity}")
            return []

        logger.info(f"Aggressively fetching {len(uuids)} objects for {entity}")

        # Use adaptive semaphore for object fetching based on error rate
        if self._consecutive_errors > 20:
            concurrency = min(25, self.config.max_concurrent)  # Reduce concurrency
            logger.info(
                f"Reducing concurrency to {concurrency} due to high error rate ({self._consecutive_errors} consecutive errors)"
            )
        else:
            concurrency = min(75, self.config.max_concurrent)  # Normal aggressive mode

        obj_semaphore = asyncio.Semaphore(concurrency)

        # Process in batches for better memory management
        batch_size = 1000
        all_objects = []

        for i in range(0, len(uuids), batch_size):
            batch_uuids = uuids[i : i + batch_size]
            logger.info(
                f"Processing batch {i//batch_size + 1}/{(len(uuids) + batch_size - 1)//batch_size} ({len(batch_uuids)} objects)"
            )

            # Create tasks for this batch
            tasks = [
                self.fetch_object(entity, uuid, obj_semaphore) for uuid in batch_uuids
            ]

            # Execute batch with progress bar
            batch_objects = []
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Fetching {entity} batch {i//batch_size + 1}",
            ):
                obj_data = await coro
                if obj_data:
                    batch_objects.append(obj_data)

            all_objects.extend(batch_objects)
            logger.info(
                f"Batch {i//batch_size + 1} completed: {len(batch_objects)}/{len(batch_uuids)} objects fetched"
            )

        logger.info(
            f"Successfully fetched {len(all_objects)}/{len(uuids)} objects for {entity}"
        )
        return all_objects

    async def scrape_entity(self, entity: str) -> Dict[str, Any]:
        """Complete scraping workflow for a single entity."""
        logger.info(f"Starting scrape for entity: {entity}")

        # Skip Tender entity as it's not accessible
        if entity == "Tender":
            logger.info("Skipping Tender entity (not accessible)")
            return {"entity": entity, "pages": 0, "objects": 0}

        try:
            # Fetch all pages
            pages_data = await self.fetch_pages(entity)

            # Fetch all objects
            objects_data = await self.fetch_all_objects(entity, pages_data)

            result = {
                "entity": entity,
                "pages": len(pages_data),
                "objects": len(objects_data),
                "total_records": sum(len(p.get("content", [])) for p in pages_data),
            }

            logger.info(f"Completed scrape for {entity}", **result)
            return result

        except Exception as e:
            logger.error(f"Failed to scrape {entity}", error=str(e))
            raise

    async def scrape_all_entities(self) -> Dict[str, Any]:
        """Scrape all entities in parallel for maximum speed."""
        logger.info("Starting aggressive EOZ data scrape")

        # Create tasks for parallel execution
        tasks = []
        for entity in self.entities:
            task = asyncio.create_task(self.scrape_entity(entity))
            tasks.append((entity, task))

        results = {}

        # Execute all tasks in parallel
        for entity, task in tasks:
            try:
                result = await task
                results[entity] = result
                logger.info(f"Completed {entity} scrape", **result)
            except Exception as e:
                logger.error(f"Critical error scraping {entity}", error=str(e))
                results[entity] = {"entity": entity, "error": str(e)}

        # Close session
        if self.session:
            await self.session.aclose()

        total_records = sum(
            r.get("total_records", 0) for r in results.values() if isinstance(r, dict)
        )
        logger.info(
            "Completed all entity scraping",
            total_records=total_records,
            results=results,
        )
        return results


# Reusable utility functions
def build_payload(
    entity: str, page: int = 0, length: int = 1000, *, flt: Optional[Dict] = None
) -> Dict[str, Any]:
    """Utility function to build API payloads."""
    return {"page": page, "entity": entity, "length": length, "filter": flt or {}}


BACKOFF = [1, 3, 10]  # Retry delays in seconds
