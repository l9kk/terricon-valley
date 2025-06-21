"""
Ultra-Fast Object Scraper - Skip completed entities, max performance
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

import httpx
import structlog
from tqdm.asyncio import tqdm

logger = structlog.get_logger()


@dataclass
class FastScraperConfig:
    """Ultra-aggressive scraping configuration."""

    base_url: str = os.getenv("EOZ_BASE_URL", "https://www.eoz.kz/api/uicommand")
    cookies: str = os.getenv("EOZ_COOKIES", "")
    max_concurrent: int = int(
        os.getenv("MAX_CONCURRENT_REQUESTS", "150")
    )  # Higher network concurrency
    io_concurrent: int = int(
        os.getenv("IO_CONCURRENT", "50")
    )  # Separate I/O concurrency
    requests_per_second: int = int(
        os.getenv("REQUESTS_PER_SECOND", "25")
    )  # Faster requests
    retry_delays: List[float] = field(
        default_factory=lambda: [0.1, 0.3, 0.8]  # Faster retries
    )
    timeout: int = 25
    min_id: int = 38900  # Start from this ID to avoid duplicates


class FastObjectScraper:
    """Ultra-fast scraper with true parallel processing."""

    def __init__(self, config: Optional[FastScraperConfig] = None):
        self.config = config or FastScraperConfig()
        self.session: Optional[httpx.AsyncClient] = None
        self.network_semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self.io_semaphore = asyncio.Semaphore(self.config.io_concurrent)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config.io_concurrent)

        # Entity mappings
        self.entity_mappings = {
            "OrderDetail": "ContractTitle",
            "_Lot": "Lot",
            "Plan": "Plan",
        }

        # Focus on these entities (skip completed ones)
        self.target_entities = ["OrderDetail", "Plan"]

    async def __aenter__(self):
        """Async context manager entry."""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        if self.config.cookies:
            headers["Cookie"] = self.config.cookies

        # Aggressive HTTP client settings
        limits = httpx.Limits(
            max_keepalive_connections=50, max_connections=200, keepalive_expiry=30
        )

        timeout = httpx.Timeout(
            connect=10.0, read=self.config.timeout, write=10.0, pool=5.0
        )

        self.session = httpx.AsyncClient(
            headers=headers,
            limits=limits,
            timeout=timeout,
            http2=True,  # Use HTTP/2 for better performance
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.aclose()
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)

    def get_entity_status(self) -> Dict[str, Dict]:
        """Get detailed status of each entity."""
        status = {}

        for entity in self.target_entities:
            pages_dir = Path(f"raw/pages/{entity}")
            objects_dir = Path(f"raw/objects/{entity}")

            # Count total IDs from pages
            total_ids = 0
            if pages_dir.exists():
                for json_file in pages_dir.glob("*.json"):
                    try:
                        with open(json_file, "r", encoding="utf-8") as f:
                            page_data = json.load(f)
                        content = page_data.get("content", [])
                        total_ids += len(content)
                    except:
                        continue

            # Count existing objects
            existing_objects = 0
            if objects_dir.exists():
                existing_objects = len(list(objects_dir.glob("*.json")))

            status[entity] = {
                "total_ids": total_ids,
                "existing_objects": existing_objects,
                "remaining": total_ids - existing_objects,
                "completion_percent": (
                    (existing_objects / total_ids * 100) if total_ids > 0 else 0
                ),
            }

        return status

    def extract_missing_ids(self, entity: str) -> List[str]:
        """Extract IDs that haven't been downloaded yet, starting from min_id."""
        pages_dir = Path(f"raw/pages/{entity}")
        if not pages_dir.exists():
            return []

        # Get existing object IDs
        objects_dir = Path(f"raw/objects/{entity}")
        existing_ids = set()
        if objects_dir.exists():
            for obj_file in objects_dir.glob("*.json"):
                existing_ids.add(obj_file.stem)

        # Extract all IDs from pages, filter by min_id
        all_ids = []
        for json_file in sorted(
            pages_dir.glob("*.json"),
            key=lambda x: int(x.stem) if x.stem.isdigit() else 0,
        ):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    page_data = json.load(f)

                content = page_data.get("content", [])
                for item in content:
                    item_id = item.get("id")
                    if item_id:
                        item_id_str = str(item_id)
                        # Filter by min_id and existing IDs
                        if (
                            item_id_str not in existing_ids
                            and int(item_id_str) >= self.config.min_id
                        ):
                            all_ids.append(item_id_str)

            except Exception as e:
                logger.warning(f"Failed to process {json_file}", error=str(e))
                continue

        return all_ids

    async def fetch_object(
        self, entity: str, object_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch single object with network semaphore control."""
        mapped_entity = self.entity_mappings.get(entity, entity)
        payload = {"entity": mapped_entity, "uuid": object_id}

        async with self.network_semaphore:
            for attempt, delay in enumerate(self.config.retry_delays):
                try:
                    response = await self.session.post(
                        f"{self.config.base_url}/get/object", json=payload
                    )

                    if response.status_code == 200:
                        data = response.json()
                        if data and isinstance(data, dict):
                            return data

                    # Fast fail on certain errors
                    if response.status_code in [404, 403]:
                        logger.debug(f"Object {object_id} not found or forbidden")
                        return None

                except Exception as e:
                    if attempt < len(self.config.retry_delays) - 1:
                        await asyncio.sleep(delay)
                        continue
                    logger.debug(
                        f"Failed to fetch {entity} object {object_id}", error=str(e)
                    )

        return None

    async def save_object(
        self, entity: str, object_id: str, data: Dict[str, Any]
    ) -> bool:
        """Save object to file using thread pool."""

        def _save_sync():
            try:
                objects_dir = Path(f"raw/objects/{entity}")
                objects_dir.mkdir(parents=True, exist_ok=True)

                file_path = objects_dir / f"{object_id}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                return True
            except Exception as e:
                logger.error(
                    f"Failed to save {entity} object {object_id}", error=str(e)
                )
                return False

        async with self.io_semaphore:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.thread_pool, _save_sync)

    async def download_and_save_object(self, entity: str, object_id: str) -> str:
        """Download and save a single object. Returns status."""
        try:
            data = await self.fetch_object(entity, object_id)
            if data:
                success = await self.save_object(entity, object_id, data)
                return "success" if success else "save_failed"
            else:
                return "not_found"
        except Exception as e:
            logger.debug(f"Error processing {entity} {object_id}", error=str(e))
            return "error"

    async def scrape_entity(self, entity: str) -> Dict[str, int]:
        """Scrape all missing objects for one entity using true parallel processing."""
        logger.info(f"Starting to scrape {entity}...")

        missing_ids = self.extract_missing_ids(entity)
        if not missing_ids:
            logger.info(f"No missing objects for {entity}")
            return {"success": 0, "save_failed": 0, "not_found": 0, "error": 0}

        logger.info(
            f"Found {len(missing_ids)} missing objects for {entity} (starting from ID {self.config.min_id})"
        )

        # Create all tasks at once for true parallel processing
        tasks = [
            self.download_and_save_object(entity, object_id)
            for object_id in missing_ids
        ]

        # Execute all tasks in parallel with progress tracking
        results = []
        progress_desc = f"Scraping {entity} ({len(tasks)} objects)"

        # Use asyncio.gather for maximum parallelism
        async for result in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc=progress_desc,
            leave=True,
        ):
            status = await result
            results.append(status)

        # Count results
        stats = {"success": 0, "save_failed": 0, "not_found": 0, "error": 0}
        for status in results:
            stats[status] = stats.get(status, 0) + 1

        return stats

    async def run(self):
        """Run the ultra-fast scraper with parallel processing."""
        logger.info("=== Ultra-Fast Object Scraper Started ===")
        logger.info(f"Starting from ID: {self.config.min_id}")
        logger.info(f"Network concurrency: {self.config.max_concurrent}")
        logger.info(f"I/O concurrency: {self.config.io_concurrent}")

        # Show initial status
        status = self.get_entity_status()
        for entity, info in status.items():
            # Count remaining items >= min_id
            remaining_filtered = sum(
                1 for missing_id in self.extract_missing_ids(entity)
            )
            logger.info(
                f"{entity}: {info['existing_objects']}/{info['total_ids']} "
                f"({info['completion_percent']:.1f}% complete, "
                f"{remaining_filtered} remaining >= ID {self.config.min_id})"
            )

        # Create tasks for entities that have remaining work
        entity_tasks = []
        for entity in self.target_entities:
            missing_ids = self.extract_missing_ids(entity)
            if missing_ids:
                logger.info(
                    f"Queuing {entity} for parallel processing ({len(missing_ids)} objects)"
                )
                entity_tasks.append((entity, self.scrape_entity(entity)))

        if not entity_tasks:
            logger.info("All entities are complete!")
            return

        # Process all entities in parallel for maximum speed
        logger.info(f"Processing {len(entity_tasks)} entities in parallel...")

        results = []
        for entity, task in entity_tasks:
            try:
                result = await task
                results.append((entity, result))
                logger.info(f"Completed {entity}: {result}")
            except Exception as e:
                logger.error(f"Failed to scrape {entity}", error=str(e))
                results.append(
                    (
                        entity,
                        {"success": 0, "save_failed": 0, "not_found": 0, "error": 0},
                    )
                )

        # Final summary
        logger.info("=== Ultra-Fast Scraping Complete ===")
        total_success = 0
        for entity, stats in results:
            success = stats.get("success", 0)
            total_success += success
            logger.info(
                f"{entity}: Success: {stats.get('success', 0)}, "
                f"Save Failed: {stats.get('save_failed', 0)}, "
                f"Not Found: {stats.get('not_found', 0)}, "
                f"Errors: {stats.get('error', 0)}"
            )

        logger.info(f"Total objects successfully downloaded: {total_success}")


async def main():
    """Main entry point."""
    async with FastObjectScraper() as scraper:
        await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
