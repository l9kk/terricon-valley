"""
Powerhouse OrderDetail Processor

Ultra-high performance EOZ OrderDetail scraper optimized for 128GB RAM + RTX 5090.
Focuses exclusively on OrderDetail data with extreme concurrency and memory optimization.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
import gc
import psutil

import httpx
import structlog
from tqdm.asyncio import tqdm
import pandas as pd
import orjson  # Ultra-fast JSON parsing

logger = structlog.get_logger()


@dataclass
class PowerhouseConfig:
    """Configuration optimized for high-end hardware."""
    
    base_url: str = os.getenv("EOZ_BASE_URL", "https://www.eoz.kz/api/uicommand")
    cookies: str = os.getenv("EOZ_COOKIES", "")
    
    # Extreme concurrency settings for 128GB RAM
    max_concurrent_pages: int = int(os.getenv("MAX_CONCURRENT_PAGES", "50"))
    max_concurrent_objects: int = int(os.getenv("MAX_CONCURRENT_OBJECTS", "500"))
    requests_per_second: int = int(os.getenv("REQUESTS_PER_SECOND", "1000"))
    
    # Memory optimization
    batch_size: int = int(os.getenv("BATCH_SIZE", "100000"))  # 100K records per batch
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "50000"))   # 50K for processing
    
    # Retry settings
    retry_delays: List[float] = field(default_factory=lambda: [0.1, 0.5, 1.0])
    timeout: int = 120
    
    # Performance monitoring
    memory_threshold_gb: float = 100.0  # Start cleanup at 100GB usage


class PowerhouseOrderDetailProcessor:
    """Ultra-high performance OrderDetail processor."""
    
    def __init__(self, config: Optional[PowerhouseConfig] = None):
        self.config = config or PowerhouseConfig()
        self.session: Optional[httpx.AsyncClient] = None
        
        # Semaphores for rate limiting
        self.page_semaphore = asyncio.Semaphore(self.config.max_concurrent_pages)
        self.object_semaphore = asyncio.Semaphore(self.config.max_concurrent_objects)
        
        # Performance tracking
        self.stats = {
            "pages_fetched": 0,
            "objects_fetched": 0,
            "objects_saved": 0,
            "errors": 0,
            "start_time": time.time(),
        }
        
        # Memory management
        self.processed_uuids: Set[str] = set()
        self.memory_cleanup_counter = 0
        
        # Ensure directories exist
        Path("raw/pages/OrderDetail").mkdir(parents=True, exist_ok=True)
        Path("raw/objects/OrderDetail").mkdir(parents=True, exist_ok=True)
        Path("dataset").mkdir(parents=True, exist_ok=True)
        
        logger.info("Powerhouse OrderDetail Processor initialized", 
                   max_concurrent_objects=self.config.max_concurrent_objects,
                   batch_size=self.config.batch_size,
                   memory_threshold=f"{self.config.memory_threshold_gb}GB")
    
    def get_memory_usage_gb(self) -> float:
        """Get current memory usage in GB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024 / 1024
    
    def cleanup_memory(self):
        """Aggressive memory cleanup for long-running processes."""
        self.memory_cleanup_counter += 1
        if self.memory_cleanup_counter % 100 == 0:
            gc.collect()
            current_memory = self.get_memory_usage_gb()
            if current_memory > self.config.memory_threshold_gb:
                logger.warning(f"High memory usage: {current_memory:.1f}GB, forcing cleanup")
                # Clear processed UUIDs if memory is high
                if len(self.processed_uuids) > 1000000:
                    self.processed_uuids.clear()
                gc.collect()
    
    async def get_session(self) -> httpx.AsyncClient:
        """Get or create HTTP session with optimal settings."""
        if self.session is None:
            # Optimized for high concurrency
            limits = httpx.Limits(
                max_keepalive_connections=1000,
                max_connections=2000,
                keepalive_expiry=300
            )
            
            self.session = httpx.AsyncClient(
                timeout=self.config.timeout,
                limits=limits,
                http2=True,  # Enable HTTP/2 for better performance
            )
            
        return self.session
    
    async def make_request(self, path: str, payload: Dict[str, Any], 
                          headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        """Make HTTP request with ultra-fast JSON parsing."""
        for attempt, delay in enumerate(self.config.retry_delays):
            try:
                session = await self.get_session()
                
                # Rate limiting
                await asyncio.sleep(1.0 / self.config.requests_per_second)
                
                response = await session.post(
                    f"{self.config.base_url}{path}",
                    json=payload,
                    headers=headers or {}
                )
                response.raise_for_status()
                
                # Ultra-fast JSON parsing with orjson
                if response.content:
                    try:
                        data = orjson.loads(response.content)
                        return data
                    except orjson.JSONDecodeError:
                        logger.debug(f"Invalid JSON response from {path}")
                        return None
                else:
                    return None
                    
            except (httpx.HTTPStatusError, httpx.RequestError, Exception) as e:
                self.stats["errors"] += 1
                if attempt < len(self.config.retry_delays) - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.debug(f"Request failed after retries: {str(e)}")
                    return None
        
        return None
    
    async def fetch_orderdetail_pages(self) -> List[Dict[str, Any]]:
        """Fetch all OrderDetail pages with maximum concurrency."""
        logger.info("Starting ultra-high speed OrderDetail page fetching")
        
        headers = {}
        if self.config.cookies:
            headers["Cookie"] = self.config.cookies
        
        pages_data = []
        page = 0
        
        # Use semaphore for page fetching
        async with self.page_semaphore:
            while True:
                payload = {
                    "page": page,
                    "entity": "OrderDetail",
                    "length": 1000,  # Maximum per page
                    "filter": {}
                }
                
                data = await self.make_request("/get/page", payload, headers)
                
                if not data or not data.get("content"):
                    logger.info(f"No more OrderDetail pages at page {page}")
                    break
                
                # Save raw page data with orjson for speed
                page_file = Path(f"raw/pages/OrderDetail/{page}.json")
                with page_file.open("wb") as f:
                    f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
                
                pages_data.append(data)
                self.stats["pages_fetched"] += 1
                
                logger.info(f"Fetched OrderDetail page {page}: {len(data['content'])} records")
                page += 1
                
                # Memory cleanup check
                self.cleanup_memory()
        
        total_records = sum(len(p.get("content", [])) for p in pages_data)
        logger.info(f"Completed OrderDetail page fetch: {len(pages_data)} pages, {total_records} total records")
        
        return pages_data
    
    async def fetch_orderdetail_object(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch single OrderDetail object with deduplication."""
        # Skip already processed UUIDs
        if uuid in self.processed_uuids:
            return None
        
        async with self.object_semaphore:
            payload = {"entity": "OrderDetail", "uuid": uuid}
            data = await self.make_request("/get/object", payload)
            
            if data and isinstance(data, dict):
                # Save raw object data with orjson
                obj_file = Path(f"raw/objects/OrderDetail/{uuid}.json")
                with obj_file.open("wb") as f:
                    f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
                
                self.processed_uuids.add(uuid)
                self.stats["objects_fetched"] += 1
                self.stats["objects_saved"] += 1
                
                # Memory cleanup
                self.cleanup_memory()
                
                return data
            
            return None
    
    async def fetch_all_orderdetail_objects(self, pages_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch all OrderDetail objects with extreme parallelism."""
        # Extract all UUIDs
        uuids = []
        for page_data in pages_data:
            for item in page_data.get("content", []):
                uuid = item.get("id")
                if uuid:
                    uuids.append(str(uuid))
        
        if not uuids:
            logger.warning("No UUIDs found for OrderDetail objects")
            return []
        
        logger.info(f"Starting ultra-parallel fetch of {len(uuids)} OrderDetail objects")
        
        # Process in large batches to maximize memory usage
        all_objects = []
        
        for i in range(0, len(uuids), self.config.batch_size):
            batch_uuids = uuids[i:i + self.config.batch_size]
            batch_num = i // self.config.batch_size + 1
            total_batches = (len(uuids) + self.config.batch_size - 1) // self.config.batch_size
            
            logger.info(f"Processing ultra-batch {batch_num}/{total_batches} ({len(batch_uuids)} objects)")
            
            # Create all tasks for this batch
            tasks = [self.fetch_orderdetail_object(uuid) for uuid in batch_uuids]
            
            # Execute with progress tracking
            batch_objects = []
            async for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Ultra-batch {batch_num}/{total_batches}",
                ncols=100
            ):
                obj_data = await coro
                if obj_data:
                    batch_objects.append(obj_data)
            
            all_objects.extend(batch_objects)
            
            # Batch statistics
            success_rate = len(batch_objects) / len(batch_uuids) * 100
            logger.info(f"Ultra-batch {batch_num} completed: {len(batch_objects)}/{len(batch_uuids)} objects ({success_rate:.1f}% success)")
            
            # Force memory cleanup between batches
            if batch_num % 10 == 0:
                gc.collect()
        
        logger.info(f"Ultra-parallel fetch completed: {len(all_objects)}/{len(uuids)} objects fetched")
        return all_objects
    
    def process_to_csv(self, objects_data: List[Dict[str, Any]]) -> Optional[Path]:
        """Process OrderDetail objects to CSV with optimized column mapping."""
        logger.info(f"Processing {len(objects_data)} OrderDetail objects to CSV")
        
        if not objects_data:
            logger.warning("No objects to process")
            return None
        
        # Flatten JSON data efficiently
        df = pd.json_normalize(objects_data, sep='.')
        
        # OrderDetail column mapping (optimized for analysis)
        column_mapping: Dict[str, str] = {
            "externalId": "lot_id",
            "providerbin": "provider_bin", 
            "customerbin": "customer_bin",
            "customer.nameru": "customer_name_ru",
            "provider.nameru": "provider_name_ru",
            "sum": "contract_sum",
            "paidSum": "paid_sum",
            "acceptdate": "accept_date",
            "descriptionRu": "description_ru",
            "system.nameRu": "platform",
            "methodTrade.id": "order_method_id",
            "methodTrade.nameRu": "method_trade_name_ru",
            "finishdate": "finish_date",
            "startdate": "start_date",
            "statusTrade.id": "status_id",
            "statusTrade.nameRu": "status_name_ru",
        }
        
        # Select and rename columns
        existing_cols: Dict[str, str] = {}
        for json_path, new_name in column_mapping.items():
            if json_path in df.columns:
                existing_cols[json_path] = new_name
            else:
                logger.debug(f"Column {json_path} not found in OrderDetail data")
        
        if existing_cols:
            df = df[list(existing_cols.keys())]
            df.columns = [existing_cols.get(col, col) for col in df.columns]
        else:
            logger.error("No mapped columns found for OrderDetail")
            return None
        
        # Data type conversions
        try:
            # Date columns
            date_columns = [col for col in df.columns if isinstance(col, str) and 'date' in col.lower()]
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Numeric columns
            numeric_columns = ['contract_sum', 'paid_sum']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # String columns
            string_columns = ['provider_bin', 'customer_bin', 'customer_name_ru', 
                            'provider_name_ru', 'description_ru', 'platform', 'method_trade_name_ru']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
                    
        except Exception as e:
            logger.warning(f"Error converting data types: {str(e)}")
        
        # Save to CSV
        output_file = Path("dataset/OrderDetail_powerhouse.csv")
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        file_size_mb = output_file.stat().st_size / 1024 / 1024
        logger.info(f"Saved OrderDetail CSV: {len(df)} rows, {len(df.columns)} columns, {file_size_mb:.1f} MB")
        
        return output_file
    
    async def run_powerhouse_processing(self) -> Dict[str, Any]:
        """Execute the complete ultra-high performance OrderDetail processing."""
        logger.info("🚀 Starting POWERHOUSE OrderDetail processing")
        
        try:
            # Phase 1: Fetch all pages
            pages_data = await self.fetch_orderdetail_pages()
            
            # Phase 2: Fetch all objects with extreme parallelism
            objects_data = await self.fetch_all_orderdetail_objects(pages_data)
            
            # Phase 3: Process to CSV
            csv_file = self.process_to_csv(objects_data)
            
            # Calculate performance stats
            elapsed_time = time.time() - self.stats["start_time"]
            objects_per_second = self.stats["objects_fetched"] / elapsed_time if elapsed_time > 0 else 0
            
            result = {
                "pages_fetched": self.stats["pages_fetched"],
                "objects_fetched": self.stats["objects_fetched"],
                "objects_saved": self.stats["objects_saved"],
                "errors": self.stats["errors"],
                "elapsed_time_seconds": elapsed_time,
                "objects_per_second": objects_per_second,
                "csv_file": str(csv_file) if csv_file else None,
                "memory_peak_gb": self.get_memory_usage_gb(),
            }
            
            logger.info("🎯 POWERHOUSE processing completed successfully", **result)
            return result
            
        except Exception as e:
            logger.error(f"❌ POWERHOUSE processing failed: {str(e)}")
            raise
        
        finally:
            # Cleanup
            if self.session:
                await self.session.aclose()
            gc.collect()
    
    def print_performance_summary(self, result: Dict[str, Any]):
        """Print detailed performance summary."""
        print("\n" + "="*80)
        print("🚀 POWERHOUSE ORDERDETAIL PROCESSOR - PERFORMANCE SUMMARY")
        print("="*80)
        print(f"📊 Pages Fetched:        {result['pages_fetched']:,}")
        print(f"📦 Objects Fetched:      {result['objects_fetched']:,}")
        print(f"💾 Objects Saved:        {result['objects_saved']:,}")
        print(f"⚠️  Errors:              {result['errors']:,}")
        print(f"⏱️  Total Time:          {result['elapsed_time_seconds']:.1f} seconds")
        print(f"🚄 Objects/Second:       {result['objects_per_second']:.1f}")
        print(f"🧠 Peak Memory Usage:    {result['memory_peak_gb']:.1f} GB")
        if result['csv_file']:
            print(f"📄 CSV Output:           {result['csv_file']}")
        print("="*80)


async def main():
    """Main entry point for powerhouse processing."""
    processor = PowerhouseOrderDetailProcessor()
    
    try:
        result = await processor.run_powerhouse_processing()
        processor.print_performance_summary(result)
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Configure logging for high-performance mode
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(30),  # WARNING level for performance
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Run the powerhouse processor
    asyncio.run(main())