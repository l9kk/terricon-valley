"""
EOZ Procurement-Risk Dataset Builder

An AI-powered system for detecting corruption patterns in Kazakhstan's public procurement data.
Scrapes EOZ portal data, processes it through ML pipelines, and provides risk visualization.
"""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import Optional

import structlog
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def run_scraper() -> None:
    """Run the aggressive data scraping pipeline."""
    from .eoz_downloader import EOZDownloader, EOZConfig
    from .normalizer import DataNormalizer
    from .joiner import DataJoiner
    from .features import FeatureEngineer
    from .risk_model import RiskModel

    logger.info("Starting aggressive EOZ data collection pipeline")

    # Show aggressive configuration
    config = EOZConfig()
    logger.info(
        "Aggressive scraping configuration",
        max_concurrent=config.max_concurrent,
        requests_per_second=config.requests_per_second,
        retry_delays=config.retry_delays,
        entities_to_scrape=["_Lot", "OrderDetail"],
    )  # Plan excluded

    # Phase 1: Data scraping
    downloader = EOZDownloader()
    await downloader.scrape_all_entities()

    # Phase 2: Data normalization
    normalizer = DataNormalizer()
    await normalizer.normalize_all_entities()

    # Phase 3: Data joining
    joiner = DataJoiner()
    await joiner.join_datasets()

    # Phase 4: Feature engineering
    feature_eng = FeatureEngineer()
    await feature_eng.engineer_features()

    # Phase 5: Risk model training
    risk_model = RiskModel()
    await risk_model.train_complete_workflow()

    logger.info("Pipeline completed successfully")


async def generate_csv() -> None:
    """Generate CSV files from existing raw JSON data."""
    from .normalizer import DataNormalizer

    logger.info("Starting CSV generation from raw JSON objects")

    normalizer = DataNormalizer()
    results = await normalizer.generate_all_csv_files()

    logger.info("CSV generation completed", results=results)

    # Print summary
    print("\n=== CSV Generation Summary ===")
    for entity, result in results.items():
        if "error" in result:
            print(f"❌ {entity}: Error - {result['error']}")
        else:
            records = result.get("records", 0)
            file_path = result.get("file", "N/A")
            print(f"✅ {entity}: {records:,} records -> {file_path}")


def run_dashboard() -> None:
    """Launch the Streamlit dashboard."""
    import subprocess
    import sys

    logger.info("Launching Streamlit dashboard")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(Path(__file__).parent / "dashboard.py"),
        ]
    )


async def join_datasets() -> None:
    """Join the separate CSV files into one unified dataset."""
    from .joiner import DataJoiner

    logger.info("Starting dataset joining process")

    joiner = DataJoiner()
    try:
        stats = joiner.execute_join(use_csv=True)

        if joiner.validate_join_results(stats):
            logger.info("Dataset joining completed successfully", stats=stats)

            # Print summary
            print("\n=== Dataset Joining Summary ===")
            print(f"✅ Records: {stats.get('records', 0):,}")
            print(f"✅ Columns: {stats.get('columns', 0)}")
            print(
                f"✅ Parquet: {stats.get('parquet_file_path', 'N/A')} ({stats.get('parquet_file_size_mb', 0):.1f} MB)"
            )
            print(
                f"✅ CSV: {stats.get('csv_file_path', 'N/A')} ({stats.get('csv_file_size_mb', 0):.1f} MB)"
            )

            quality = stats.get("data_quality", {})
            print(f"📊 Unique customers: {quality.get('unique_customers', 0):,}")
            print(f"📊 Unique providers: {quality.get('unique_providers', 0):,}")
            print(
                f"📊 Avg contract value: {quality.get('avg_contract_value', 0):,.2f} ₸"
            )
        else:
            print("❌ Dataset joining validation failed")

    except Exception as e:
        logger.error("Dataset joining failed", error=str(e))
        print(f"❌ Error: {e}")


async def run_plan_scraper() -> None:
    """Run the fast Plan object scraper (using existing pages)."""
    from .plan_scraper import PlanScraper

    logger.info("Starting Plan object scraping from existing pages")

    scraper = PlanScraper()
    result = await scraper.scrape_plans()

    logger.info("Plan object scraping completed successfully", **result)

    # Print summary
    print("\n=== Plan Object Scraping Summary ===")
    print(f"✅ Pages loaded: {result['pages']}")
    print(f"✅ Objects fetched: {result['objects']}")
    print(f"✅ Total records: {result['total_records']:,}")
    print(f"📁 Objects saved to: raw/objects/Plan/")
    
    if result['objects'] > 0:
        success_rate = (result['objects'] / result['total_records']) * 100
        print(f"📊 Success rate: {success_rate:.1f}%")


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="EOZ Procurement-Risk Dataset Builder")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Scraper command
    scraper_parser = subparsers.add_parser(
        "scrape", help="Run data collection pipeline"
    )

    # CSV generation command
    csv_parser = subparsers.add_parser(
        "csv", help="Generate CSV files from existing raw JSON data"
    )

    # Join command
    join_parser = subparsers.add_parser(
        "join", help="Join separate CSV files into unified procurement dataset"
    )

    # Dashboard command
    dashboard_parser = subparsers.add_parser(
        "dashboard", help="Launch Streamlit dashboard"
    )

    # Plan scraper command
    plan_parser = subparsers.add_parser(
        "scrape-plans", help="Scrape Plan objects from existing pages (fast)"
    )

    # Parse arguments
    args = parser.parse_args()

    if args.command == "scrape":
        asyncio.run(run_scraper())
    elif args.command == "csv":
        asyncio.run(generate_csv())
    elif args.command == "join":
        asyncio.run(join_datasets())
    elif args.command == "dashboard":
        run_dashboard()
    elif args.command == "scrape-plans":
        asyncio.run(run_plan_scraper())
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
