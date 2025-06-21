"""
Data Joiner

Uses DuckDB to join normalized datasets into a single procurement dataset.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional

import duckdb
import structlog

logger = structlog.get_logger()


class DataJoiner:
    """Joins normalized Parquet files using DuckDB."""

    def __init__(self):
        self.bronze_path = Path("bronze")
        self.dataset_path = Path("dataset")
        self.dataset_path.mkdir(exist_ok=True)

        # Support both Parquet (bronze) and CSV (dataset) sources
        self.csv_path = self.dataset_path

    def create_joined_query(self, use_csv: bool = False) -> str:
        """Create SQL query to join all entities."""
        # Note: Skipping Tender entity as it's not accessible from EOZ API

        if use_csv:
            # Use CSV files from dataset directory
            order_source = f"read_csv_auto('{self.csv_path}/OrderDetail.csv')"
            lot_source = f"read_csv_auto('{self.csv_path}/_Lot.csv')"
            plan_source = f"read_csv_auto('{self.csv_path}/Plan.csv')"
        else:
            # Use Parquet files from bronze directory
            order_source = "parquet_scan('bronze/OrderDetail.parquet')"
            lot_source = "parquet_scan('bronze/_Lot.parquet')"
            plan_source = "parquet_scan('bronze/Plan.parquet')"

        query = f"""
        SELECT 
            -- OrderDetail columns (contract data)
            o.lot_id,
            o.provider_bin,
            o.customer_bin,
            o.contract_sum,
            o.paid_sum,
            o.accept_date,
            o.order_method_id,
            
            -- Lot columns (tender lot data) 
            l.plan_id,
            l.tender_id,
            l.lot_amount,
            l.title_ru,
            l.lot_method_id,
            l.lot_start_date,
            
            -- Plan columns (planning data)
            p.plan_price,
            p.plan_method_id,
            p.plan_customer_bin,
            
            -- Derived fields
            CASE 
                WHEN o.contract_sum IS NOT NULL AND l.lot_amount IS NOT NULL 
                THEN o.contract_sum / l.lot_amount 
                ELSE NULL 
            END AS price_ratio,
            
            CASE 
                WHEN o.paid_sum IS NOT NULL AND o.contract_sum IS NOT NULL 
                THEN o.paid_sum / o.contract_sum 
                ELSE NULL 
            END AS payment_ratio,
            
            -- Risk flags (preliminary)
            CASE WHEN o.order_method_id = 6 OR l.lot_method_id = 6 THEN 1 ELSE 0 END AS single_bidder_flag,
            CASE WHEN o.paid_sum < 0.9 * o.contract_sum THEN 1 ELSE 0 END AS underpaid_flag
            
        FROM {order_source} o
        LEFT JOIN {lot_source} l 
            ON CAST(o.lot_id AS VARCHAR) = CAST(l.lot_id AS VARCHAR)
        LEFT JOIN {plan_source} p 
            ON CAST(l.plan_id AS VARCHAR) = CAST(p.plan_id AS VARCHAR)
        WHERE o.lot_id IS NOT NULL
        """
        return query

    def execute_join(self, use_csv: bool = True) -> Dict[str, Any]:
        """Execute the join query and save results."""
        logger.info("Starting data join operation", use_csv=use_csv)

        # Check that required files exist
        if use_csv:
            required_files = ["OrderDetail.csv", "_Lot.csv", "Plan.csv"]
            check_path = self.csv_path
        else:
            required_files = ["OrderDetail.parquet", "_Lot.parquet", "Plan.parquet"]
            check_path = self.bronze_path

        missing_files = []
        for file in required_files:
            if not (check_path / file).exists():
                missing_files.append(file)

        if missing_files:
            raise FileNotFoundError(f"Missing required files: {missing_files}")

        try:
            # Create DuckDB connection
            conn = duckdb.connect()

            # Execute join query
            query = self.create_joined_query(use_csv=use_csv)
            logger.info("Executing join query")

            result = conn.execute(query)
            df = result.df()

            if df.empty:
                logger.warning("Join query returned empty result")
                return {"records": 0, "file": None}

            # Save joined dataset in both formats
            parquet_file = self.dataset_path / "procurements.parquet"
            csv_file = self.dataset_path / "procurements.csv"

            df.to_parquet(parquet_file, compression="zstd", index=False)
            df.to_csv(csv_file, index=False, encoding="utf-8")

            # Generate summary statistics
            stats = {
                "records": len(df),
                "columns": len(df.columns),
                "parquet_file_size_mb": parquet_file.stat().st_size / 1024 / 1024,
                "csv_file_size_mb": csv_file.stat().st_size / 1024 / 1024,
                "parquet_file_path": str(parquet_file),
                "csv_file_path": str(csv_file),
                "date_range": {
                    "start": (
                        df["lot_start_date"].min()
                        if "lot_start_date" in df.columns
                        else None
                    ),
                    "end": (
                        df["lot_start_date"].max()
                        if "lot_start_date" in df.columns
                        else None
                    ),
                },
            }

            # Data quality checks
            stats["data_quality"] = {
                "null_lot_ids": df["lot_id"].isnull().sum(),
                "null_contract_sums": df["contract_sum"].isnull().sum(),
                "null_provider_bins": df["provider_bin"].isnull().sum(),
                "unique_customers": df["customer_bin"].nunique(),
                "unique_providers": df["provider_bin"].nunique(),
                "avg_contract_value": (
                    df["contract_sum"].mean() if "contract_sum" in df.columns else None
                ),
            }

            logger.info(
                "Data join completed successfully",
                **{
                    k: v
                    for k, v in stats.items()
                    if k not in ["file_path", "data_quality"]
                },
            )
            conn.close()

            return stats

        except Exception as e:
            logger.error("Failed to execute data join", error=str(e))
            raise

    def validate_join_results(self, stats: Dict[str, Any]) -> bool:
        """Validate the join results meet minimum quality standards."""
        logger.info("Validating join results")

        if stats.get("records", 0) == 0:
            logger.error("No records in joined dataset")
            return False

        # Check for reasonable data coverage
        quality = stats.get("data_quality", {})

        if quality.get("null_lot_ids", 0) > stats.get("records", 0) * 0.1:
            logger.warning("High percentage of null lot IDs")

        if quality.get("null_provider_bins", 0) > stats.get("records", 0) * 0.5:
            logger.warning("High percentage of null provider BINs")

        if quality.get("unique_customers", 0) < 10:
            logger.warning("Very few unique customers found")

        logger.info("Join validation completed", quality_checks=quality)
        return True

    async def join_datasets(self) -> Dict[str, Any]:
        """Complete dataset joining workflow."""
        logger.info("Starting dataset joining process")

        try:
            # Execute the join
            stats = self.execute_join()

            # Validate results
            if not self.validate_join_results(stats):
                logger.warning("Join validation failed")

            logger.info("Dataset joining completed", summary=stats)
            return stats

        except Exception as e:
            logger.error("Dataset joining failed", error=str(e))
            raise

    def create_supplementary_views(self) -> None:
        """Create additional DuckDB views for analysis."""
        logger.info("Creating supplementary analysis views")

        try:
            conn = duckdb.connect()

            # Customer analysis view
            customer_view = """
            CREATE OR REPLACE VIEW customer_analysis AS
            SELECT 
                customer_bin,
                COUNT(*) as total_contracts,
                SUM(contract_sum) as total_value,
                AVG(contract_sum) as avg_contract_value,
                COUNT(DISTINCT provider_bin) as unique_providers,
                AVG(payment_ratio) as avg_payment_ratio
            FROM parquet_scan('dataset/procurements.parquet')
            WHERE customer_bin IS NOT NULL
            GROUP BY customer_bin
            """

            # Provider analysis view
            provider_view = """
            CREATE OR REPLACE VIEW provider_analysis AS
            SELECT 
                provider_bin,
                COUNT(*) as total_contracts,
                SUM(contract_sum) as total_value,
                AVG(contract_sum) as avg_contract_value,
                COUNT(DISTINCT customer_bin) as unique_customers,
                SUM(single_bidder_flag) as single_bidder_wins
            FROM parquet_scan('dataset/procurements.parquet')
            WHERE provider_bin IS NOT NULL
            GROUP BY provider_bin
            """

            conn.execute(customer_view)
            conn.execute(provider_view)
            conn.close()

            logger.info("Created supplementary analysis views")

        except Exception as e:
            logger.warning("Failed to create supplementary views", error=str(e))
