"""
Data Normalizer

Processes raw JSON data into normalized Parquet files with proper column selection.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import structlog

logger = structlog.get_logger()


class DataNormalizer:
    """Normalizes raw EOZ JSON data to structured Parquet format."""

    def __init__(self):
        self.chunk_size = int(os.getenv("CHUNK_SIZE", "50000"))
        self.compression = os.getenv("PARQUET_COMPRESSION", "zstd")

        # Column mappings for each entity
        self.column_mappings = {
            "Plan": {
                "externalId": "plan_id",
                "sum": "plan_price",
                "methodTrade.id": "plan_method_id",
                "customerBin.biniin": "plan_customer_bin",
                "nameRu": "plan_name_ru",
                "descriptionRu": "plan_description_ru",
            },
            "_Lot": {
                "externalId": "lot_id",
                "externalPlanId": "plan_id",
                "externalTenderId": "tender_id",
                "customerBin.biniin": "customer_bin",
                "customerBin.nameru": "customer_name_ru",
                "organizerbin.nameru": "organizer_name_ru",
                "amount": "lot_amount",
                "titleRu": "title_ru",
                "descriptionRu": "description_ru",
                "system.nameRu": "platform",
                "methodTrade.id": "lot_method_id",
                "methodTrade.nameRu": "method_trade_name_ru",
                "startDate": "lot_start_date",
            },
            "OrderDetail": {
                "externalId": "lot_id",  # OrderDetail links to lot via externalId
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
            },
        }

    def load_raw_data(self, entity: str) -> List[Dict[str, Any]]:
        """Load all raw JSON data for an entity."""
        logger.info(f"Loading raw data for {entity}")

        all_data = []

        # Load object data directly (we have complete objects)
        objects_dir = Path(f"raw/objects/{entity}")
        if objects_dir.exists():
            for obj_file in objects_dir.glob("*.json"):
                try:
                    with obj_file.open("r", encoding="utf-8") as f:
                        obj = json.load(f)
                        all_data.append(obj)
                except Exception as e:
                    logger.warning(
                        f"Failed to load object file {obj_file}", error=str(e)
                    )

        logger.info(f"Loaded {len(all_data)} records for {entity}")
        return all_data

    def normalize_dataframe(
        self, data: List[Dict[str, Any]], entity: str
    ) -> pd.DataFrame:
        """Normalize JSON data to flat DataFrame."""
        if not data:
            logger.warning(f"No data to normalize for {entity}")
            return pd.DataFrame()

        # Flatten nested JSON with dot notation
        df = pd.json_normalize(data, sep=".")

        logger.info(f"Normalized {entity}: {len(df)} rows, {len(df.columns)} columns")

        # Select and rename columns based on mapping
        column_mapping = self.column_mappings.get(entity, {})

        # Keep only mapped columns that exist in the data
        existing_cols = {}
        for json_path, new_name in column_mapping.items():
            if json_path in df.columns:
                existing_cols[json_path] = new_name
            else:
                logger.warning(f"Column {json_path} not found in {entity} data")

        if existing_cols:
            df = df[list(existing_cols.keys())].rename(columns=existing_cols)
        else:
            logger.error(f"No mapped columns found for {entity}")
            return pd.DataFrame()

        # Data type conversions
        df = self.convert_data_types(df, entity)

        logger.info(f"Final {entity} DataFrame: {len(df)} rows, {list(df.columns)}")
        return df

    def convert_data_types(self, df: pd.DataFrame, entity: str) -> pd.DataFrame:
        """Convert data types appropriately."""
        try:
            # Date columns
            date_columns = [col for col in df.columns if "date" in col.lower()]
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Numeric columns
            numeric_columns = [
                col
                for col in df.columns
                if any(x in col.lower() for x in ["price", "amount", "_sum", "_id"])
                and not any(
                    exclude in col.lower()
                    for exclude in ["name", "title", "description", "platform"]
                )
            ]
            for col in numeric_columns:
                if col in df.columns and not col.endswith("_id"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif col.endswith("_id"):
                    # IDs should be strings to preserve leading zeros
                    df[col] = df[col].astype(str)

            # String columns - let pandas handle string columns naturally
            # Don't force conversion to string as it can corrupt data
            string_columns = [
                col
                for col in df.columns
                if any(
                    x in col.lower()
                    for x in ["bin", "title", "name", "description", "platform"]
                )
            ]
            # Only ensure BIN columns are strings (they should be treated as categorical data)
            for col in string_columns:
                if col in df.columns and "bin" in col.lower():
                    df[col] = df[col].astype(str)

        except Exception as e:
            logger.warning(f"Error converting data types for {entity}", error=str(e))

        return df

    def save_parquet(self, df: pd.DataFrame, entity: str) -> Path:
        """Save DataFrame to Parquet format."""
        output_file = Path(f"bronze/{entity}.parquet")

        try:
            # Save with compression
            df.to_parquet(
                output_file, compression=self.compression, index=False, engine="pyarrow"
            )

            logger.info(
                f"Saved {entity} to {output_file}: {len(df)} rows, {output_file.stat().st_size / 1024 / 1024:.1f} MB"
            )
            return output_file

        except Exception as e:
            logger.error(f"Failed to save {entity} Parquet", error=str(e))
            raise

    def save_csv(self, df: pd.DataFrame, entity: str) -> Path:
        """Save DataFrame to CSV file."""
        output_dir = Path("dataset")
        output_dir.mkdir(exist_ok=True)

        output_file = output_dir / f"{entity}.csv"

        # Save with proper encoding for Russian text
        df.to_csv(output_file, index=False, encoding="utf-8")

        logger.info(
            f"Saved CSV for {entity}",
            file=str(output_file),
            records=len(df),
            columns=len(df.columns),
        )

        return output_file

    async def normalize_entity(self, entity: str) -> Dict[str, Any]:
        """Complete normalization workflow for a single entity."""
        logger.info(f"Starting normalization for {entity}")

        try:
            # Load raw data
            raw_data = self.load_raw_data(entity)

            if not raw_data:
                logger.warning(f"No raw data found for {entity}")
                return {"entity": entity, "records": 0, "file": None}

            # Normalize to DataFrame
            df = self.normalize_dataframe(raw_data, entity)

            if df.empty:
                logger.warning(f"Empty DataFrame after normalization for {entity}")
                return {"entity": entity, "records": 0, "file": None}

            # Save to Parquet
            output_file = self.save_parquet(df, entity)

            result = {
                "entity": entity,
                "records": len(df),
                "columns": list(df.columns),
                "file": str(output_file),
            }

            logger.info(
                f"Completed normalization for {entity}",
                **{k: v for k, v in result.items() if k != "columns"},
            )
            return result

        except Exception as e:
            logger.error(f"Failed to normalize {entity}", error=str(e))
            raise

    async def normalize_all_entities(self) -> Dict[str, Any]:
        """Normalize all entities."""
        logger.info("Starting data normalization for all entities")

        entities = ["Plan", "_Lot", "OrderDetail"]
        results = {}

        for entity in entities:
            try:
                result = await self.normalize_entity(entity)
                results[entity] = result
            except Exception as e:
                logger.error(f"Failed to normalize {entity}", error=str(e))
                results[entity] = {"entity": entity, "error": str(e)}

        total_records = sum(r.get("records", 0) for r in results.values())
        logger.info(
            f"Completed normalization: {total_records} total records", results=results
        )

        return results

    async def generate_csv_for_entity(self, entity: str) -> Dict[str, Any]:
        """Generate CSV file for a single entity."""
        try:
            logger.info(f"Generating CSV for {entity}")

            # Load raw data
            raw_data = self.load_raw_data(entity)

            if not raw_data:
                logger.warning(f"No raw data found for {entity}")
                return {"entity": entity, "records": 0, "file": None}

            # Normalize to DataFrame
            df = self.normalize_dataframe(raw_data, entity)

            if df.empty:
                logger.warning(f"Empty DataFrame after normalization for {entity}")
                return {"entity": entity, "records": 0, "file": None}

            # Save to CSV
            output_file = self.save_csv(df, entity)

            result = {
                "entity": entity,
                "records": len(df),
                "columns": list(df.columns),
                "file": str(output_file),
            }

            logger.info(
                f"Completed CSV generation for {entity}",
                **{k: v for k, v in result.items() if k != "columns"},
            )
            return result

        except Exception as e:
            logger.error(f"Failed to generate CSV for {entity}", error=str(e))
            raise

    async def generate_all_csv_files(self) -> Dict[str, Any]:
        """Generate CSV files for all entities."""
        logger.info("Starting CSV generation for all entities")

        entities = ["Plan", "_Lot", "OrderDetail"]
        results = {}

        for entity in entities:
            try:
                result = await self.generate_csv_for_entity(entity)
                results[entity] = result
            except Exception as e:
                logger.error(f"Failed to generate CSV for {entity}", error=str(e))
                results[entity] = {"entity": entity, "error": str(e)}

        total_records = sum(r.get("records", 0) for r in results.values())
        logger.info(
            f"Completed CSV generation: {total_records} total records", results=results
        )

        return results
