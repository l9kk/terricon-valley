"""
Feature Engineering

Implements corruption detection features and risk scoring algorithms.
"""

import os
import hashlib
from pathlib import Path
from typing import Dict, Any, Tuple
from datetime import timedelta

import pandas as pd
import numpy as np
from scipy import stats
import structlog

logger = structlog.get_logger()


class FeatureEngineer:
    """Engineers features for procurement corruption detection."""

    def __init__(self):
        self.dataset_path = Path("dataset")

    def load_dataset(self) -> pd.DataFrame:
        """Load the joined procurement dataset."""
        file_path = self.dataset_path / "procurements.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"Dataset not found: {file_path}")

        logger.info(f"Loading dataset from {file_path}")
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded dataset: {len(df)} records, {len(df.columns)} columns")

        return df

    def calculate_price_zscore(self, df: pd.DataFrame) -> pd.Series:
        """Calculate price Z-score using MAD (Median Absolute Deviation)."""
        logger.info("Calculating price Z-scores using MAD")

        price_z = pd.Series(0.0, index=df.index)

        # Group by title_ru for similar procurements
        if "title_ru" in df.columns and "contract_sum" in df.columns:
            for title, group in df.groupby("title_ru"):
                if len(group) >= 3:  # Need at least 3 samples for meaningful statistics
                    prices = group["contract_sum"].dropna()
                    if len(prices) >= 3:
                        # Calculate MAD-based Z-score
                        median = prices.median()
                        mad = np.median(np.abs(prices - median))

                        if mad > 0:
                            z_scores = 0.6745 * (prices - median) / mad
                            price_z.loc[group.index] = z_scores.reindex(
                                group.index
                            ).fillna(0)

        logger.info(
            f"Price Z-score stats: mean={price_z.mean():.2f}, std={price_z.std():.2f}, max={price_z.max():.2f}"
        )
        return price_z

    def detect_single_bidder(self, df: pd.DataFrame) -> pd.Series:
        """Detect single bidder procurements."""
        logger.info("Detecting single bidder procurements")

        # Method ID 6 indicates single source procurement
        single_flag = (
            ((df.get("order_method_id") == 6) | (df.get("lot_method_id") == 6))
            .fillna(False)
            .astype(int)
        )

        single_count = single_flag.sum()
        logger.info(
            f"Found {single_count} single bidder procurements ({single_count/len(df)*100:.1f}%)"
        )

        return single_flag

    def detect_repeated_winners(self, df: pd.DataFrame) -> pd.Series:
        """Detect repeated winner patterns (potential collusion)."""
        logger.info("Detecting repeated winner patterns")

        repeat_flag = pd.Series(0, index=df.index)

        if "customer_bin" in df.columns and "provider_bin" in df.columns:
            # Calculate win rates for each customer-provider pair
            for (customer, provider), group in df.groupby(
                ["customer_bin", "provider_bin"]
            ):
                if pd.isna(customer) or pd.isna(provider):
                    continue

                # Get all contracts for this customer
                customer_contracts = df[df["customer_bin"] == customer]
                total_contracts = len(customer_contracts)
                provider_wins = len(group)

                # Flag as suspicious if provider wins >60% and has ≥5 contracts
                if total_contracts >= 5 and provider_wins / total_contracts > 0.6:
                    repeat_flag.loc[group.index] = 1

        repeat_count = repeat_flag.sum()
        logger.info(
            f"Found {repeat_count} repeated winner cases ({repeat_count/len(df)*100:.1f}%)"
        )

        return repeat_flag

    def detect_contract_splitting(self, df: pd.DataFrame) -> pd.Series:
        """Detect contract splitting (circumventing thresholds)."""
        logger.info("Detecting contract splitting patterns")

        split_flag = pd.Series(0, index=df.index)

        if all(
            col in df.columns
            for col in ["customer_bin", "title_ru", "contract_sum", "lot_start_date"]
        ):
            # Group by customer and title
            for (customer, title), group in df.groupby(["customer_bin", "title_ru"]):
                if pd.isna(customer) or pd.isna(title) or len(group) < 3:
                    continue

                # Check for small contracts (≤100,000 KZT) within 30 days
                small_contracts = group[group["contract_sum"] <= 100000]

                if len(small_contracts) >= 3:
                    # Check if they're within 30 days of each other
                    dates = pd.to_datetime(small_contracts["lot_start_date"]).dropna()
                    if len(dates) >= 3:
                        date_range = dates.max() - dates.min()
                        if date_range <= timedelta(days=30):
                            split_flag.loc[small_contracts.index] = 1

        split_count = split_flag.sum()
        logger.info(
            f"Found {split_count} contract splitting cases ({split_count/len(df)*100:.1f}%)"
        )

        return split_flag

    def detect_underpayment(self, df: pd.DataFrame) -> pd.Series:
        """Detect underpayment patterns."""
        logger.info("Detecting underpayment patterns")

        underpaid_flag = pd.Series(0, index=df.index)

        if "paid_sum" in df.columns and "contract_sum" in df.columns:
            # Flag contracts where payment is <90% of contract value
            mask = (
                (df["paid_sum"].notna())
                & (df["contract_sum"].notna())
                & (df["contract_sum"] > 0)
                & (df["paid_sum"] < 0.9 * df["contract_sum"])
            )
            underpaid_flag.loc[mask] = 1

        underpaid_count = underpaid_flag.sum()
        logger.info(
            f"Found {underpaid_count} underpayment cases ({underpaid_count/len(df)*100:.1f}%)"
        )

        return underpaid_flag

    def calculate_risk_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate composite risk score."""
        logger.info("Calculating composite risk scores")

        # Risk score formula: 2*price_flag + 1.5*single_flag + 1.5*repeat_flag + split_flag + underpaid_flag
        risk_score = (
            2.0 * df["price_flag"].fillna(0)
            + 1.5 * df["single_flag"].fillna(0)
            + 1.5 * df["repeat_flag"].fillna(0)
            + 1.0 * df["split_flag"].fillna(0)
            + 1.0 * df["underpaid_flag"].fillna(0)
        )

        logger.info(
            f"Risk score stats: mean={risk_score.mean():.2f}, std={risk_score.std():.2f}, max={risk_score.max():.2f}"
        )

        # Risk categories
        risk_categories = pd.cut(
            risk_score, bins=[-np.inf, 1, 3, np.inf], labels=["Low", "Medium", "High"]
        )

        category_counts = risk_categories.value_counts()
        logger.info(f"Risk distribution: {category_counts.to_dict()}")

        return risk_score

    def create_composite_keys(self, df: pd.DataFrame) -> pd.Series:
        """Create composite keys for records missing lot_id."""
        logger.info("Creating composite keys for missing lot_id")

        composite_keys = pd.Series("", index=df.index)

        # For records with missing lot_id, create fallback key
        missing_mask = df["lot_id"].isna()

        if missing_mask.any():
            for idx in df[missing_mask].index:
                row = df.loc[idx]

                # Composite key: sha256(lot_number + customer_bin + round(contract_sum,0))
                key_parts = [
                    str(row.get("lot_number", "")),
                    str(row.get("customer_bin", "")),
                    str(round(row.get("contract_sum", 0))),
                ]

                key_string = "|".join(key_parts)
                composite_key = hashlib.sha256(key_string.encode()).hexdigest()[:16]
                composite_keys.loc[idx] = composite_key

        logger.info(f"Created {missing_mask.sum()} composite keys")
        return composite_keys

    def add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add temporal analysis features."""
        logger.info("Adding temporal features")

        if "lot_start_date" in df.columns:
            df["lot_start_date"] = pd.to_datetime(df["lot_start_date"])
            df["year"] = df["lot_start_date"].dt.year
            df["month"] = df["lot_start_date"].dt.month
            df["quarter"] = df["lot_start_date"].dt.quarter
            df["day_of_week"] = df["lot_start_date"].dt.dayofweek

            # Add fiscal year patterns (Kazakhstan fiscal year)
            df["fiscal_year"] = df["lot_start_date"].dt.year

        if "accept_date" in df.columns:
            df["accept_date"] = pd.to_datetime(df["accept_date"])

            # Time between lot start and acceptance
            if "lot_start_date" in df.columns:
                df["processing_days"] = (
                    (df["accept_date"] - df["lot_start_date"])
                    .astype("timedelta64[D]")
                    .astype(int)
                )

        logger.info("Added temporal features")
        return df

    async def engineer_features(self) -> Dict[str, Any]:
        """Complete feature engineering workflow."""
        logger.info("Starting feature engineering")

        try:
            # Load dataset
            df = self.load_dataset()

            # Add temporal features
            df = self.add_temporal_features(df)

            # Calculate price Z-scores
            df["price_z"] = self.calculate_price_zscore(df)
            df["price_flag"] = (df["price_z"] > 3).astype(int)

            # Detection flags
            df["single_flag"] = self.detect_single_bidder(df)
            df["repeat_flag"] = self.detect_repeated_winners(df)
            df["split_flag"] = self.detect_contract_splitting(df)
            df["underpaid_flag"] = self.detect_underpayment(df)

            # Composite risk score
            df["risk_score"] = self.calculate_risk_score(df)

            # Composite keys for missing lot_id
            df["composite_key"] = self.create_composite_keys(df)

            # Save enriched dataset
            output_file = self.dataset_path / "procurements_featured.parquet"
            df.to_parquet(output_file, compression="zstd", index=False)

            # Summary statistics
            stats = {
                "records": len(df),
                "features_added": [
                    "price_z",
                    "price_flag",
                    "single_flag",
                    "repeat_flag",
                    "split_flag",
                    "underpaid_flag",
                    "risk_score",
                ],
                "risk_distribution": {
                    "low_risk": (df["risk_score"] < 1).sum(),
                    "medium_risk": (
                        (df["risk_score"] >= 1) & (df["risk_score"] < 3)
                    ).sum(),
                    "high_risk": (df["risk_score"] >= 3).sum(),
                },
                "corruption_indicators": {
                    "price_inflation": df["price_flag"].sum(),
                    "single_bidder": df["single_flag"].sum(),
                    "repeated_winners": df["repeat_flag"].sum(),
                    "contract_splitting": df["split_flag"].sum(),
                    "underpayment": df["underpaid_flag"].sum(),
                },
                "file_path": str(output_file),
            }

            logger.info("Feature engineering completed", summary=stats)
            return stats

        except Exception as e:
            logger.error("Feature engineering failed", error=str(e))
            raise
