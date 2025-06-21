"""
Risk Model

Implements Isolation Forest for anomaly detection and risk scoring.
"""

import os
import pickle
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import structlog

logger = structlog.get_logger()


class RiskModel:
    """Trains and applies Isolation Forest for procurement risk detection."""

    def __init__(self):
        self.dataset_path = Path("dataset")
        self.model_path = Path("models")
        self.model_path.mkdir(exist_ok=True)

        # Model parameters from environment
        self.contamination = float(os.getenv("ISOLATION_CONTAMINATION", "0.05"))
        self.random_state = int(os.getenv("RANDOM_STATE", "42"))

        self.model: Optional[IsolationForest] = None
        self.scaler: Optional[StandardScaler] = None
        self.feature_columns: List[str] = []

    def load_dataset(self) -> pd.DataFrame:
        """Load the feature-engineered dataset."""
        file_path = self.dataset_path / "procurements_featured.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"Featured dataset not found: {file_path}")

        logger.info(f"Loading featured dataset from {file_path}")
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded dataset: {len(df)} records, {len(df.columns)} columns")

        return df

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for machine learning."""
        logger.info("Preparing features for ML model")

        # Select numerical features for anomaly detection
        feature_candidates = [
            "contract_sum",
            "lot_amount",
            "plan_price",
            "price_ratio",
            "payment_ratio",
            "price_z",
            "processing_days",
            "year",
            "month",
            "quarter",
        ]

        # Keep only existing columns
        self.feature_columns = [col for col in feature_candidates if col in df.columns]

        if not self.feature_columns:
            raise ValueError("No suitable features found for model training")

        # Create feature matrix
        X = df[self.feature_columns].copy()

        # Handle missing values
        X = X.fillna(X.median())

        # Remove infinite values
        X = X.replace([np.inf, -np.inf], np.nan)
        X = X.fillna(X.median())

        logger.info(
            f"Prepared {len(self.feature_columns)} features: {self.feature_columns}"
        )
        return X

    def _train_isolation_forest(self, X: pd.DataFrame) -> Dict[str, Any]:
        """Train the Isolation Forest model."""
        logger.info("Training Isolation Forest model")

        # Initialize scaler
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        # Initialize and train Isolation Forest
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100,
            max_samples="auto",
            n_jobs=-1,
        )

        logger.info(f"Training with contamination={self.contamination}")
        self.model.fit(X_scaled)

        # Get anomaly scores and predictions
        anomaly_scores = self.model.decision_function(X_scaled)
        predictions = self.model.predict(X_scaled)

        # Calculate statistics
        n_anomalies = (predictions == -1).sum()
        anomaly_rate = n_anomalies / len(X)

        stats = {
            "total_samples": len(X),
            "features_used": len(self.feature_columns),
            "anomalies_detected": int(n_anomalies),
            "anomaly_rate": float(anomaly_rate),
            "contamination_target": self.contamination,
            "score_stats": {
                "min": float(anomaly_scores.min()),
                "max": float(anomaly_scores.max()),
                "mean": float(anomaly_scores.mean()),
                "std": float(anomaly_scores.std()),
            },
        }

        logger.info(
            "Model training completed",
            **{k: v for k, v in stats.items() if k != "score_stats"},
        )
        return stats

    def save_model(self) -> Dict[str, str]:
        """Save the trained model and scaler."""
        logger.info("Saving trained model")

        if not self.model or not self.scaler:
            raise ValueError("Model not trained yet")

        # Save model
        model_file = self.model_path / "isolation_forest.pkl"
        with model_file.open("wb") as f:
            pickle.dump(self.model, f)

        # Save scaler
        scaler_file = self.model_path / "scaler.pkl"
        with scaler_file.open("wb") as f:
            pickle.dump(self.scaler, f)

        # Save feature columns
        features_file = self.model_path / "features.pkl"
        with features_file.open("wb") as f:
            pickle.dump(self.feature_columns, f)

        files_saved = {
            "model": str(model_file),
            "scaler": str(scaler_file),
            "features": str(features_file),
        }

        logger.info("Model saved successfully", files=files_saved)
        return files_saved

    def load_model(self) -> bool:
        """Load a previously trained model."""
        logger.info("Loading trained model")

        try:
            # Load model
            model_file = self.model_path / "isolation_forest.pkl"
            with model_file.open("rb") as f:
                self.model = pickle.load(f)

            # Load scaler
            scaler_file = self.model_path / "scaler.pkl"
            with scaler_file.open("rb") as f:
                self.scaler = pickle.load(f)

            # Load feature columns
            features_file = self.model_path / "features.pkl"
            with features_file.open("rb") as f:
                self.feature_columns = pickle.load(f)

            logger.info(
                f"Model loaded successfully with {len(self.feature_columns)} features"
            )
            return True

        except Exception as e:
            logger.warning("Failed to load saved model", error=str(e))
            return False

    def predict_risks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the trained model to predict risks."""
        logger.info("Applying model to predict risks")

        if not self.model or not self.scaler:
            raise ValueError("Model not trained or loaded")

        # Prepare features
        X = self.prepare_features(df)
        X_scaled = self.scaler.transform(X)

        # Get predictions and scores
        anomaly_scores = self.model.decision_function(X_scaled)
        predictions = self.model.predict(X_scaled)

        # Add results to dataframe
        result_df = df.copy()
        result_df["anomaly_score"] = anomaly_scores
        result_df["is_anomaly"] = (predictions == -1).astype(int)

        # Normalize anomaly scores to 0-1 scale for interpretability
        score_min, score_max = anomaly_scores.min(), anomaly_scores.max()
        if score_max > score_min:
            result_df["anomaly_score_normalized"] = (anomaly_scores - score_min) / (
                score_max - score_min
            )
        else:
            result_df["anomaly_score_normalized"] = 0.5

        # Combine with existing risk score
        if "risk_score" in result_df.columns:
            result_df["combined_risk_score"] = (
                0.6 * result_df["risk_score"]
                + 0.4 * result_df["anomaly_score_normalized"] * 5
            )
        else:
            result_df["combined_risk_score"] = result_df["anomaly_score_normalized"] * 5

        logger.info(
            f"Risk prediction completed: {(predictions == -1).sum()} anomalies detected"
        )
        return result_df

    async def train_complete_workflow(self) -> Dict[str, Any]:
        """Complete model training workflow."""
        logger.info("Starting risk model training")

        try:
            # Load dataset
            df = self.load_dataset()

            # Prepare features
            X = self.prepare_features(df)

            # Train model
            training_stats = self._train_isolation_forest(X)

            # Save model
            saved_files = self.save_model()

            # Apply model to full dataset and save results
            df_with_predictions = self.predict_risks(df)

            # Save final dataset with ML predictions
            output_file = self.dataset_path / "procurements_final.parquet"
            df_with_predictions.to_parquet(output_file, compression="zstd", index=False)

            # Final statistics
            final_stats = {
                **training_stats,
                "model_files": saved_files,
                "final_dataset": str(output_file),
                "final_anomalies": int(df_with_predictions["is_anomaly"].sum()),
                "high_risk_combined": int(
                    (df_with_predictions["combined_risk_score"] >= 3).sum()
                ),
            }

            logger.info("Risk model training completed", summary=final_stats)
            return final_stats

        except Exception as e:
            logger.error("Risk model training failed", error=str(e))
            raise

    def evaluate_model(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Evaluate model performance against known risk indicators."""
        logger.info("Evaluating model performance")

        try:
            # Compare anomaly detection with existing risk flags
            risk_flags = [
                "price_flag",
                "single_flag",
                "repeat_flag",
                "split_flag",
                "underpaid_flag",
            ]
            existing_flags = [flag for flag in risk_flags if flag in df.columns]

            if existing_flags:
                # Create composite risk indicator
                df["has_risk_flag"] = df[existing_flags].sum(axis=1) > 0

                # Compare with anomaly detection
                from sklearn.metrics import classification_report, confusion_matrix

                if "is_anomaly" in df.columns:
                    # Convert anomaly prediction to binary (1 for anomaly, 0 for normal)
                    y_true = df["has_risk_flag"].astype(int)
                    y_pred = df["is_anomaly"]

                    # Calculate metrics
                    report = classification_report(y_true, y_pred, output_dict=True)

                    # Safely extract metrics with proper typing
                    evaluation = {
                        "precision": (
                            report.get("1", {}).get("precision", 0)
                            if isinstance(report, dict)
                            else 0
                        ),
                        "recall": (
                            report.get("1", {}).get("recall", 0)
                            if isinstance(report, dict)
                            else 0
                        ),
                        "f1_score": (
                            report.get("1", {}).get("f1-score", 0)
                            if isinstance(report, dict)
                            else 0
                        ),
                        "accuracy": (
                            report.get("accuracy", 0) if isinstance(report, dict) else 0
                        ),
                        "support": (
                            int(report.get("1", {}).get("support", 0))
                            if isinstance(report, dict)
                            else 0
                        ),
                    }

                    logger.info("Model evaluation completed", metrics=evaluation)
                    return evaluation

            logger.warning("Cannot evaluate model - insufficient risk indicators")
            return {}

        except Exception as e:
            logger.error("Model evaluation failed", error=str(e))
            return {}
