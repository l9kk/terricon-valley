"""
Streamlit Dashboard

Interactive web interface for EOZ procurement risk visualization and analysis.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def load_config():
    """Load configuration and setup page."""
    st.set_page_config(
        page_title="EOZ Procurement Risk Monitor",
        page_icon="🛰️",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def load_data() -> Optional[pd.DataFrame]:
    """Load the final processed dataset."""
    dataset_path = Path("dataset")

    # Try to load the most complete dataset available
    for filename in [
        "procurements_final.parquet",
        "procurements_featured.parquet",
        "procurements.parquet",
    ]:
        file_path = dataset_path / filename
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path)
                st.sidebar.success(f"Loaded: {filename}")
                return df
            except Exception as e:
                st.sidebar.error(f"Error loading {filename}: {e}")

    st.error("No dataset found! Please run the scraper first.")
    return None


def create_risk_color_scale():
    """Create color scale for risk visualization."""
    return {
        "Low": "#28a745",  # Green
        "Medium": "#ffc107",  # Yellow
        "High": "#dc3545",  # Red
    }


def add_risk_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Add risk category based on risk score."""
    if "combined_risk_score" in df.columns:
        risk_col = "combined_risk_score"
    elif "risk_score" in df.columns:
        risk_col = "risk_score"
    else:
        # Create a basic risk score
        df["risk_score"] = 0
        risk_col = "risk_score"

    df["risk_category"] = pd.cut(
        df[risk_col], bins=[-np.inf, 1, 3, np.inf], labels=["Low", "Medium", "High"]
    )

    return df


def create_sidebar_filters(df: pd.DataFrame) -> Dict:
    """Create sidebar filters and return filter values."""
    st.sidebar.title("🔍 Filters")

    filters = {}

    # Date range filter
    if "lot_start_date" in df.columns:
        df["lot_start_date"] = pd.to_datetime(df["lot_start_date"])
        min_date = df["lot_start_date"].min()
        max_date = df["lot_start_date"].max()

        if pd.notna(min_date) and pd.notna(max_date):
            date_range = st.sidebar.date_input(
                "Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
            if len(date_range) == 2:
                filters["date_range"] = date_range

    # Risk category filter
    if "risk_category" in df.columns:
        risk_categories = st.sidebar.multiselect(
            "Risk Level", options=["Low", "Medium", "High"], default=["Medium", "High"]
        )
        filters["risk_categories"] = risk_categories

    # Contract value range
    if "contract_sum" in df.columns:
        contract_values = df["contract_sum"].dropna()
        if len(contract_values) > 0:
            min_val, max_val = float(contract_values.min()), float(
                contract_values.max()
            )
            value_range = st.sidebar.slider(
                "Contract Value Range (KZT)",
                min_value=min_val,
                max_value=max_val,
                value=(min_val, max_val),
                format="%.0f",
            )
            filters["value_range"] = value_range

    # Customer filter
    if "customer_bin" in df.columns:
        customers = df["customer_bin"].dropna().unique()
        if len(customers) > 0:
            selected_customers = st.sidebar.multiselect(
                "Customer BIN",
                options=sorted(customers)[:100],  # Limit to first 100 for performance
                default=[],
            )
            filters["customers"] = selected_customers

    # Corruption flags
    flag_columns = [col for col in df.columns if col.endswith("_flag")]
    if flag_columns:
        st.sidebar.subheader("🚩 Corruption Indicators")
        selected_flags = []
        for flag in flag_columns:
            if st.sidebar.checkbox(
                flag.replace("_flag", "").replace("_", " ").title(), False
            ):
                selected_flags.append(flag)
        filters["flags"] = selected_flags

    return filters


def apply_filters(df: pd.DataFrame, filters: Dict) -> pd.DataFrame:
    """Apply filters to the dataframe."""
    filtered_df = df.copy()

    # Date range filter
    if "date_range" in filters and len(filters["date_range"]) == 2:
        start_date, end_date = filters["date_range"]
        mask = (filtered_df["lot_start_date"] >= pd.Timestamp(start_date)) & (
            filtered_df["lot_start_date"] <= pd.Timestamp(end_date)
        )
        filtered_df = filtered_df[mask]

    # Risk category filter
    if "risk_categories" in filters and filters["risk_categories"]:
        filtered_df = filtered_df[
            filtered_df["risk_category"].isin(filters["risk_categories"])
        ]

    # Value range filter
    if "value_range" in filters:
        min_val, max_val = filters["value_range"]
        mask = (filtered_df["contract_sum"] >= min_val) & (
            filtered_df["contract_sum"] <= max_val
        )
        filtered_df = filtered_df[mask]

    # Customer filter
    if "customers" in filters and filters["customers"]:
        filtered_df = filtered_df[
            filtered_df["customer_bin"].isin(filters["customers"])
        ]

    # Flag filters
    if "flags" in filters and filters["flags"]:
        # Create boolean mask for any flag being True
        flag_columns = [col for col in filters["flags"] if col in filtered_df.columns]
        if flag_columns:
            # Use OR logic across flag columns
            mask = pd.Series(False, index=filtered_df.index)
            for col in flag_columns:
                mask = mask | filtered_df[col]
            filtered_df = filtered_df[mask]

    return pd.DataFrame(filtered_df)


def create_summary_metrics(df: pd.DataFrame):
    """Create summary metrics cards."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Contracts", f"{len(df):,}", delta=None)

    with col2:
        if "contract_sum" in df.columns:
            total_value = df["contract_sum"].sum()
            st.metric("Total Value (KZT)", f"{total_value:,.0f}", delta=None)

    with col3:
        if "risk_category" in df.columns:
            high_risk = (df["risk_category"] == "High").sum()
            st.metric(
                "High Risk Contracts",
                f"{high_risk:,}",
                delta=f"{high_risk/len(df)*100:.1f}%",
            )

    with col4:
        if "customer_bin" in df.columns:
            unique_customers = df["customer_bin"].nunique()
            st.metric("Unique Customers", f"{unique_customers:,}", delta=None)


def create_risk_distribution_chart(df: pd.DataFrame):
    """Create risk distribution visualization."""
    if "risk_category" not in df.columns:
        st.warning("Risk categories not available")
        return

    st.subheader("📊 Risk Distribution")

    risk_counts = df["risk_category"].value_counts()
    colors = create_risk_color_scale()

    # Create pie chart
    fig = px.pie(
        values=risk_counts.values,
        names=risk_counts.index,
        color=risk_counts.index,
        color_discrete_map=colors,
        title="Contract Risk Distribution",
    )

    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)


def create_temporal_analysis(df: pd.DataFrame):
    """Create temporal risk analysis."""
    if "lot_start_date" not in df.columns or "risk_category" not in df.columns:
        st.warning("Temporal data not available")
        return

    st.subheader("📈 Risk Trends Over Time")

    # Aggregate by month
    df["month"] = df["lot_start_date"].dt.to_period("M")
    monthly_risk = df.groupby(["month", "risk_category"]).size().unstack(fill_value=0)

    if not monthly_risk.empty:
        colors = create_risk_color_scale()

        fig = go.Figure()

        for risk_level in ["Low", "Medium", "High"]:
            if risk_level in monthly_risk.columns:
                fig.add_trace(
                    go.Scatter(
                        x=monthly_risk.index.astype(str),
                        y=monthly_risk[risk_level],
                        mode="lines+markers",
                        name=risk_level,
                        line=dict(color=colors[risk_level]),
                    )
                )

        fig.update_layout(
            title="Risk Trends by Month",
            xaxis_title="Month",
            yaxis_title="Number of Contracts",
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)


def create_corruption_indicators_chart(df: pd.DataFrame):
    """Create corruption indicators visualization."""
    flag_columns = [col for col in df.columns if col.endswith("_flag")]

    if not flag_columns:
        st.warning("Corruption indicators not available")
        return

    st.subheader("🚨 Corruption Indicators")

    # Calculate flag frequencies
    flag_counts = df[flag_columns].sum().sort_values(ascending=True)
    flag_names = [
        name.replace("_flag", "").replace("_", " ").title()
        for name in flag_counts.index
    ]

    fig = px.bar(
        x=flag_counts.values,
        y=flag_names,
        orientation="h",
        title="Corruption Indicators Frequency",
        color=flag_counts.values,
        color_continuous_scale="Reds",
    )

    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


def create_risk_table(df: pd.DataFrame):
    """Create interactive risk table with traffic light colors."""
    st.subheader("📋 High-Risk Contracts")

    # Select relevant columns for display
    display_columns = []
    preferred_columns = [
        "lot_id",
        "customer_bin",
        "provider_bin",
        "contract_sum",
        "title_ru",
        "lot_start_date",
        "risk_category",
    ]

    for col in preferred_columns:
        if col in df.columns:
            display_columns.append(col)

    # Add risk score if available
    if "combined_risk_score" in df.columns:
        display_columns.append("combined_risk_score")
    elif "risk_score" in df.columns:
        display_columns.append("risk_score")

    # Add corruption flags
    flag_columns = [col for col in df.columns if col.endswith("_flag")]
    display_columns.extend(flag_columns[:5])  # Limit to first 5 flags

    # Filter for high and medium risk only
    if "risk_category" in df.columns:
        high_risk_df = df[df["risk_category"].isin(["High", "Medium"])]
    else:
        high_risk_df = df.head(100)  # Show first 100 if no risk categories

    if len(high_risk_df) == 0:
        st.info("No high-risk contracts found with current filters")
        return

    # Display table
    display_df = high_risk_df[display_columns].copy()

    # Format numeric columns
    for col in display_df.columns:
        if df[col].dtype in ["float64", "int64"] and "sum" in col.lower():
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else ""
            )

    # Color coding based on risk
    def highlight_risk(row):
        if "risk_category" in row.index:
            if row["risk_category"] == "High":
                return ["background-color: #ffebee"] * len(row)
            elif row["risk_category"] == "Medium":
                return ["background-color: #fff8e1"] * len(row)
        return [""] * len(row)

    styled_df = display_df.style.apply(highlight_risk, axis=1)
    st.dataframe(styled_df, use_container_width=True, height=400)

    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Download High-Risk Contracts CSV",
        data=csv,
        file_name=f"high_risk_contracts_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )


def create_analytics_section(df: pd.DataFrame):
    """Create advanced analytics section."""
    st.subheader("🔬 Advanced Analytics")

    tab1, tab2, tab3 = st.tabs(
        ["Customer Analysis", "Provider Analysis", "Market Concentration"]
    )

    with tab1:
        if "customer_bin" in df.columns and "contract_sum" in df.columns:
            customer_stats = (
                df.groupby("customer_bin")
                .agg(
                    {
                        "contract_sum": ["sum", "count", "mean"],
                        "risk_category": lambda x: (
                            (x == "High").sum() if "risk_category" in df.columns else 0
                        ),
                    }
                )
                .round(2)
            )

            customer_stats.columns = [
                "Total Value",
                "Contract Count",
                "Avg Value",
                "High Risk Count",
            ]
            customer_stats = customer_stats.sort_values(
                "Total Value", ascending=False
            ).head(20)

            st.write("Top 20 Customers by Contract Value")
            st.dataframe(customer_stats, use_container_width=True)

    with tab2:
        if "provider_bin" in df.columns and "contract_sum" in df.columns:
            provider_stats = (
                df.groupby("provider_bin")
                .agg(
                    {
                        "contract_sum": ["sum", "count", "mean"],
                        "customer_bin": "nunique",
                        "risk_category": lambda x: (
                            (x == "High").sum() if "risk_category" in df.columns else 0
                        ),
                    }
                )
                .round(2)
            )

            provider_stats.columns = [
                "Total Value",
                "Contract Count",
                "Avg Value",
                "Unique Customers",
                "High Risk Count",
            ]
            provider_stats = provider_stats.sort_values(
                "Total Value", ascending=False
            ).head(20)

            st.write("Top 20 Providers by Contract Value")
            st.dataframe(provider_stats, use_container_width=True)

    with tab3:
        if "provider_bin" in df.columns and "customer_bin" in df.columns:
            # Market concentration analysis
            total_contracts = len(df)
            top_providers = df["provider_bin"].value_counts().head(10)
            concentration_pct = (top_providers / total_contracts * 100).round(1)

            fig = px.bar(
                x=concentration_pct.values,
                y=concentration_pct.index,
                orientation="h",
                title="Top 10 Providers Market Share (%)",
                labels={"x": "Market Share (%)", "y": "Provider BIN"},
            )
            st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard function."""
    load_config()

    # Header
    st.title("🛰️ EOZ Procurement Risk Monitor")
    st.markdown(
        "**AI-powered corruption detection for Kazakhstan's public procurement**"
    )

    # Load data
    df = load_data()
    if df is None:
        st.stop()

    # Type assertion since we've checked for None
    assert df is not None

    # Add risk categories
    df = add_risk_categories(df)

    # Sidebar filters
    filters = create_sidebar_filters(df)
    filtered_df = apply_filters(df, filters)

    # Main content
    if len(filtered_df) == 0:
        st.warning("No data matches the selected filters")
        st.stop()

    # Summary metrics
    create_summary_metrics(filtered_df)

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        create_risk_distribution_chart(filtered_df)

    with col2:
        create_corruption_indicators_chart(filtered_df)

    # Temporal analysis
    create_temporal_analysis(filtered_df)

    # Risk table
    create_risk_table(filtered_df)

    # Advanced analytics
    create_analytics_section(filtered_df)

    # Footer
    st.markdown("---")
    st.markdown("*Data source: EOZ Kazakhstan Public Procurement Portal*")
    st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")


if __name__ == "__main__":
    main()
