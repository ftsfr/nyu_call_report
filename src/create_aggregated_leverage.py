"""
Create aggregated leverage datasets by size quartiles.

Two aggregation methods:
1. Equal-weight quartiles (by bank count): Each quartile contains ~25% of banks
2. Value-weight quartiles (by total assets): Each quartile contains ~25% of total banking assets

Banks are sorted by their initial total assets (first observation) and tracked over time.
"""

import sys
sys.path.insert(1, "./src/")

import pandas as pd
import numpy as np
import chartbook
import pull_nyu_call_report

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"


def assign_ew_quartiles(df_initial: pd.DataFrame) -> pd.DataFrame:
    """
    Assign banks to equal-weight quartiles based on initial assets.
    Each quartile contains ~25% of banks by count.
    """
    df_sorted = df_initial.sort_values("initial_assets").reset_index(drop=True)
    n_banks = len(df_sorted)

    # Assign quartile based on rank
    df_sorted["quartile"] = pd.qcut(
        df_sorted["initial_assets"].rank(method="first"),
        q=4,
        labels=["Q1_small_ew", "Q2_ew", "Q3_ew", "Q4_large_ew"]
    )

    return df_sorted[["rssdid", "quartile"]]


def assign_vw_quartiles(df_initial: pd.DataFrame) -> pd.DataFrame:
    """
    Assign banks to value-weight quartiles based on initial assets.
    Each quartile contains ~25% of total banking assets.
    """
    df_sorted = df_initial.sort_values("initial_assets").reset_index(drop=True)

    # Calculate cumulative share of total assets
    total_assets = df_sorted["initial_assets"].sum()
    df_sorted["cum_assets"] = df_sorted["initial_assets"].cumsum()
    df_sorted["cum_share"] = df_sorted["cum_assets"] / total_assets

    # Assign quartiles based on cumulative asset share
    def get_vw_quartile(cum_share):
        if cum_share <= 0.25:
            return "Q1_small_vw"
        elif cum_share <= 0.50:
            return "Q2_vw"
        elif cum_share <= 0.75:
            return "Q3_vw"
        else:
            return "Q4_large_vw"

    df_sorted["quartile"] = df_sorted["cum_share"].apply(get_vw_quartile)

    return df_sorted[["rssdid", "quartile"]]


def create_aggregated_leverage():
    """Create both EW and VW aggregated leverage datasets."""

    # Load raw data
    print("Loading NYU Call Report data...")
    df_all = pull_nyu_call_report.load_nyu_call_report(data_dir=DATA_DIR)

    # Filter out invalid equity values
    df_clean = df_all[(df_all["equity"] > 1e-10) & (df_all["assets"] > 0)].copy()
    df_clean["leverage"] = df_clean["assets"] / df_clean["equity"]

    # Drop infinite/NaN values
    df_clean = df_clean.dropna(subset=["leverage"])
    df_clean = df_clean[~df_clean["leverage"].isin([float("inf"), float("-inf")])]

    print(f"Total observations: {len(df_clean)}")
    print(f"Unique banks: {df_clean['rssdid'].nunique()}")

    # Get initial assets for each bank (first observation)
    df_initial = (
        df_clean.sort_values("date")
        .groupby("rssdid")
        .first()
        .reset_index()[["rssdid", "assets"]]
        .rename(columns={"assets": "initial_assets"})
    )

    print(f"Banks with initial assets: {len(df_initial)}")

    # Create EW quartile assignments
    ew_quartiles = assign_ew_quartiles(df_initial)

    # Create VW quartile assignments
    vw_quartiles = assign_vw_quartiles(df_initial)

    # Print quartile distribution
    print("\nEqual-weight quartile distribution (by bank count):")
    print(ew_quartiles["quartile"].value_counts().sort_index())

    print("\nValue-weight quartile distribution (by bank count):")
    print(vw_quartiles["quartile"].value_counts().sort_index())

    # Merge quartile assignments with leverage data
    df_leverage = df_clean[["rssdid", "date", "leverage", "assets"]].copy()

    # --- Equal-weight aggregation ---
    df_ew = df_leverage.merge(ew_quartiles, on="rssdid")

    # Calculate mean leverage per quartile per date (equal-weight)
    df_ew_agg = (
        df_ew.groupby(["date", "quartile"])
        .agg({"leverage": "mean"})
        .reset_index()
    )

    df_ew_agg = df_ew_agg.rename(columns={
        "quartile": "unique_id",
        "date": "ds",
        "leverage": "y"
    })

    df_ew_agg = df_ew_agg.sort_values(["unique_id", "ds"]).reset_index(drop=True)

    print(f"\nEW aggregated dataset: {len(df_ew_agg)} rows")
    print(f"Unique quartiles: {df_ew_agg['unique_id'].unique().tolist()}")

    # --- Value-weight aggregation ---
    df_vw = df_leverage.merge(vw_quartiles, on="rssdid")

    # Calculate asset-weighted mean leverage per quartile per date
    def weighted_mean(group):
        return np.average(group["leverage"], weights=group["assets"])

    df_vw_agg = (
        df_vw.groupby(["date", "quartile"])
        .apply(weighted_mean, include_groups=False)
        .reset_index(name="leverage")
    )

    df_vw_agg = df_vw_agg.rename(columns={
        "quartile": "unique_id",
        "date": "ds",
        "leverage": "y"
    })

    df_vw_agg = df_vw_agg.sort_values(["unique_id", "ds"]).reset_index(drop=True)

    print(f"\nVW aggregated dataset: {len(df_vw_agg)} rows")
    print(f"Unique quartiles: {df_vw_agg['unique_id'].unique().tolist()}")

    # Save datasets
    ew_path = DATA_DIR / "ftsfr_nyu_call_report_leverage_ew_quartile.parquet"
    vw_path = DATA_DIR / "ftsfr_nyu_call_report_leverage_vw_quartile.parquet"

    df_ew_agg.to_parquet(ew_path)
    df_vw_agg.to_parquet(vw_path)

    print(f"\nSaved: {ew_path}")
    print(f"Saved: {vw_path}")

    return df_ew_agg, df_vw_agg


if __name__ == "__main__":
    create_aggregated_leverage()
