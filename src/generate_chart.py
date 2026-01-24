"""Generate interactive HTML chart for NYU Call Report."""

import pandas as pd
import plotly.express as px
import os
from pathlib import Path

# Get the project root (one level up from src/)
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "_data"
OUTPUT_DIR = PROJECT_ROOT / "_output"


def generate_bank_leverage_chart():
    """Generate bank leverage time series chart from NYU Call Report data."""
    # Load NYU Call Report leverage data
    df = pd.read_parquet(DATA_DIR / "ftsfr_nyu_call_report_leverage.parquet")

    # Create line chart
    fig = px.line(
        df.sort_values("ds"),
        x="ds",
        y="y",
        color="unique_id",
        title="Bank Leverage from NYU Call Report (Drechsler et al. 2017)",
        labels={
            "ds": "Date",
            "y": "Leverage Ratio",
            "unique_id": "Series"
        }
    )

    # Update layout
    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save chart
    output_path = OUTPUT_DIR / "bank_leverage_replication.html"
    fig.write_html(str(output_path))
    print(f"Chart saved to {output_path}")

    return fig


if __name__ == "__main__":
    generate_bank_leverage_chart()
