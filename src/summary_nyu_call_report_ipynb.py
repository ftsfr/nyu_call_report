# %%
"""
# NYU Call Report Data

This notebook provides summary statistics and visualizations for the NYU Call Report data
containing bank-level leverage and liquidity metrics.

## Data Source

Data is publicly available from [NYU Stern](https://pages.stern.nyu.edu/~pschnabl/data/data_callreport.htm).
"""

# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"

warnings.filterwarnings("ignore")

# %%
"""
## Load the Datasets

We have four datasets:
1. **Bank Leverage**: Total assets / Total equity at individual bank level
2. **Holding Company Leverage**: Aggregated at bank holding company level
3. **Bank Cash Liquidity**: Cash / Total assets at individual bank level
4. **Holding Company Cash Liquidity**: Aggregated at holding company level
"""

# %%
# Load leverage data
leverage_df = pd.read_parquet(DATA_DIR / "ftsfr_nyu_call_report_leverage.parquet")
print(f"Bank Leverage shape: {leverage_df.shape}")
leverage_df.head()

# %%
# Load holding company leverage
hc_leverage_df = pd.read_parquet(DATA_DIR / "ftsfr_nyu_call_report_holding_company_leverage.parquet")
print(f"Holding Company Leverage shape: {hc_leverage_df.shape}")
hc_leverage_df.head()

# %%
# Load cash liquidity data
cash_df = pd.read_parquet(DATA_DIR / "ftsfr_nyu_call_report_cash_liquidity.parquet")
print(f"Bank Cash Liquidity shape: {cash_df.shape}")
cash_df.head()

# %%
# Load holding company cash liquidity
hc_cash_df = pd.read_parquet(DATA_DIR / "ftsfr_nyu_call_report_holding_company_cash_liquidity.parquet")
print(f"Holding Company Cash Liquidity shape: {hc_cash_df.shape}")
hc_cash_df.head()

# %%
"""
## Summary Statistics - Bank Leverage
"""

# %%
# Summary statistics for leverage
print("Bank Leverage Summary:")
print(f"  Number of unique banks: {leverage_df['unique_id'].nunique()}")
print(f"  Date range: {leverage_df['ds'].min()} to {leverage_df['ds'].max()}")
print(f"  Observations: {len(leverage_df)}")
print(f"\nLeverage Statistics:")
print(leverage_df['y'].describe())

# %%
"""
## Time Series - Aggregate Leverage

Cross-sectional median leverage over time.
"""

# %%
# Calculate aggregate statistics over time
leverage_ts = leverage_df.groupby('ds')['y'].agg(['median', 'mean', 'std', 'count'])
leverage_ts.columns = ['median', 'mean', 'std', 'count']

fig, axes = plt.subplots(2, 1, figsize=(14, 10))

# Median leverage over time
ax = axes[0]
ax.plot(leverage_ts.index, leverage_ts['median'], linewidth=1, label='Median')
ax.fill_between(leverage_ts.index,
                leverage_ts['median'] - leverage_ts['std'],
                leverage_ts['median'] + leverage_ts['std'],
                alpha=0.3, label='+/- 1 Std')
ax.set_title('Bank Leverage Over Time (Median with +/- 1 Std)', fontsize=12)
ax.set_xlabel('Date')
ax.set_ylabel('Leverage (Assets/Equity)')
ax.legend()
ax.grid(True, alpha=0.3)

# Number of banks over time
ax = axes[1]
ax.plot(leverage_ts.index, leverage_ts['count'], linewidth=1, color='green')
ax.set_title('Number of Banks in Sample Over Time', fontsize=12)
ax.set_xlabel('Date')
ax.set_ylabel('Number of Banks')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# %%
"""
## Summary Statistics - Cash Liquidity
"""

# %%
# Summary statistics for cash liquidity
print("Bank Cash Liquidity Summary:")
print(f"  Number of unique banks: {cash_df['unique_id'].nunique()}")
print(f"  Date range: {cash_df['ds'].min()} to {cash_df['ds'].max()}")
print(f"  Observations: {len(cash_df)}")
print(f"\nCash Liquidity Statistics:")
print(cash_df['y'].describe())

# %%
"""
## Time Series - Aggregate Cash Liquidity
"""

# %%
# Calculate aggregate statistics over time
cash_ts = cash_df.groupby('ds')['y'].agg(['median', 'mean', 'std'])

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(cash_ts.index, cash_ts['median'], linewidth=1, label='Median')
ax.fill_between(cash_ts.index,
                cash_ts['median'] - cash_ts['std'],
                cash_ts['median'] + cash_ts['std'],
                alpha=0.3, label='+/- 1 Std')
ax.set_title('Bank Cash Liquidity Over Time (Median with +/- 1 Std)', fontsize=12)
ax.set_xlabel('Date')
ax.set_ylabel('Cash Liquidity (Cash/Assets)')
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# %%
"""
## Distribution Analysis
"""

# %%
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Bank leverage distribution
ax = axes[0, 0]
leverage_df['y'].clip(upper=50).hist(ax=ax, bins=100, edgecolor='black', alpha=0.7)
ax.set_title('Bank Leverage Distribution (capped at 50)', fontsize=10)
ax.set_xlabel('Leverage')
ax.set_ylabel('Frequency')

# Holding company leverage distribution
ax = axes[0, 1]
hc_leverage_df['y'].clip(upper=50).hist(ax=ax, bins=100, edgecolor='black', alpha=0.7)
ax.set_title('Holding Company Leverage Distribution (capped at 50)', fontsize=10)
ax.set_xlabel('Leverage')
ax.set_ylabel('Frequency')

# Bank cash liquidity distribution
ax = axes[1, 0]
cash_df['y'].hist(ax=ax, bins=100, edgecolor='black', alpha=0.7)
ax.set_title('Bank Cash Liquidity Distribution', fontsize=10)
ax.set_xlabel('Cash/Assets')
ax.set_ylabel('Frequency')

# Holding company cash liquidity distribution
ax = axes[1, 1]
hc_cash_df['y'].hist(ax=ax, bins=100, edgecolor='black', alpha=0.7)
ax.set_title('Holding Company Cash Liquidity Distribution', fontsize=10)
ax.set_xlabel('Cash/Assets')
ax.set_ylabel('Frequency')

plt.tight_layout()
plt.show()

# %%
"""
## Summary

This dataset provides quarterly bank-level and holding company-level metrics from
regulatory call reports (1976-2020). Key metrics include:

- **Leverage**: Total Assets / Total Equity - measures bank capital structure
- **Cash Liquidity**: Cash / Total Assets - measures liquid asset holdings

These metrics are widely used in:
- Banking regulation and stress testing
- Financial stability monitoring
- Academic research on bank behavior and risk
"""
