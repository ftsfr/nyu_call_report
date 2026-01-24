# NYU Call Report Pipeline

This pipeline downloads and processes bank call report data from NYU Stern.

## Data Source

The data is publicly available from [NYU Stern](https://pages.stern.nyu.edu/~pschnabl/data/data_callreport.htm).

## Outputs

- `ftsfr_nyu_call_report_leverage.parquet`: Bank-level leverage (Assets/Equity)
- `ftsfr_nyu_call_report_holding_company_leverage.parquet`: Holding company leverage
- `ftsfr_nyu_call_report_cash_liquidity.parquet`: Bank-level cash liquidity (Cash/Assets)
- `ftsfr_nyu_call_report_holding_company_cash_liquidity.parquet`: Holding company cash liquidity

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the pipeline:
   ```bash
   doit
   ```

3. View the generated documentation in `docs/index.html`

## Data Coverage

- Time period: 1976-2020
- Frequency: Quarterly
- Granularity: Individual banks and bank holding companies

## Academic References

### Primary Paper

- **Drechsler, Savov, and Schnabl (2017)** - "The Deposits Channel of Monetary Policy"
  - Quarterly Journal of Economics
  - Uses call report data to study bank deposit spreads and monetary policy transmission

### Key Findings

- Deposit spreads increase with the Fed funds rate
- Market power in deposit markets allows banks to widen spreads
- The deposits channel provides a new perspective on how monetary policy affects bank lending
