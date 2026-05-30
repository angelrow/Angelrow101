# SPX & VIX 1-Minute Intraday Data

Historical 1-minute intraday data for **$SPX** and **$VIX**, organised one CSV per calendar month.

## Coverage

- **Range:** January 2016 – May 2026
- **Files:** 125 monthly CSVs per symbol (250 total)
- **Source:** Barchart.com historical data download (Premier)
- **Columns:** `Time, Open, High, Low, Latest, Change, %Change, Volume`

## Folder structure

```
SPX - JANUARY 2016 - MAY 2026 - (1 MIN DATA)/
    spx_intraday-1min_historical-data-download-JANUARY-2016.csv
    ... (one file per month) ...
    spx_intraday-1min_historical-data-download-MAY-2026.csv

VIX - JANUARY 2016 - MAY 2026 - (1 MIN DATA)/
    vix_intraday-1min_historical-data-download-JANUARY-2016.csv
    ... (one file per month) ...
    vix_intraday-1min_historical-data-download-MAY-2026.csv
```

## Notes on data quality

- Each monthly file stays under Barchart's 10,000-row-per-download cap.
- **VIX coverage depth changes over time:** early-2016 months hold ~400 bars/day
  (regular session only), while recent years hold ~764 bars/day (extended hours).
  This reflects how far back Barchart's extended-hours history reaches — the early
  months are complete, just thinner. Keep this in mind when comparing months.
- All months January 2016 – May 2026 are present exactly once, with no duplicates
  or gaps.
