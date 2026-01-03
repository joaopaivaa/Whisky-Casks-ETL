# :inbox_tray: Whisky Casks ETL

Comprehensive ETL project for extracting, cleaning, transforming and consolidating whisky casks auction data from multiple auction houses websites into analysis-ready datasets.

## Project overview

This repository centralizes the end-to-end data pipeline used to collect whisky casks listings and auction results from several online auction houses. The pipeline is organized into separate extraction (scrapers), cleaning (per-source normalization), and consolidation (gold-layer transformation) steps. The final outputs are CSV tables suitable for analysis and valuation modeling.

## Repository structure

- `bronze/` — Raw, unmodified CSV exports generated directly by scrapers.
- `silver/` — Cleaned, normalized datasets per source.
- `gold/` — Consolidated, enriched and valuation-ready datasets.
- `dim_distilleries_info/` — Auxiliary data and scraper for distilleries adata.
- R scripts: files named like `* - Scraper.R` and `* - Data Cleaning.R`, plus `transformation_gold_layer.R`.
- Python scripts: `Whisky Hammer - Images Analysis.py` (image processing/analysis).

## Data layers and flow

1. Extraction: each `* - Scraper.R` crawls a target auction site and writes a CSV into `bronze/`.
2. Cleaning: each `* - Data Cleaning.R` reads its corresponding bronze CSV, applies normalization rules (column types, deduplication, text cleaning) and writes to `silver/`.
3. Consolidation: `transformation_gold_layer.R` reads all `silver/` datasets, performs joins, enrichment and inflation adjustment, and writes final database to `gold/`.

## Outputs

Primary outputs in `gold/` include:

- `casks_database.csv` — Consolidated data.
- `casks_database__casks_valuation.csv` — Selected columns and filtered rows to be applied on valuation modeling project.

## Contribution notes

If you want to contribute a new scraper or cleaning rule:

1. Add a new scraper `MySource - Scraper.R` that writes `bronze/MySource - Casks Database.csv`.
2. Add a cleaning script `MySource - Data Cleaning.R` that reads the bronze CSV and writes `silver/my_source.csv`.
3. Update `transformation_gold_layer.R` if necessary to incorporate the new source.
4. Open a pull request with a clear description, sample input (small CSV) and expected output.

## License

If this repository includes a license file, follow those documents for community and legal guidance.
