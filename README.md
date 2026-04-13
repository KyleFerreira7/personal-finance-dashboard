# Finance Tracker

A personal finance dashboard built around South African bank statements. The project has two parts: a Python pipeline that extracts and consolidates transaction data from FNB PDF statements, and a self-contained HTML dashboard for visualisation, categorisation, and budget tracking.

![Finance Tracker Dashboard](https://img.shields.io/badge/Python-3.8+-blue) ![License](https://img.shields.io/badge/license-MIT-green)

---

## What it does

Drop your FNB bank statement PDFs into a folder, run one command, and get a clean categorised ledger. Open the dashboard in your browser, load the ledger, and get an interactive view of your spending, income, savings rate, and budget progress — across multiple months and accounts.

The project handles the messy reality of real bank statements: PDFs with inconsistent layouts, transactions that span year boundaries, duplicate entries across overlapping statements, and merchants that need normalising before they mean anything.

---

## Project structure

```
Finance Tracker/
├── pdf_parser.py        # PDF extraction engine — do not run directly
├── pdf_to_ledger.py     # Pipeline script — this is the one you run
├── dashboard.html       # Self-contained dashboard — open in any browser
├── Bank Statements/     # Place your PDF statements here
│   └── *.pdf
└── derived/             # Auto-created on first run
    ├── ledger.csv        # Consolidated transaction ledger
    └── merchant_map.csv  # Merchant → category mapping
```

---

## How it works

### 1. PDF parsing (`pdf_parser.py`)

Uses `pdfplumber` to extract transaction tables from FNB statements. Handles two distinct layouts:

- **Credit card statements** — single signed amount column, `Cr` suffix for credits
- **Cheque / savings account statements** — separate debit and credit columns

The parser detects the statement type automatically, extracts year and month from the filename or statement header text, and applies cross-year logic so December transactions in a January statement get the correct year. Merchant names are cleaned by stripping trailing store codes, phone numbers, and amount artifacts.

### 2. Pipeline (`pdf_to_ledger.py`)

Orchestrates the full process:

1. Discovers all PDFs recursively in `Bank Statements/`
2. Calls `parse_pdf()` on each file, tags with account type and source filename
3. Deduplicates using a transaction ID built from `date|amount|description|file`
4. Applies 35+ regex categorisation rules covering SA merchants and services
5. Falls back to historical ledger for merchants seen in previous runs
6. Merges with any existing `ledger.csv`, preserving manual category edits
7. Rebuilds `merchant_map.csv` as a persistent lookup for future runs

The merchant map is the learning layer — once you manually correct a category in the dashboard and export the CSV, the next pipeline run picks it up automatically. Over time, the "Other" bucket shrinks.

### 3. Dashboard (`dashboard.html`)

A single self-contained HTML file with no server dependency. Load it in any browser, drop in your `ledger.csv`, and it renders entirely client-side. Nothing is uploaded anywhere.

**Overview tab**
- Income vs spend bar chart with net line overlay
- Spend by category horizontal bar chart (sorted by total)
- Top merchants ranked list
- Needs / Wants / Savings % split (50/30/20 benchmark)
- Cumulative net position line chart

**Transactions tab**
- Searchable, filterable table across all accounts
- Inline category editing — select a new category from the dropdown
- Amber highlight on changed rows
- Export updated CSV to feed corrections back into the pipeline

**Budgets tab**
- Monthly income, total budgeted, total spent, and remaining summary
- Per-category budget cards with progress bars (green → amber → red)
- Budget vs actual horizontal bar chart
- Budgets persist in browser localStorage between sessions

---

## Setup

### Requirements

```
Python 3.8+
pdfplumber
pandas
openpyxl
```

Install dependencies:

```bash
pip install pdfplumber pandas openpyxl
```

### Running the pipeline

```bash
# Navigate to the project folder
cd "Finance Tracker"

# Place your FNB PDF statements in Bank Statements/
# Then run:
python pdf_to_ledger.py
```

The script prints a summary showing each PDF parsed, categorisation stats, and a category breakdown. Output is written to `derived/ledger.csv`.

### Using the dashboard

Open `dashboard.html` in any browser. Drop `derived/ledger.csv` onto the upload zone, or click to browse.

To keep manual category edits:
1. Edit categories in the Transactions tab
2. Click **Save category changes**
3. Click **↓ Export ledger.csv**
4. Replace `derived/ledger.csv` with the downloaded file
5. Next time you run `pdf_to_ledger.py`, your edits are preserved

---

## Categorisation rules

The pipeline includes 35+ regex rules covering common SA merchants:

| Category | Examples |
|---|---|
| Groceries | Checkers, Pick n Pay, Woolworths Food, SPAR |
| Eating Out | Nando's, Steers, Ocean Basket, Uber Eats |
| Coffee | Bootlegger, Jason Bakery, Vida e Caffè |
| Petrol | Engen, BP, Shell, Total |
| Transport | Uber, Bolt, MyCiti, parking |
| Subscriptions | Netflix, Spotify, DStv, OpenAI, Adobe |
| Medical Aid | Discovery Health, Momentum, Bonitas |
| Insurance | OUTsurance, Sanlam, King Price |
| Investments | EasyEquities, unit trusts, ETFs |
| Travel | FlySafair, Airbnb, Booking.com, Avis |

Rules are applied top-to-bottom with first-match logic. The merchant map takes precedence over all rules.

---

## CSV format

If you want to use the dashboard with data from a different source, the required columns are:

| Column | Format | Example |
|---|---|---|
| `date` | MM/DD/YYYY | `3/15/2025` |
| `month` | MM/1/YYYY | `3/1/2025` |
| `amount` | Signed float | `-450.00` |
| `description` | Text | `Checkers Waterfront` |
| `category` | Text | `Groceries` |

Optional columns that improve the experience: `merchant`, `account_type` (credit / savings), `file`, `year`, `txn_id`.

---

## Tech stack

| Layer | Technology |
|---|---|
| PDF extraction | pdfplumber, pandas |
| Categorisation | Python regex |
| Dashboard | Vanilla HTML / CSS / JavaScript |
| Charts | Chart.js 4.4 |
| Data pipeline | Python 3, pandas |
| Hosting | Static — any web server or GitHub Pages |

No frameworks, no build step, no backend. The dashboard is a single file.

---

## Notes

- Currently tested against FNB Aspire credit card and FNB cheque/savings account statements. Other FNB account types may work but have not been verified.
- The dashboard uses `localStorage` to persist budgets and manual category overrides between sessions. Clearing browser data will reset these.
- Re-running the pipeline after adding new PDFs is safe — existing manual category edits in the ledger are preserved.

---

## License

MIT
