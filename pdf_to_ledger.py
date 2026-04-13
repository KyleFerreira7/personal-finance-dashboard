#!/usr/bin/env python3
"""
pdf_to_ledger.py

Run this script to process your FNB bank statement PDFs into a clean ledger CSV.
Supports both credit card and savings/cheque account statements.

Usage:
    python pdf_to_ledger.py

Place PDFs in ./Bank Statements/ — subfolders are fine.
Output is written to ./derived/ledger.csv and ./derived/merchant_map.csv
Re-running is safe: manual category edits in the existing ledger are preserved.
"""

from pathlib import Path
import datetime
import re
import sys

import pandas as pd

try:
    from pdf_parser import parse_pdf
except ImportError as e:
    print("ERROR: Could not import parse_pdf from pdf_parser.py:", e)
    sys.exit(1)

PROJECT_ROOT = Path(__file__).parent
DATA_DIR     = PROJECT_ROOT / "Bank Statements"
DERIVED_DIR  = PROJECT_ROOT / "derived"
DERIVED_DIR.mkdir(exist_ok=True)
LEDGER_PATH  = DERIVED_DIR / "ledger.csv"
MAP_PATH     = DERIVED_DIR / "merchant_map.csv"

# ── Today's date ceiling ───────────────────────────────────────────────────────
TODAY = pd.Timestamp.today().normalize()

# ── Categorisation rules ───────────────────────────────────────────────────────
# Rules applied top-to-bottom; first match wins.
# merchant_map.csv always takes precedence over these rules.
#
# IMPORTANT: Order matters! More specific patterns must come BEFORE broader ones.
# E.g. "uber eats" must precede "uber", "bolt food" must precede "bolt".

RULES = [
    # ── Income ──────────────────────────────────────────────────────────────────
    {"pattern": r"\b(salary|payroll|stipend|bonus)\b",               "category": "Salary"},
    {"pattern": r"(refund|rebate|cashback|reversal)",                 "category": "Other Income"},
    {"pattern": r"\binterest\s+(earned|received|credited)\b",        "category": "Other Income"},
    {"pattern": r"\bmagtape\s*credit\b",                              "category": "Other Income"},
    {"pattern": r"\bgeneral\s*credit\b",                              "category": "Other Income"},

    # ── Transfers (BEFORE banking so transfer keywords match first) ─────────────
    {"pattern": r"(internal\s*transfer|inter[-\s]*account|payment\s*to\s*self)",  "category": "Transfers"},
    {"pattern": r"(cc\s*payment|credit\s*card\s*payment)",                        "category": "Transfers"},
    {"pattern": r"(fnb\s*app\s*transfer\s*to\s*credit\s*card)",                   "category": "Transfers"},
    {"pattern": r"(fnb\s*app\s*transfer\s*to\b)",                                 "category": "Transfers"},
    {"pattern": r"(fnb\s*app\s*payment\s*to\b)",                                  "category": "Transfers"},
    {"pattern": r"\b1sa\s+credit\s+card\b",                                       "category": "Transfers"},
    {"pattern": r"\bonline\s*(?:banking\s*)?transfer\b",                           "category": "Transfers"},

    # ── Bank charges (BEFORE other patterns to catch FNB fee lines) ─────────────
    {"pattern": r"#\s*(int\s*pymt\s*fee|interest\s*payment\s*fee)",               "category": "Bank Charges"},
    {"pattern": r"#\s*(atm\s*transaction\s*fee|atm\s*fee)",                       "category": "Bank Charges"},
    {"pattern": r"#\s*(credit\s*card\s*account\s*fee|card\s*account\s*fee)",      "category": "Bank Charges"},
    {"pattern": r"#\s*(credit\s*facility\s*service\s*fee)",                       "category": "Bank Charges"},
    {"pattern": r"#\s*(declined\s*auth\s*fee|declined\s*authorisation\s*fee)",    "category": "Bank Charges"},
    {"pattern": r"#\s*(monthly\s*(?:account\s*)?fee|service\s*fee)",              "category": "Bank Charges"},
    {"pattern": r"#\s*(sms\s*fee|notification\s*fee|card\s*fee)",                 "category": "Bank Charges"},
    {"pattern": r"#\s*electronic\s*trf\s*fee",                                    "category": "Bank Charges"},
    {"pattern": r"#\s*pre[-\s]*paid\s*(airtime|electricity)\s*fee",               "category": "Bank Charges"},
    {"pattern": r"#\s*\w+.*fee",                                                  "category": "Bank Charges"},
    {"pattern": r"(bank\s*charges?|service\s*fee|monthly\s*fee|atm\s*fee)",       "category": "Bank Charges"},
    {"pattern": r"(int\s*pymt\s*fee|sms\s*fee|card\s*fee)",                       "category": "Bank Charges"},
    {"pattern": r"\bn\s*network\b",                                               "category": "Bank Charges"},
    {"pattern": r"^interest$",                                                    "category": "Interest Charged"},
    {"pattern": r"\binterest\s+(charged|debit|on\s*account)\b",                   "category": "Interest Charged"},

    # ── Insurance ────────────────────────────────────────────────────────────────
    {"pattern": r"\bfnb\s*life\b",                                                "category": "Insurance"},
    {"pattern": r"(outsurance|sanlam|old\s*mutual|momentum\s*(?!health)|discovery\s*insure)", "category": "Insurance"},
    {"pattern": r"(king\s*price|hollard|miway|1st\s*for\s*women|auto\s*&\s*general)",         "category": "Insurance"},

    # ── Savings & investments ────────────────────────────────────────────────────
    {"pattern": r"(tfsa|tax[-\s]*free\s*savings)",                   "category": "Tax-Free Savings"},
    {"pattern": r"(investment|unit\s*trust|etf\b|brokerage|easyequities)", "category": "Investments"},
    {"pattern": r"\b(pension|retirement)\b",                         "category": "Pension"},

    # ── Education ────────────────────────────────────────────────────────────────
    {"pattern": r"\bunisa\b",                                        "category": "Education"},
    {"pattern": r"\bstuvia\b",                                      "category": "Education"},
    {"pattern": r"\bteflacademy\b",                                  "category": "Education"},

    # ── Housing ──────────────────────────────────────────────────────────────────
    {"pattern": r"(rent\b|levies|body\s*corp|mortgage|bond\s*repay|home\s*loan)", "category": "Rent"},
    {"pattern": r"(electricity|prepaid\s*elec|eskom|city\s*power)",  "category": "Electricity"},
    # FIXED: removed "water" from this pattern — was false-matching "waterfront"
    {"pattern": r"(sanitation|rates\s*and\s*taxes|municipal)",       "category": "Rates & Utilities"},

    # ── Connectivity ─────────────────────────────────────────────────────────────
    {"pattern": r"(fibre|fiber|router|wi[-\s]*fi|afrihost|cool\s*ideas|vumatel|openserve|webafrica)", "category": "Internet"},
    {"pattern": r"(vodacom|mtn|cell\s*c|telkom\s*mobile|rain\s*mobile)", "category": "Phone"},
    {"pattern": r"\bprepaid\s*purchase\s*(?:airtime|data)\b",        "category": "Airtime"},
    {"pattern": r"(airtime|data\s*bundle|top[-\s]*up)",              "category": "Airtime"},

    # ── Groceries ────────────────────────────────────────────────────────────────
    {"pattern": r"(pick\s*n\s*pay|pn\s*p|checkers|spar\b|woolworths\s*food|shoprite|ok\s*foods|boxer|usave|food\s*lovers)", "category": "Groceries"},
    {"pattern": r"\baldi\b",                                         "category": "Groceries"},
    {"pattern": r"\bmakro\b",                                        "category": "Groceries"},

    # ── Eating out — YOCO specific merchants (BEFORE generic Yoco/restaurant) ───
    {"pattern": r"yoco\s*\*?\s*kari\s*eatery",                      "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*the\s*block\s*tak",                  "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*made\s*with\s*prid",                 "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*sassoli",                             "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*tree\s*of\s*life",                   "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*unlimited\s*stat",                   "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*kristens?\s*kick",                   "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*portuguese\s*cul",                   "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*lebanese\s*baker",                   "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*the\s*salene",                       "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*vredenheim",                         "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*secrets?\s*of\s*sum",                "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*four\s*and\s*twent",                 "category": "Eating Out"},

    # ── Eating out — YOCO coffee merchants (BEFORE generic Yoco) ────────────────
    {"pattern": r"yoco\s*\*?\s*gaga\s*bout\s*koff",                 "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*natyflexcoffee",                     "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*natix\s*coffee",                     "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*rosetta\s*roast",                    "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*origin\s*coffee",                    "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*deluxe\s*coffee",                    "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*truth\s*coffee",                     "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*tribe\s*coffee",                     "category": "Coffee"},

    # ── Eating out — YOCO sports merchants (BEFORE generic Yoco) ────────────────
    {"pattern": r"yoco\s*\*?\s*arturf",                              "category": "Sports"},
    {"pattern": r"yoco\s*\*?\s*fives\s*futbol",                     "category": "Sports"},
    {"pattern": r"yoco\s*\*?\s*welgemoed\s*pade",                   "category": "Sports"},

    # ── Eating out — YOCO travel ────────────────────────────────────────────────
    {"pattern": r"yoco\s*\*?\s*spier\s*resort",                     "category": "Travel"},

    # ── Eating out — YOCO generic keyword fallbacks ─────────────────────────────
    {"pattern": r"yoco\s*\*?\s*\w*\s*(eatery|kitchen|grill|food|deli|bistro|burger|pizza|sushi|thai|indian|chinese|mexican|tapas)", "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*\w*\s*(coffee|koff|roast|brew|cafe|café)",   "category": "Coffee"},
    {"pattern": r"yoco\s*\*?\s*\w*\s*(bar\b|pub\b|wine|beer|cocktail)",    "category": "Eating Out"},
    {"pattern": r"yoco\s*\*?\s*\w*\s*(salon|barber|hair|nail|beauty)",      "category": "Grooming"},
    {"pattern": r"yoco\s*\*?\s*\w*\s*(sport|padel|paddle|futbol|arena|golf|fitness)", "category": "Sports"},
    {"pattern": r"yoco\s*\*?\s*\w*\s*(escape|adventure|activity)",          "category": "Entertainment"},
    {"pattern": r"yoco\s*\*?\s*\w*\s*(flower|florist)",                     "category": "Shopping"},

    # ── Eating out & coffee (SPECIFIC food delivery BEFORE generic uber/bolt) ───
    {"pattern": r"(uber\s*eats|mr\s*d(?:elivery)?|bolt\s*food|orderin|e[-\s]*meal)", "category": "Eating Out"},
    {"pattern": r"(restaurant|ristorante|trattoria|osteria)",                "category": "Eating Out"},
    {"pattern": r"(bar\b|burger|pizza|sushi|take[-\s]*away|pub\b)",          "category": "Eating Out"},
    {"pattern": r"(bossa|cinnabon|fat\s*cactus|hudsons|jason\s*bakery|jarryds)", "category": "Eating Out"},
    {"pattern": r"(kauai|mcd|mcdonald|moksh|mugg\s*&?\s*bean|ocean\s*basket)", "category": "Eating Out"},
    {"pattern": r"(rocomamas|creamery|sweetbeet|tiger'?s?\s*milk|wimpy)",    "category": "Eating Out"},
    {"pattern": r"(nando'?s|steers|spur|kfc|debonairs|roman'?s?\s*pizza|fishaways)", "category": "Eating Out"},
    {"pattern": r"(burger\s*king|bk\s*kenilworth|bao\s*down|col'?\s*cacchio|la\s*toscana)", "category": "Eating Out"},
    {"pattern": r"(lavender\s*thai|fego\s*caffe|doppio\s*zero|panarottis)", "category": "Eating Out"},
    {"pattern": r"(wagamama|the\s*hussar|cattle\s*baron|cappuccinos)",       "category": "Eating Out"},
    {"pattern": r"(punjab|bwh\s*cape\s*gate|lifestyle\s*on\s*kloof|eataly)", "category": "Eating Out"},
    {"pattern": r"(john\s*dorys|roasted\s*and\s*raw|scheckters?\s*raw)",     "category": "Eating Out"},
    {"pattern": r"(famous\s*kalahari|kapstadt\s*brauha|fat\s*harry)",        "category": "Eating Out"},
    {"pattern": r"(chardonnay\s*deli|craft\s*wheat|giovanni\s*esposito)",    "category": "Eating Out"},
    {"pattern": r"(de\s*akker|montagu|jerrys|the\s*avenue)",                 "category": "Eating Out"},
    {"pattern": r"(grill|steakhouse|smokehouse|braai|brewery|brauhaus)",     "category": "Eating Out"},
    {"pattern": r"\bdeli\b",                                                  "category": "Eating Out"},

    # ── Coffee ──────────────────────────────────────────────────────────────────
    {"pattern": r"(bootlegger|seattle|starbucks|vida\s*e|the\s*grind)",      "category": "Coffee"},
    {"pattern": r"(caffe\s*da\s*corsa|selva\s*caf|\b3\s*at\s*1\b)",         "category": "Coffee"},
    {"pattern": r"(cafe|café|bakery|patisserie)",                             "category": "Coffee"},
    {"pattern": r"\bcoffee\b",                                                "category": "Coffee"},

    # ── Transport (AFTER food delivery to avoid uber eats → Transport) ──────────
    {"pattern": r"(engen|shell|bp\b|total\s*garage|sasol\s*garage|petrol|diesel|thunder\s*brothers)", "category": "Petrol"},
    {"pattern": r"(lynedoch\s*service\s*station|total\s*n1\s*city|doncaster\s*motors)", "category": "Petrol"},
    {"pattern": r"\b(service\s*station|garage|caltex|totalenergies)\b",      "category": "Petrol"},
    {"pattern": r"(uber(?!\s*eats)|bolt(?!\s*food)|indriver|myciti|gautrain|metered\s*taxi)", "category": "Transport"},
    {"pattern": r"(parking|servest|wilson\s*parking)",                       "category": "Transport"},
    {"pattern": r"(zapper|advance\s*(canal|va\s*wat|park)|montclare\s*place)", "category": "Transport"},
    {"pattern": r"(dott\s*scooter|huguenot\s*tunnel)",                       "category": "Transport"},
    {"pattern": r"\b(tunnel|toll)\b",                                         "category": "Transport"},

    # ── Health ───────────────────────────────────────────────────────────────────
    {"pattern": r"(clinic|doctor|gp\b|dentist|hospital|medical\s*aid|netcare|mediclinic|life\s*health)", "category": "Medical"},
    {"pattern": r"(drs?\s*smook|optometrist|physio)",                        "category": "Medical"},
    {"pattern": r"(discovery\s*health|bonitas|momentum\s*health|bestmed|fedhealth|gems\b)", "category": "Medical Aid"},
    {"pattern": r"(clicks|dis[-\s]*chem)",                                   "category": "Pharmacy"},
    {"pattern": r"(partners\s*hair|barber|salon|hair\s*cut|nail\b)",         "category": "Grooming"},
    {"pattern": r"\burban\s*men\b",                                          "category": "Grooming"},
    {"pattern": r"(gym|virgin\s*active|planet\s*fitness|crossfit|biokineticist)", "category": "Gym"},

    # ── Sports & hobbies ─────────────────────────────────────────────────────────
    {"pattern": r"(golf\b|padel|football|tennis|kayak|arturf|playtomic|futbol|sporting)", "category": "Sports"},
    {"pattern": r"(montague\s*arena|van\s*der\s*stel\s*sport)",              "category": "Sports"},
    {"pattern": r"^io\s+[a-f0-9]",                                           "category": "Sports"},

    # ── Shopping ────────────────────────────────────────────────────────────────
    {"pattern": r"(takealot|takealo|superbalist|amazon(?!\s*(?:web|prime|music))|zando|bash\s*store)", "category": "Shopping"},
    {"pattern": r"(paygate|payflex|snapscan\s*wallet|pyg)",                  "category": "Shopping"},
    {"pattern": r"(tyger\s*valley\s*centre|first\s*world\s*trader)",         "category": "Shopping"},
    {"pattern": r"(exclusive\s*books|typo\b|grassroo|camp\s*master)",        "category": "Shopping"},
    {"pattern": r"(builders\s*warehouse|v\s*&?\s*a\s*waterfront)",           "category": "Shopping"},

    # ── Clothing ────────────────────────────────────────────────────────────────
    {"pattern": r"\bnike\b",                                                  "category": "Clothing"},
    {"pattern": r"(adidas|crocs|freedom\s*of\s*movement|new\s*balance|old\s*khaki)", "category": "Clothing"},
    {"pattern": r"(puma|sportsmans?\s*warehouse|totalsports|smw\b)",         "category": "Clothing"},
    {"pattern": r"(zara|woolworths\s*(?!food)|h\s*&\s*m\b|cotton\s*on)",    "category": "Clothing"},
    {"pattern": r"(mr\s*price|truworths|edgars|ackermans|pep\b|poetry\b)",   "category": "Clothing"},
    {"pattern": r"\blovisa\b",                                                "category": "Clothing"},

    # ── Subscriptions (app stores + streaming) ──────────────────────────────────
    {"pattern": r"(apple\.com|apple\s*com|icloud|itunes|google\s*play|app\s*store)", "category": "Subscriptions"},
    {"pattern": r"(netflix|spotify|openai|chatgpt|youtube\s*premium|apple\s*tv)",    "category": "Subscriptions"},
    {"pattern": r"(showmax|dstv|microsoft\s*365|adobe|audible|kindle)",              "category": "Subscriptions"},
    {"pattern": r"(amazon\s*prime|disney\s*(?:plus|\+)|hbo|paramount)",              "category": "Subscriptions"},
    {"pattern": r"(playstation|psn\b|framer)",                                       "category": "Subscriptions"},

    # ── Entertainment ────────────────────────────────────────────────────────────
    {"pattern": r"(ster\s*kinekor|nu\s*metro|cinema|event\s*ticketing|computicket|webtickets)", "category": "Entertainment"},
    {"pattern": r"(escape\s*room|unlock\s*escape)",                          "category": "Entertainment"},

    # ── Travel ────────────────────────────────────────────────────────────────────
    {"pattern": r"(airbnb|booking\.com|booking\s*com|flight|airways|kulula|flysafair|saa\b)", "category": "Travel"},
    {"pattern": r"(hotel|hostel|guesthouse|car\s*hire|avis|budget\s*car|europcar)",           "category": "Travel"},
    {"pattern": r"(copia\s*eco|cabin|lodge|resort)",                                          "category": "Travel"},
    {"pattern": r"(acsa|www\.headout\.com|walkert)",                                          "category": "Travel"},
    {"pattern": r"(metro\s*barcelona|autolinee\s*toscane)",                                   "category": "Travel"},
    {"pattern": r"\b(amsterdam|barcelona|milano|milan|berlin|rome|roma|firenze|florence)\b",  "category": "Travel"},
    {"pattern": r"(ryanair|easyjet|vueling|flixbus|trenitalia|renfe|db\s*bahn)",              "category": "Travel"},

    # ── Alcohol ──────────────────────────────────────────────────────────────────
    {"pattern": r"(liquor|liquorshop|bottle\s*store|tops\b|wine\s*cellar)",  "category": "Alcohol"},
]


# ── Helpers ────────────────────────────────────────────────────────────────────


def normalize_merchant(s: str) -> str:
    """
    Normalize merchant name for consistent matching.
    FIXED: apply .lower() FIRST so casing never creates different normalized keys.
    """
    if not s:
        return ""
    s = str(s).lower().strip()
    # Remove trailing numeric IDs (transaction refs, terminal IDs)
    s = re.sub(r"\s*\d{2,}\b.*$", "", s)
    # Remove embedded amounts
    s = re.sub(r"\d{1,3}(?:[ ,]\d{3})*\.\d{2}\b", "", s)
    # Keep only alpha, digits, ampersand, hyphen, space
    s = re.sub(r"[^a-z0-9&\-\s]", " ", s)
    # Collapse whitespace
    return re.sub(r"\s+", " ", s).strip()



def make_txn_id(row) -> str:
    d = pd.to_datetime(row.get("date"), errors="coerce")
    dstr = d.strftime("%Y-%m-%d") if not pd.isna(d) else ""
    amt  = f"{float(row.get('amount', 0)):.2f}"
    desc = str(row.get("description") or "")[:80]
    src  = str(row.get("file") or "")
    return "|".join([dstr, amt, desc, src])



def apply_rules(df: pd.DataFrame) -> pd.Series:
    cats = pd.Series(index=df.index, dtype="object")
    text = (
        df.get("description", "").fillna("").astype(str) + " " +
        df.get("merchant",    "").fillna("").astype(str)
    ).str.lower()
    for rule in RULES:
        try:
            pat = re.compile(rule["pattern"], flags=re.I)
        except re.error:
            continue
        hit = text.str.contains(pat, na=False)
        cats.loc[hit & cats.isna()] = rule["category"]
    return cats



def load_map() -> dict:
    if not MAP_PATH.exists():
        return {}
    df = pd.read_csv(MAP_PATH)
    df.columns = [c.strip().lower() for c in df.columns]
    if not {"merchant", "category"}.issubset(df.columns):
        return {}
    df["merchant_norm"] = df["merchant"].astype(str).str.strip().str.lower()
    df = df.dropna(subset=["category"])
    df = df[df["category"].str.strip() != ""]
    # FIXED: Also normalize the merchant_norm column for consistent lookups
    df["merchant_norm"] = df["merchant_norm"].apply(normalize_merchant)
    # Deduplicate: keep last (most recent) entry per normalized merchant
    df = df.drop_duplicates(subset=["merchant_norm"], keep="last")
    return dict(zip(df["merchant_norm"], df["category"]))



def save_map(mmap: dict, df_combined: pd.DataFrame):
    existing = {}
    if MAP_PATH.exists():
        existing = load_map()

    snap = (
        df_combined[["merchant_norm", "merchant", "category"]]
        .dropna(subset=["merchant_norm"])
        .drop_duplicates(subset=["merchant_norm"], keep="last")
        .set_index("merchant_norm")
    )

    rows = []
    all_norms = set(snap.index) | set(mmap.keys()) | set(existing.keys())
    now = datetime.datetime.utcnow().isoformat(timespec="seconds")

    for norm in sorted(all_norms):
        cat      = existing.get(norm) or mmap.get(norm)
        if not cat and norm in snap.index:
            cat = snap.at[norm, "category"]
        merchant = snap.at[norm, "merchant"] if norm in snap.index else norm
        rows.append({"merchant": merchant, "merchant_norm": norm,
                     "category": cat or "Other", "updated_at": now})

    pd.DataFrame(rows).to_csv(MAP_PATH, index=False)
    print(f"  merchant_map.csv  → {len(rows)} merchants")



# ── Main pipeline ──────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("pdf_to_ledger  —  FNB statement processor")
    print("=" * 60)

    if not DATA_DIR.exists():
        print(f"\nERROR: Bank Statements folder not found:\n  {DATA_DIR}")
        sys.exit(1)

    pdf_files = sorted(DATA_DIR.rglob("*.pdf"))
    print(f"\nFound {len(pdf_files)} PDF(s)\n")
    if not pdf_files:
        print("No PDFs found. Nothing to do.")
        return

    # ── Parse PDFs ─────────────────────────────────────────────────────────────
    frames = []
    for p in pdf_files:
        try:
            df = parse_pdf(str(p))
            if df is None or df.empty:
                print(f"  [skip]  {p.name}  (no transactions found)")
                continue
            df["file"] = p.stem
            acct_type = df["account_type"].iloc[0] if "account_type" in df.columns else "unknown"
            print(f"  [ok]    {p.name}  ({len(df)} rows, {acct_type})")
            frames.append(df)
        except Exception as e:
            print(f"  [fail]  {p.name}  — {e}")

    if not frames:
        print("\nNo transactions parsed. Exiting.")
        return

    raw = pd.concat(frames, ignore_index=True)

    # ── Normalise dtypes ───────────────────────────────────────────────────────
    raw["date"]         = pd.to_datetime(raw["date"], errors="coerce")
    raw["amount"]       = pd.to_numeric(raw["amount"], errors="coerce")
    raw["description"]  = raw["description"].astype(str).str.strip()
    raw["merchant"]     = raw.get("merchant", raw["description"]).astype(str).str.strip()
    raw["account_type"] = raw.get("account_type", "unknown").astype(str)
    raw["merchant_norm"] = raw["merchant"].map(normalize_merchant)
    raw["txn_id"]        = raw.apply(make_txn_id, axis=1)

    before = len(raw)
    raw = raw.dropna(subset=["date", "amount"])
    raw = raw.drop_duplicates(subset=["txn_id"])

    # ── HARD CEILING: remove future-dated transactions ─────────────────────────
    future_mask = raw["date"] > TODAY
    n_future = future_mask.sum()
    if n_future > 0:
        print(f"\n⚠ Removed {n_future} future-dated transactions (after {TODAY.date()})")
        raw = raw[~future_mask]

    print(f"\nParsed {before} rows → {len(raw)} after dedup, cleaning & date filter")

    # ── Load existing ledger ───────────────────────────────────────────────────
    ledger_prev = pd.DataFrame()
    if LEDGER_PATH.exists():
        ledger_prev = pd.read_csv(LEDGER_PATH, parse_dates=["date"], low_memory=False)
        if "txn_id" not in ledger_prev.columns:
            ledger_prev["txn_id"] = ledger_prev.apply(make_txn_id, axis=1)
        # Also filter future dates from existing ledger
        ledger_prev["date"] = pd.to_datetime(ledger_prev["date"], errors="coerce")
        old_future = ledger_prev["date"] > TODAY
        if old_future.sum() > 0:
            print(f"  Also removing {old_future.sum()} future-dated rows from existing ledger")
            ledger_prev = ledger_prev[~old_future]
        print(f"Loaded existing ledger: {len(ledger_prev)} rows")

    # ── Categorise ────────────────────────────────────────────────────────────
    mmap = load_map()

    raw["category"] = raw["merchant_norm"].map(mmap)
    rule_cats = apply_rules(raw)
    raw["category"] = raw["category"].combine_first(rule_cats)

    if not ledger_prev.empty and "category" in ledger_prev.columns:
        hist = (
            ledger_prev[ledger_prev["category"].notna() & (ledger_prev["category"] != "Other")]
            .groupby(ledger_prev["merchant"].astype(str).str.strip().str.lower())["category"]
            .agg(lambda s: s.mode().iat[0] if not s.mode().empty else None)
        )
        raw["category"] = raw["category"].combine_first(raw["merchant_norm"].map(hist))

    raw["category"] = raw["category"].fillna("Other")

    # ── Merge & dedupe ────────────────────────────────────────────────────────
    combined = pd.concat([ledger_prev, raw], ignore_index=True, sort=False)
    if "txn_id" not in combined.columns:
        combined["txn_id"] = combined.apply(make_txn_id, axis=1)
    combined = combined.sort_values("date").drop_duplicates(subset=["txn_id"], keep="last").reset_index(drop=True)

    combined["date"]  = pd.to_datetime(combined["date"], errors="coerce")
    combined["month"] = combined["date"].dt.to_period("M").dt.to_timestamp()
    combined["year"]  = combined["date"].dt.year

    # ── Final future-date sweep (belt and suspenders) ──────────────────────────
    combined = combined[combined["date"] <= TODAY]

    # ── Save ──────────────────────────────────────────────────────────────────
    print(f"\nSaving to {DERIVED_DIR}/")
    out_cols = ["txn_id", "date", "month", "year", "file", "account_type",
                "description", "merchant", "category", "amount"]
    out_cols = [c for c in out_cols if c in combined.columns]
    combined[out_cols].to_csv(LEDGER_PATH, index=False)
    print(f"  ledger.csv        → {len(combined)} transactions")

    save_map(mmap, combined)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\nCategory summary:")
    summary = (
        combined.groupby("category")["amount"]
        .agg(["count", "sum"])
        .rename(columns={"count": "txns", "sum": "total"})
        .sort_values("total")
    )
    for cat, row in summary.iterrows():
        print(f"  {cat:<30} {row['txns']:>5} txns   R {row['total']:>10,.2f}")

    if "account_type" in combined.columns:
        print("\nBy account type:")
        for atype, grp in combined.groupby("account_type"):
            inc = grp[grp["amount"] > 0]["amount"].sum()
            spd = grp[grp["amount"] < 0]["amount"].sum()
            print(f"  {atype:<12}  income R {inc:>10,.2f}   spend R {abs(spd):>10,.2f}")

    # ── Transfer summary ──────────────────────────────────────────────────────
    transfers = combined[combined["category"] == "Transfers"]
    if not transfers.empty:
        print(f"\n⚠ {len(transfers)} transactions categorised as 'Transfers' (inter-account)")
        print("  These are excluded from spend/income analysis in the dashboard.")

    # ── Uncategorised summary ─────────────────────────────────────────────────
    other = combined[combined["category"] == "Other"]
    if not other.empty:
        print(f"\n⚠ {len(other)} transactions still categorised as 'Other':")
        top_other = other.groupby("merchant")["amount"].agg(["count", "sum"]).sort_values("count", ascending=False).head(15)
        for merch, row in top_other.iterrows():
            print(f"    {merch:<40} {row['count']:>3} txns   R {row['sum']:>10,.2f}")

    print("\nDone. Open dashboard.html and load derived/ledger.csv")


if __name__ == "__main__":
    main()
