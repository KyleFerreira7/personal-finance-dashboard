# pdf_parser.py
"""
Parses FNB statement PDFs — supports both:
  - Credit card statements  (single signed amount column, ends with Cr for credits)
  - Cheque / savings account statements (separate Debit / Credit columns)

Returns a DataFrame with columns:
  date, month, year, description, merchant, amount, account_type
  amount is always signed: positive = money in, negative = money out
"""
from __future__ import annotations
import re
import calendar
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import pdfplumber


# ── Month lookup ───────────────────────────────────────────────────────────────
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ── Filename date patterns ─────────────────────────────────────────────────────
YEAR_RE             = re.compile(r"(20\d{2})")
FNAME_YR_MON_NUM_RE = re.compile(r"(20\d{2})[ _\-\.]([01]\d)\b")
FNAME_MON_NAME_RE   = re.compile(r"\b([A-Za-z]{3,9})[ _\-\.]*(20\d{2})\b", re.I)


# ── In-PDF date / stamp patterns ───────────────────────────────────────────────
STMT_DATE_RE  = re.compile(r"Statement\s+Date\s+(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})", re.I)
SUP_STAMP_RE  = re.compile(r"\b(20\d{2})/([01]\d)/([0-3]\d)\s+SUP\b")


# ── Amount patterns ────────────────────────────────────────────────────────────
AMOUNT_TAIL_RE = re.compile(
    r"(?<!\d)(?P<amt>\d{1,3}(?:[ ,]\d{3})*\.\d{2})(?P<cr>\s*Cr)?\s*$", re.I
)

# ── THREE amounts at end of line = debit/credit/balance (savings account) ──────
THREE_AMOUNTS_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:[, ]\d{3})*\.\d{2})\s+"
    r"(\d{1,3}(?:[, ]\d{3})*\.\d{2})\s+"
    r"(\d{1,3}(?:[, ]\d{3})*\.\d{2})\s*$"
)

# Two bare amounts at end of line (savings account — could be debit+balance or credit+balance)
TWO_AMOUNTS_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:[, ]\d{3})*\.\d{2})\s+(\d{1,3}(?:[, ]\d{3})*\.\d{2})\s*$"
)
# Single bare amount somewhere at end of line (savings: only one column populated)
ONE_AMOUNT_END_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[, ]\d{3})*\.\d{2})\s*$")


# ── Header / skip patterns ─────────────────────────────────────────────────────
# Credit card header
CC_HEADER_RE = re.compile(
    r"\bTran\s*Date\b.*\bTransaction\s*Details\b.*\bStraight\s*Facility\b", re.I
)
# Savings/cheque account header variants
SAV_HEADER_RE = re.compile(
    r"\bDate\b.*\bDescription\b.*\b(Debit|Debits)\b.*\b(Credit|Credits)\b", re.I
)


SKIP_LINE_RE = re.compile(
    r"^(Opening\s+Balance|Closing\s+Balance|Card\s+Total|Items\s+marked|"
    r"#|Card\s+No\.|Page\s+\d+\s+of\s+\d+|First\s+National\s+Bank|"
    r"An\s+Authorised|Copy\s+Tax\s+Invoice|Foreign\s+Amount:|"
    r"Balance\s+Brought\s+Forward|Balance\s+Carried\s+Forward|"
    r"Totals?\s+for|Sub\s*[Tt]otal)",
    re.I,
)


FOOTER_GARBAGE_RE = re.compile(
    r"\bFNB\s+(ASPIRE|CHEQUE|SAVINGS|GOLD|PLATINUM|EASY)\b"
    r"|CSFZFN0:|PSFZFN|^\s*$"
    r"|\bAccount\s+Number\b|\bBranch\s+Code\b",
    re.I,
)



# ── Helpers ────────────────────────────────────────────────────────────────────


def _safe_ts(year: int, month: int, day: int) -> pd.Timestamp:
    last = calendar.monthrange(year, month)[1]
    return pd.Timestamp(year=year, month=month, day=min(day, last))



def _parse_month_name(s: str) -> Optional[int]:
    return MONTH_MAP.get(s.strip().lower()[:3])



def _extract_year_month_from_filename(path: str) -> Tuple[Optional[int], Optional[int]]:
    name = Path(path).stem
    m = FNAME_YR_MON_NUM_RE.search(name)
    if m:
        y, mth = int(m.group(1)), int(m.group(2))
        if 1 <= mth <= 12:
            return y, mth
    m = FNAME_MON_NAME_RE.search(name)
    if m:
        return int(m.group(2)), _parse_month_name(m.group(1))
    hits = YEAR_RE.findall(name)
    return (int(hits[-1]), None) if hits else (None, None)



def _extract_statement_meta(pages_text: List[str]) -> Tuple[Optional[int], Optional[int]]:
    for txt in pages_text[:2]:
        m = STMT_DATE_RE.search(txt)
        if m:
            return int(m.group(3)), _parse_month_name(m.group(2))
        m = SUP_STAMP_RE.search(txt)
        if m:
            return int(m.group(1)), int(m.group(2))
    return None, None



def _detect_account_type(pages_text: List[str]) -> str:
    """Return 'credit' or 'savings' based on header content."""
    combined = " ".join(pages_text[:3])
    if CC_HEADER_RE.search(combined):
        return "credit"
    if SAV_HEADER_RE.search(combined):
        return "savings"
    # Fallback: look for keywords in first page text
    first = pages_text[0] if pages_text else ""
    if re.search(r"\b(cheque|savings|current)\s+account\b", first, re.I):
        return "savings"
    return "credit"  # default — credit card parser is more forgiving



def _clean_lines(pdf: pdfplumber.PDF) -> List[str]:
    out = []
    for page in pdf.pages:
        txt = page.extract_text() or ""
        for ln in txt.splitlines():
            if not FOOTER_GARBAGE_RE.search(ln):
                out.append(ln)
    return out



# ── Credit card parser ─────────────────────────────────────────────────────────


def _parse_credit_rows(lines: List[str]) -> List[Tuple[str, str, str]]:
    """Returns list of (tran_date_str, description, amount_text)."""
    rows: List[Tuple[str, str, str]] = []
    in_table = False
    cur_date = cur_amount = ""
    cur_desc: List[str] = []
    date_re = re.compile(r"^([0-3]?\d)\s+([A-Za-z]{3})\b")


    def flush():
        nonlocal cur_date, cur_amount, cur_desc
        if cur_date and cur_amount:
            rows.append((cur_date, " ".join(p.strip() for p in cur_desc if p.strip()), cur_amount))
        cur_date = cur_amount = ""
        cur_desc = []


    for ln in lines:
        line = ln.strip()
        if not line:
            continue
        if CC_HEADER_RE.search(line):
            in_table = True; flush(); continue
        if SKIP_LINE_RE.search(line):
            flush(); continue
        mds = date_re.match(line)
        if mds:
            in_table = True
        if not in_table:
            continue
        if mds:
            flush()
            cur_date = f"{mds.group(1)} {mds.group(2)}"
            rest = line[mds.end():].rstrip()
            m_amt = AMOUNT_TAIL_RE.search(rest)
            if m_amt:
                cur_amount = rest[m_amt.start():m_amt.end()].strip()
                desc = rest[:m_amt.start()].rstrip()
                if desc:
                    cur_desc.append(desc)
                flush()
            else:
                if rest:
                    cur_desc.append(rest)
        else:
            m_amt = AMOUNT_TAIL_RE.search(line)
            if m_amt:
                cur_amount = line[m_amt.start():m_amt.end()].strip()
                cont = line[:m_amt.start()].rstrip()
                if cont:
                    cur_desc.append(cont)
                flush()
            else:
                cur_desc.append(line)


    flush()
    return rows



# ── Savings / cheque account parser ───────────────────────────────────────────


def _parse_amount_f(s: str) -> float:
    """Parse a single amount string like '1 234.56' or '10,000.00' to float."""
    return float(s.replace(" ", "").replace(",", ""))


def _parse_savings_rows(lines: List[str]) -> List[Tuple[str, str, float]]:
    """
    Returns list of (tran_date_str, description, signed_amount).
    FNB savings statements have columns: Date | Description | Debit | Credit | Balance

    FIXED: Now properly distinguishes debit/credit from balance by looking
    for THREE amounts (debit, credit, balance) or TWO amounts where one is
    debit/credit and the other is balance. Uses running balance tracking to
    disambiguate when only two amounts appear.
    """
    rows: List[Tuple[str, str, float]] = []
    in_table = False
    cur_date = ""
    cur_desc: List[str] = []
    cur_lines_raw: List[str] = []   # collect all raw lines for current txn
    date_re = re.compile(r"^(\d{1,2})\s+([A-Za-z]{3})\b|^(\d{4}[-/]\d{2}[-/]\d{2})\b")

    # Track running balance to help disambiguate amounts
    last_balance: Optional[float] = None

    def flush_sav():
        """Parse amounts from collected raw lines and flush."""
        nonlocal rows, last_balance
        if not cur_date:
            return

        desc = " ".join(p.strip() for p in cur_desc if p.strip())
        if not desc:
            return

        # Combine all raw lines to find amounts
        # The amounts are typically on the first or last raw line of the transaction
        # FNB format: Description ... Debit Credit Balance
        # Only one of Debit/Credit is populated per row, balance is always present

        amount = None

        for raw_line in reversed(cur_lines_raw):
            # Try three amounts: debit, credit, balance
            m3 = THREE_AMOUNTS_RE.search(raw_line)
            if m3:
                v1 = _parse_amount_f(m3.group(1))
                v2 = _parse_amount_f(m3.group(2))
                v3 = _parse_amount_f(m3.group(3))
                # v3 is balance; v1=debit, v2=credit
                # debit is expense (negative), credit is income (positive)
                if v2 > 0 and v1 == 0:
                    amount = v2       # credit (income)
                elif v1 > 0 and v2 == 0:
                    amount = -v1      # debit (expense)
                elif v1 > 0 and v2 > 0:
                    # Both populated (unusual) — net them
                    amount = v2 - v1
                last_balance = v3
                break

            # Try two amounts: could be (debit, balance) or (credit, balance)
            m2 = TWO_AMOUNTS_RE.search(raw_line)
            if m2:
                v1 = _parse_amount_f(m2.group(1))
                v2 = _parse_amount_f(m2.group(2))

                # v2 is likely the balance (last column). v1 is debit or credit.
                # Use running balance to figure out which:
                # If last_balance - v1 ≈ v2 → v1 is debit
                # If last_balance + v1 ≈ v2 → v1 is credit
                if last_balance is not None:
                    diff_debit  = abs((last_balance - v1) - v2)
                    diff_credit = abs((last_balance + v1) - v2)
                    if diff_debit < 0.02:
                        amount = -v1  # debit
                        last_balance = v2
                        break
                    elif diff_credit < 0.02:
                        amount = v1   # credit
                        last_balance = v2
                        break

                # Fallback: use keyword heuristic
                is_credit_kw = bool(re.search(
                    r"\b(salary|deposit|credit|interest\s+earned|transfer\s+from|"
                    r"payment\s+received|general\s+credit|reversal|refund)\b",
                    desc, re.I
                ))
                if is_credit_kw:
                    amount = v1
                else:
                    amount = -v1
                last_balance = v2
                break

            # Single amount — likely just the balance column (skip these,
            # they're usually continuation lines or balance-only rows)
            m1 = ONE_AMOUNT_END_RE.search(raw_line)
            if m1:
                val = _parse_amount_f(m1.group(1))
                # If we have a previous balance, try to infer
                if last_balance is not None:
                    diff = abs(val - last_balance)
                    if diff < 0.02:
                        # Same as last balance — this is just the balance, no txn
                        last_balance = val
                        return  # skip this "transaction"
                    # It could be that the single amount IS the balance after a txn
                    # We can't reliably tell debit vs credit without more context
                    # Use keyword heuristic
                    is_credit_kw = bool(re.search(
                        r"\b(salary|deposit|credit|interest\s+earned|transfer\s+from|"
                        r"payment\s+received|general\s+credit|reversal|refund)\b",
                        desc, re.I
                    ))
                    # Try to compute: if last_balance + X = val or last_balance - X = val
                    implied_credit = val - last_balance   # positive if credit
                    implied_debit  = last_balance - val    # positive if debit
                    if implied_credit > 0 and is_credit_kw:
                        amount = implied_credit
                    elif implied_debit > 0 and not is_credit_kw:
                        amount = -implied_debit
                    else:
                        # Ambiguous — use the keyword heuristic on the raw value
                        amount = val if is_credit_kw else -val
                    last_balance = val
                else:
                    # No previous balance — can't disambiguate
                    is_credit_kw = bool(re.search(
                        r"\b(salary|deposit|credit|interest\s+earned|transfer\s+from|"
                        r"payment\s+received|general\s+credit|reversal|refund)\b",
                        desc, re.I
                    ))
                    amount = val if is_credit_kw else -val
                    # Don't update last_balance since we're not sure this was a balance
                break

        if amount is not None and abs(amount) > 0.001:
            rows.append((cur_date, desc, amount))

    for ln in lines:
        line = ln.strip()
        if not line:
            continue
        if SAV_HEADER_RE.search(line):
            in_table = True
            flush_sav()
            cur_date = ""; cur_desc = []; cur_lines_raw = []
            continue
        if SKIP_LINE_RE.search(line):
            flush_sav()
            cur_date = ""; cur_desc = []; cur_lines_raw = []
            continue

        mds = date_re.match(line)
        if mds:
            in_table = True
            flush_sav()
            cur_desc = []; cur_lines_raw = [line]
            if mds.group(3):  # YYYY-MM-DD style
                cur_date = mds.group(3)
                rest = line[mds.end():].strip()
            else:
                cur_date = f"{mds.group(1)} {mds.group(2)}"
                rest = line[mds.end():].strip()
            if rest:
                cur_desc.append(rest)
        elif in_table:
            cur_lines_raw.append(line)
            cur_desc.append(line)

    flush_sav()
    return rows



# ── Table extraction fallback for savings accounts ────────────────────────────


def _parse_savings_via_tables(pdf: pdfplumber.PDF) -> List[Tuple[str, str, float]]:
    """
    Use pdfplumber's table extractor on each page.
    Looks for tables that have Date, Description, Debit, Credit columns.
    Properly reads Debit/Credit columns and ignores Balance column.
    """
    rows: List[Tuple[str, str, float]] = []

    for page in pdf.pages:
        tables = page.extract_tables() or []
        for table in tables:
            if not table or len(table) < 2:
                continue
            # Try to identify columns from first row
            header = [str(c or "").strip().lower() for c in table[0]]
            d_col  = next((i for i, h in enumerate(header) if "date" in h), None)
            de_col = next((i for i, h in enumerate(header) if "debit" in h), None)
            cr_col = next((i for i, h in enumerate(header) if "credit" in h), None)
            tx_col = next((i for i, h in enumerate(header) if "description" in h or "details" in h or "transaction" in h), None)
            # Identify and SKIP balance column
            bal_col = next((i for i, h in enumerate(header) if "balance" in h), None)

            if d_col is None or (de_col is None and cr_col is None):
                continue  # not a transaction table

            for row in table[1:]:
                if not row or len(row) <= max(c for c in [d_col, de_col, cr_col, tx_col] if c is not None):
                    continue
                date_s = str(row[d_col] or "").strip()
                if not date_s or not re.match(r"\d", date_s):
                    continue
                desc = str(row[tx_col] or "").strip() if tx_col is not None else ""
                debit  = _to_float(str(row[de_col] or "")) if de_col is not None else 0.0
                credit = _to_float(str(row[cr_col] or "")) if cr_col is not None else 0.0
                if debit == 0 and credit == 0:
                    continue
                amount = credit - debit  # positive = in, negative = out
                rows.append((date_s, desc, amount))
    return rows



def _to_float(s: str) -> float:
    s = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        return float(s)
    except ValueError:
        return 0.0



# ── Amount / description cleaners ─────────────────────────────────────────────


def _to_float_amount(amt_text: str) -> float:
    """Credit card: signed amount from text like '1 234.56Cr'."""
    s = amt_text.strip()
    is_credit = s.lower().endswith("cr")
    s = re.sub(r"[Cc][Rr]$", "", s).strip().replace(" ", "").replace(",", "")
    try:
        val = float(s)
    except ValueError:
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        val = float(m.group(1)) if m else 0.0
    return val if is_credit else -val



def _clean_description(desc: str) -> str:
    d = re.sub(r"\s+", " ", desc).strip()
    d = re.sub(r"\b\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\b.*", "", d).strip()
    d = re.sub(r"\bSUP\s+\d+\s+of\s+\d+\b.*", "", d, flags=re.I).strip()
    # Remove trailing balance figures like "10,592.57Cr" or "34,090.61"
    d = re.sub(r"\s+\d{1,3}(?:[, ]\d{3})*\.\d{2}(?:\s*Cr)?\s*$", "", d, flags=re.I).strip()
    return d



def _extract_merchant(desc: str) -> str:
    d = desc
    m = AMOUNT_TAIL_RE.search(d)
    if m:
        d = d[:m.start()].rstrip()
    d = re.sub(r"\s*\b\d{3}-\d{3}-\d{4}\b\s*$", "", d)
    m2 = re.search(r"(.*?)(\d{1,3}(?:[ ,]\d{3})*\.\d{2})\s*$", d)
    if m2:
        d = m2.group(1).rstrip()
    d = re.sub(r"(?:\s+\d{2,})+$", "", d).strip()
    return re.sub(r"\s+", " ", d).strip()



# ── Year assignment ────────────────────────────────────────────────────────────


def _parse_date_str(date_s: str) -> Tuple[Optional[int], Optional[int]]:
    """Return (day, month_num) from '15 Jan' or '2024-01-15' etc."""
    # YYYY-MM-DD
    m = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", date_s)
    if m:
        return int(m.group(3)), int(m.group(2))
    # DD Mon
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3})", date_s)
    if m:
        mon = _parse_month_name(m.group(2))
        return int(m.group(1)), mon
    return None, None



# ── Public API ─────────────────────────────────────────────────────────────────


def parse_pdf(file_or_path: str) -> pd.DataFrame:
    """
    Parse an FNB statement PDF (credit card OR savings/cheque account).
    Returns DataFrame: ['date','month','year','description','merchant','amount','account_type']
    amount: positive = money in, negative = money out
    """
    file_path = str(file_or_path)
    fname_year, fname_stmt_month = _extract_year_month_from_filename(file_path)

    with pdfplumber.open(file_path) as pdf:
        pages_text = [(p.extract_text() or "") for p in pdf.pages]
        stmt_year_pdf, stmt_month_pdf = _extract_statement_meta(pages_text)
        account_type = _detect_account_type(pages_text)

        stmt_year  = fname_year       or stmt_year_pdf
        stmt_month = fname_stmt_month or stmt_month_pdf

        if stmt_year is None:
            for txt in pages_text:
                mm = SUP_STAMP_RE.search(txt)
                if mm:
                    stmt_year = int(mm.group(1))
                    if stmt_month is None:
                        stmt_month = int(mm.group(2))
                    break

        lines = _clean_lines(pdf)

        # Parse rows depending on account type
        if account_type == "savings":
            # Try table extraction first (more reliable), fall back to line parser
            tbl_rows = _parse_savings_via_tables(pdf)
            raw_rows_sav = tbl_rows if tbl_rows else _parse_savings_rows(lines)
        else:
            raw_rows_sav = None

        raw_rows_cc = _parse_credit_rows(lines) if account_type == "credit" else []

    # ── Build records ──────────────────────────────────────────────────────────
    recs = []
    months_seen: List[int] = []

    if account_type == "credit":
        for tran_dt, desc, amt_text in raw_rows_cc:
            desc_clean = _clean_description(desc)
            merchant   = _extract_merchant(desc_clean)
            amount     = _to_float_amount(amt_text)
            day, mon   = _parse_date_str(tran_dt)
            if day is None or mon is None:
                continue
            months_seen.append(mon)
            recs.append({"day": day, "month_num": mon, "description": desc_clean,
                         "merchant": merchant, "amount": amount})
    else:
        for tran_dt, desc, amount in (raw_rows_sav or []):
            desc_clean = _clean_description(desc)
            merchant   = _extract_merchant(desc_clean)
            day, mon   = _parse_date_str(tran_dt)
            if day is None or mon is None:
                continue
            months_seen.append(mon)
            recs.append({"day": day, "month_num": mon, "description": desc_clean,
                         "merchant": merchant, "amount": amount})

    if not recs:
        return pd.DataFrame(columns=["date", "month", "year", "description",
                                     "merchant", "amount", "account_type"])

    df = pd.DataFrame.from_records(recs)

    # ── Assign year ────────────────────────────────────────────────────────────
    if stmt_month is None and months_seen:
        stmt_month = max(months_seen)
    base_year = stmt_year or pd.Timestamp.today().year

    def _assign_year(mon: int) -> int:
        if stmt_month is None:
            return base_year
        return base_year - 1 if mon > stmt_month else base_year

    df["year"] = df["month_num"].apply(_assign_year)
    df["date"] = [_safe_ts(y, m, d) for y, m, d in zip(df["year"], df["month_num"], df["day"])]
    df = df.drop(columns=["day", "month_num"])

    # ── HARD CEILING: drop any dates after today ───────────────────────────────
    today = pd.Timestamp.today().normalize()
    before_count = len(df)
    df = df[df["date"] <= today]
    dropped = before_count - len(df)
    if dropped > 0:
        print(f"  [date filter] Dropped {dropped} rows with future dates (after {today.date()})")

    try:
        df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    except Exception:
        df["month"] = df["date"].dt.to_period("M").dt.start_time

    df["account_type"] = account_type
    df = df[["date", "month", "year", "description", "merchant", "amount", "account_type"]]
    df = df.drop_duplicates(subset=["date", "description", "amount"])

    return df