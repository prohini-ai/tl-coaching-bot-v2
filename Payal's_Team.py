# ----------------------------
# TL Coaching Bot — Safe Startup Header
# ----------------------------

import io
import re
import hashlib
import random
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components  # optional
import urllib.parse as urlparse  # for Google Form prefill URLs

# ✅ Must be the first Streamlit command
st.set_page_config(page_title="Payal's Team", page_icon="🧭", layout="wide")

# 🧠 Optional: Google Analytics (GA4)
st.markdown("""
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-NRWJ132Y6X"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-NRWJ132Y6X');
</script>
""", unsafe_allow_html=True)

# 🪄 Optional: CSS tweak for button style (works with older Streamlit versions)
st.markdown("""
<style>
div.stButton > button {
    width: 100%;
    border-radius: 8px;
    font-weight: 600;
    background-color: #4F46E5;
    color: white;
}
.link-btn a {
    display: inline-block; text-decoration: none; padding: 0.5rem 0.75rem;
    border: 1px solid #e2e8f0; border-radius: 8px;
}
.summary-chip {
  display:inline-block; background:#fff; border:1px solid #e2e8f0;
  padding:.25rem .5rem; border-radius:999px; margin-right:.4rem; font-size:.85rem;
}
</style>
""", unsafe_allow_html=True)

# ----------------------------
# APP HEADER / TITLE
# ----------------------------
st.title("🧭 TL Coaching Bot – Beta Version")
st.caption("Your AI-powered assistant for faster, smarter coaching prep")

# ---------------------------- THEME -------------------------------------------
st.markdown("""
<style>
:root {
  --teal:#14B8A6; --coral:#FB7185; --lime:#84CC16;
  --slate:#334155; --ink:#0F172A; --bg:#F8FAFC; --muted:#E2E8F0;
}
html, body, .block-container { background-color: var(--bg); }
h1, h2, h3, h4 { color:var(--ink); }
.small { font-size:0.9rem; color:#475569; }
.card {
  background:white; border:1px solid var(--muted); border-radius:16px;
  padding:1rem 1.2rem; box-shadow:0 1px 2px rgba(0,0,0,0.04); margin-bottom:1rem;
}
.badge { display:inline-block; padding:3px 10px; border-radius:999px; font-size:0.8rem; }
.badge-teal { background:rgba(20,184,166,.12); color:var(--teal); border:1px solid rgba(20,184,166,.35); }
.badge-muted { background:#f1f5f9; color:#475569; border:1px solid #e2e8f0; }
.ribbon {
  background: linear-gradient(90deg, rgba(20,184,166,.12), rgba(132,204,22,.12));
  border:1px solid #dbeafe; padding:.8rem 1rem; border-radius:14px; margin:.4rem 1rem 1rem 0;
}
.section-title { border-left:4px solid var(--teal); padding-left:.5rem; }
.copybox { background:#0b1220; color:#e2e8f0; border-radius:12px; padding:12px; white-space:pre-wrap; }
.kpi { display:flex; gap:12px; flex-wrap:wrap; }
.kpi .chip { background:#fff; border:1px solid var(--muted); padding:.4rem .7rem; border-radius:12px; }
.mini-row { display:flex; gap:.5rem; align-items:center; flex-wrap:wrap; margin:.25rem 0 .5rem 0;}
</style>
""", unsafe_allow_html=True)

def show_df(df, height=None):
    try:
        st.dataframe(df, use_container_width=True, height=height)
    except TypeError:
        if height:
            st.dataframe(df, height=height)
        else:
            st.dataframe(df)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', (s or ''))

# ---------------------------- SAFE CSV/XLSX READER -----------------------------
def _read_tabular_upload(content_bytes: bytes, *, header="infer", dtype=None) -> pd.DataFrame:
    """Safe reader: UTF-16 (TSV), UTF-8-SIG, latin-1/cp1252 fallback, and XLSX (PK header)."""
    if not content_bytes:
        return pd.DataFrame()
    head = content_bytes[:4]

    # XLSX by ZIP magic
    if head.startswith(b'PK\x03\x04'):
        try:
            return pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl",
                                 header=None if header is None else 0)
        except Exception:
            try:
                return pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")
            except Exception:
                return pd.DataFrame()

    # UTF-16 BOM (often Excel Unicode Text TSV)
    if head.startswith(b'\xff\xfe') or head.startswith(b'\xfe\xff'):
        try:
            return pd.read_csv(io.BytesIO(content_bytes), encoding='utf-16',
                               sep='\t', engine='python', header=header, dtype=dtype)
        except Exception:
            try:
                return pd.read_csv(io.BytesIO(content_bytes), encoding='utf-16',
                                   engine='python', header=header, dtype=dtype)
            except Exception:
                return pd.DataFrame()

    # UTF-8 with BOM
    if head.startswith(b'\xef\xbb\xbf'):
        try:
            return pd.read_csv(io.BytesIO(content_bytes), encoding='utf-8-sig',
                               engine='python', header=header, dtype=dtype)
        except Exception:
            return pd.DataFrame()

    # No BOM: try utf-8 then fallbacks
    try:
        return pd.read_csv(io.BytesIO(content_bytes), engine='python',
                           header=header, dtype=dtype)
    except Exception:
        for enc in ('latin-1', 'cp1252', 'utf-16'):
            try:
                return pd.read_csv(io.BytesIO(content_bytes), encoding=enc,
                                   engine='python', header=header, dtype=dtype)
            except Exception:
                continue
        return pd.DataFrame()

# ---------------------------- Helpers: identity & dates ------------------------
DATE_RE_YMD = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

def norm_email(x: str) -> str:
    if pd.isna(x): return ""
    return str(x).strip().lower()

def coerce_float(x) -> Optional[float]:
    if pd.isna(x): return None
    s = str(x).strip().replace("%","")
    if s in ("", "NA", "None", "nan", "null", "-", "N/A"): return None
    try:
        return float(s)
    except Exception:
        try:
            return float(pd.to_numeric(s, errors="coerce"))
        except Exception:
            return None

def to_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())

def parse_any_date(obj) -> Optional[date]:
    if pd.isna(obj): return None
    s = str(obj).strip()
    if s == "": return None
    if DATE_RE_YMD.fullmatch(s):
        try: return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception: pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d",
                "%b %d, %Y", "%B %d, %Y"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: continue
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None

def fmt_mmddyyyy(d: date) -> str:
    return d.strftime("%m/%d/%Y")

def fmt_wb(d: date) -> str:
    return d.strftime("%m/%d")

def mondays_between(start_wb: date, end_wb: date) -> List[date]:
    start = to_monday(start_wb); end = to_monday(end_wb)
    if end < start: start, end = end, start
    out = []; cur = start
    while cur <= end:
        out.append(cur); cur += timedelta(days=7)
    return out

# ---------- Interval helpers (for accordion-based Input Date Range) ------------
def _month_last_day(y: int, m: int) -> date:
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)

def build_interval_options(interval: str, *, weeks_back: int = 32, months_back: int = 12) -> List[Tuple[str, Tuple[date, date]]]:
    """Return labeled (start,end) pairs for Weekly / Bi-weekly / Monthly."""
    today = date.today()
    options: List[Tuple[str, Tuple[date, date]]] = []

    if interval == "Weekly":
        mon = to_monday(today)
        vals = [mon - timedelta(weeks=i) for i in range(weeks_back)]
        for m in vals:
            lab = f"{fmt_mmddyyyy(m)}"
            options.append((lab, (m, m)))
        return options

    if interval == "Bi-weekly":
        mon = to_monday(today)
        vals = [mon - timedelta(weeks=2*i) for i in range(weeks_back//2)]
        for m in vals:
            start = m
            end = m + timedelta(days=7)  # span two Mondays
            lab = f"{fmt_mmddyyyy(start)} – {fmt_mmddyyyy(end)}"
            options.append((lab, (start, end)))
        return options

    # Monthly
    cur_y, cur_m = today.year, today.month
    for i in range(months_back):
        m = cur_m - i
        y = cur_y
        while m <= 0:
            m += 12
            y -= 1
        first = date(y, m, 1)
        last = _month_last_day(y, m)
        start = to_monday(first)
        end = to_monday(last)
        lab = f"{first.strftime('%b %Y')} ({fmt_mmddyyyy(start)} – {fmt_mmddyyyy(end)})"
        options.append((lab, (start, end)))
    return options

# ---------------------------- Metrics (Agent Dashboard) ------------------------
def load_metrics_agent_dashboard(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    """
    Revised to match your grid-style Agent Dashboard:
    - Two-row header: Row A (week dates), Row B (sublabels like TPH/Quality/Normalised TPH).
    - Id columns: Email / Agent.
    - Prefer 'Normalised TPH' if present; else 'TPH'.
    - Ignore non-agent rows (no '@' in email; rows like 'Select Mode', 'Select Date', 'TL').
    """
    if not content: 
        return pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])
    raw = _read_tabular_upload(content, header=None, dtype=str).fillna("")
    if raw.empty: 
        return pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])

    # Find header row (contains 'Email' and/or 'Agent')
    header_row = None
    for i in range(min(100, len(raw))):
        vals = [str(v).strip().lower() for v in raw.iloc[i].tolist()]
        if any(x in vals for x in ["email", "email address"]) and any(x in vals for x in ["agent","agent name"]):
            header_row = i
            break
    if header_row is None:
        # Fallback: use previous logic that looked for "Agent Name" or "Agent Email"
        for i in range(min(100, len(raw))):
            vals = [str(v).strip() for v in raw.iloc[i].tolist()]
            if "Agent Name" in vals or "Agent Email" in vals or "Email" in vals:
                header_row = i
                break
    if header_row is None:
        return pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])

    top = [str(c).strip() for c in raw.iloc[header_row].tolist()]            # week row (dates & 'Email'/'Agent')
    bot = [str(c).strip() for c in raw.iloc[header_row+1].tolist()]          # sublabels row
    data = raw.iloc[header_row+2:].reset_index(drop=True)                    # body

    # Locate identity columns (Email, Agent Name)
    # We search in BOTH header rows to be safe.
    def find_col(names: List[str]) -> Optional[int]:
        names_l = [n.lower() for n in names]
        for idx, (a, b) in enumerate(zip(top + [""]*(len(bot)-len(top)), bot + [""]*(len(top)-len(bot)))):
            a_l, b_l = str(a).strip().lower(), str(b).strip().lower()
            if a_l in names_l or b_l in names_l:
                return idx
        # second pass exacts on each row independently
        for idx, a in enumerate(top):
            if str(a).strip().lower() in names_l:
                return idx
        for idx, b in enumerate(bot):
            if str(b).strip().lower() in names_l:
                return idx
        return None

    c_mail = find_col(["email address","agent email","email"])
    c_name = find_col(["agent name","agent"])

    # Build week markers from the top row (forward fill date labels)
    dates_f, cur = [], None
    for v in top:
        v = str(v).strip()
        if DATE_RE.fullmatch(v):
            cur = v
        dates_f.append(cur)

    # Helper to find sublabel offsets (Normalised TPH / TPH / Quality)
    def find_offset(date_index: int, labels: List[str]) -> Optional[int]:
        base = date_index
        for off in range(0, 10):
            lab = bot[base + off] if base + off < len(bot) else ""
            if any(lbl.lower() == str(lab).strip().lower() for lbl in labels):
                return off
        return None

    groups = {}
    for j, dstr in enumerate(dates_f):
        if not dstr or not DATE_RE.fullmatch(dstr): 
            continue
        # Prefer Normalised TPH / Normalized TPH
        off_tph_norm = find_offset(j, ["Normalised TPH","Normalized TPH"])
        off_tph      = find_offset(j, ["TPH"])
        off_qa       = find_offset(j, ["Quality","Quality %","QA","QA %"])
        if off_tph_norm is None and off_tph is None and off_qa is None:
            continue
        groups.setdefault(dstr, {})
        if off_tph_norm is not None:
            groups[dstr]["TPH"] = j + off_tph_norm
        elif off_tph is not None:
            groups[dstr]["TPH"] = j + off_tph
        if off_qa is not None:
            groups[dstr]["Quality"] = j + off_qa

    wanted_strs = {wb.isoformat() for wb in wanted_wbs}
    rows = []
    for _, r in data.iterrows():
        # identity
        agent_name = str(r.iloc[c_name]).strip() if c_name is not None and c_name < len(r) else ""
        agent_mail = norm_email(r.iloc[c_mail]) if c_mail is not None and c_mail < len(r) else ""
        # filter out non-agent rows
        if not agent_mail or "@" not in agent_mail:
            continue
        # map per-week KPIs
        for dstr, cols in groups.items():
            if dstr not in wanted_strs:
                continue
            wb = datetime.strptime(dstr, "%Y-%m-%d").date()
            tph = coerce_float(r.iloc[cols.get("TPH")]) if "TPH" in cols and cols.get("TPH") < len(r) else None
            qa  = coerce_float(r.iloc[cols.get("Quality")]) if "Quality" in cols and cols.get("Quality") < len(r) else None
            rows.append({
                "agent_email": agent_mail or "",
                "agent_name": agent_name or "",
                "wb": wb,
                "TPH": tph if tph is not None else 0.0,
                "Quality": qa if qa is not None else None
            })
    df = pd.DataFrame(rows)
    if df.empty: 
        return df
    df["agent_email"] = df["agent_email"].apply(norm_email)
    return df

def load_metrics_fallback_flat(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    if not content: return pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])
    df0 = _read_tabular_upload(content)
    if df0.empty: return pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])

    cols_l = [c.strip().lower() for c in df0.columns]
    m = dict(zip(cols_l, df0.columns))
    def pick(names):
        for n in names:
            if n.lower() in m: return m[n.lower()]
        return None
    c_agent = pick(["agent email","agent_email","email address","email","agent","agent name","name"])
    c_week  = pick(["week beginning","week","week_start","wb","date","timestamp"])
    # Prefer Normalised TPH if present
    c_tph   = pick(["normalised tph","normalized tph","tph","jph","jobs per hour","tickets per hour"])
    c_qa    = pick(["quality","quality %","qa","qa %","accuracy"])
    if c_agent is None or c_week is None or c_tph is None:
        return pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])

    df = pd.DataFrame({
        "agent_email": df0[c_agent].apply(norm_email),
        "wb": df0[c_week].apply(parse_any_date),
        "TPH": df0[c_tph].apply(coerce_float),
        "Quality": df0[c_qa].apply(coerce_float) if c_qa else None,
    })
    df = df[df["wb"].notna()].copy()
    df["wb"] = df["wb"].apply(to_monday)
    df = df[df["wb"].isin(wanted_wbs)]
    # name if available
    name_col = pick(["agent name","agent","name"])
    df["agent_name"] = df0[name_col] if name_col else ""
    return df

def load_metrics(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    # Keep structure the same, but prefer the grid parser for your sheet; fall back to flat.
    df = load_metrics_agent_dashboard(content, wanted_wbs)
    if df.empty:
        df = load_metrics_fallback_flat(content, wanted_wbs)
    return df

# ---------------------------- QA audits (keep detailed rows) -------------------
def load_qa(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    """
    Enhanced for CTL dashboard:
    - Email: F 'Email Address' (preferred) or E 'Agent Name/ Email' (fallback) or standard aliases.
    - WB: D 'WB' (preferred); fallbacks: Week Beginning, week, Timestamp, Audit Date, date.
    - JIRA: H 'QA audit ticket link' in addition to existing aliases.
    - Final: K 'QA-LOB Score(Design+delivery)' (strip %).
    - Notes: L and M concatenated (skip blanks).
    - Optional passthrough: R 'TL Email ID' (not used elsewhere but kept if present).
    """
    if not content:
        return pd.DataFrame(columns=["agent_email","wb","jira","notes","final"])
    df0 = _read_tabular_upload(content)
    if df0.empty:
        return pd.DataFrame(columns=["agent_email","wb","jira","notes","final"])

    cols_l = {str(c).strip().lower(): c for c in df0.columns}

    def get(*cands):
        for c in cands:
            key = str(c).strip().lower()
            if key in cols_l:
                return cols_l[key]
        return None

    # CTL-specific & general aliases
    c_mail  = get("email address","agent email","agent name/ email","agent name/email","email")
    c_wb    = get("wb","week beginning","week","week_start","timestamp","audit date","date")
    c_jira  = get("qa audit ticket link","jira link:","jira link","jira")
    c_final = get("qa-lob score(design+delivery)","final score","qa %","quality","quality %")
    c_notes1 = get("audit notes","notes","markdown","qa markdown","qa markdown 1","markdown 1")
    c_notes2 = get("audit notes 2","notes 2","markdown 2","qa markdown 2")

    # If CTL columns L/M aren't named, try positional (L=11,M=12 zero-based) safely
    if c_notes1 is None and df0.shape[1] > 11:
        c_notes1 = df0.columns[11]
    if c_notes2 is None and df0.shape[1] > 12:
        c_notes2 = df0.columns[12]

    # Optional passthrough (not used but preserved if needed later)
    c_tl = get("tl email id")

    if c_mail is None or c_wb is None:
        return pd.DataFrame(columns=["agent_email","wb","jira","notes","final"])

    wb = df0[c_wb].apply(parse_any_date).apply(lambda d: to_monday(d) if d else None)

    # Build notes by combining L and M (skip blanks)
    def combine_notes(row):
        parts = []
        if c_notes1 is not None:
            v = str(row.get(c_notes1, "")).strip()
            if v: parts.append(v)
        if c_notes2 is not None:
            v = str(row.get(c_notes2, "")).strip()
            if v: parts.append(v)
        return "; ".join(parts)

    out = pd.DataFrame({
        "agent_email": df0[c_mail].apply(norm_email),
        "wb": wb,
        "jira": df0[c_jira] if c_jira else "",
        "notes": df0.apply(lambda r: combine_notes(r), axis=1),
        "final": df0[c_final].apply(coerce_float) if c_final else None
    })
    if c_tl and c_tl in df0.columns:
        out["tl_email"] = df0[c_tl].astype(str)

    out = out[out["wb"].notna()].copy()
    out = out[out["wb"].isin(wanted_wbs)]
    return out  # row-level table

# ---------------------------- WFM loaders -------------------------------------
def load_adherence(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    if not content:
        return pd.DataFrame(columns=["agent_email","wb","adherence_ytd"])
    df0 = _read_tabular_upload(content)
    if df0.empty:
        return pd.DataFrame(columns=["agent_email","wb","adherence_ytd"])
    cols_l = {str(c).lower(): c for c in df0.columns}
    def g(*xs):
        for x in xs:
            if x.lower() in cols_l: return cols_l[x.lower()]
        return None
    c_mail = g("employee_id","agent_email","email","agent email","email address","employee","employee ")
    c_time = g("time interval","date","wb","week beginning","week")
    c_pct  = g("adherence total % - ytd","adherence_ytd","adherence total %")
    if c_mail is None or c_time is None or c_pct is None:
        return pd.DataFrame(columns=["agent_email","wb","adherence_ytd"])
    df = pd.DataFrame({
        "agent_email": df0[c_mail].apply(norm_email),
        "wb": df0[c_time].apply(parse_any_date),
        "adherence_ytd": df0[c_pct].apply(coerce_float)
    })
    df = df[df["wb"].notna()].copy()
    df["wb"] = df["wb"].apply(to_monday)
    df = df[df["wb"].isin(wanted_wbs)]
    df = df.sort_values(["agent_email","wb"])
    agg = (df.groupby(["agent_email","wb"])
             .agg(adherence_ytd=("adherence_ytd",
                  lambda s: next((x for x in reversed(list(s)) if pd.notna(x)), s.mean())))
             .reset_index())
    return agg

def load_shrinkage(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    if not content:
        return pd.DataFrame(columns=["agent_email","wb","shrinkage_pct","unaccounted_time"])
    df0 = _read_tabular_upload(content)
    if df0.empty:
        return pd.DataFrame(columns=["agent_email","wb","shrinkage_pct","unaccounted_time"])
    cols_l = {str(c).lower(): c for c in df0.columns}
    def g(*xs):
        for x in xs:
            if x.lower() in cols_l: return cols_l[x.lower()]
        return None
    c_mail = g("employee_id","agent_email","email","email address")
    c_time = g("time interval","date","wb","week beginning","week")
    c_shr  = g("shrinkage %","shrinkage")
    c_unac = g("unaccounted time value","unaccounted")
    if c_mail is None or c_time is None:
        return pd.DataFrame(columns=["agent_email","wb","shrinkage_pct","unaccounted_time"])
    df = pd.DataFrame({
        "agent_email": df0[c_mail].apply(norm_email),
        "wb": df0[c_time].apply(parse_any_date),
        "shrinkage_pct": df0[c_shr].apply(coerce_float) if c_shr else None,
        "unaccounted_time": df0[c_unac].apply(coerce_float) if c_unac else 0.0
    })
    df = df[df["wb"].notna()].copy()
    df["wb"] = df["wb"].apply(to_monday)
    df = df[df["wb"].isin(wanted_wbs)]
    agg = (df.groupby(["agent_email","wb"])
             .agg(shrinkage_pct=("shrinkage_pct","mean"),
                  unaccounted_time=("unaccounted_time","sum"))
             .reset_index())
    return agg

def load_conformance(content: bytes, wanted_wbs: List[date]) -> pd.DataFrame:
    if not content:
        return pd.DataFrame(columns=["agent_email","wb","conformance_pct"])
    df0 = _read_tabular_upload(content)
    if df0.empty:
        return pd.DataFrame(columns=["agent_email","wb","conformance_pct"])
    cols_l = {str(c).lower(): c for c in df0.columns}
    def g(*xs):
        for x in xs:
            if x.lower() in cols_l: return cols_l[x.lower()]
        return None
    c_mail = g("employee_id","agent_email","email","employee","employee ","email address")
    c_time = g("time interval","date","wb","week beginning","week")
    c_act  = g("conformance activity","activity")
    c_dyn  = g("[0] conformance % - dynamic","conformance % - dynamic","conformance %")
    if c_mail is None or c_time is None or c_dyn is None:
        return pd.DataFrame(columns=["agent_email","wb","conformance_pct"])
    df = pd.DataFrame({
        "agent_email": df0[c_mail].apply(lambda s: norm_email(str(s).strip())),
        "wb": df0[c_time].apply(parse_any_date),
        "activity": df0[c_act].astype(str).str.strip() if c_act else "",
        "conformance_pct": df0[c_dyn].apply(coerce_float)
    })
    df = df[df["wb"].notna()].copy()
    df["wb"] = df["wb"].apply(to_monday)
    df = df[df["wb"].isin(wanted_wbs)]
    if "activity" in df.columns and (df["activity"] == "Overall").any():
        df = df[df["activity"] == "Overall"]
    agg = (df.groupby(["agent_email","wb"])
             .agg(conformance_pct=("conformance_pct","mean"))
             .reset_index())
    return agg

# ---------------------------- Risk & Priority ----------------------------------
def build_flags_and_priority(row, tph_target, qa_target, w_tph, w_qa):
    flags = []
    if "TPH" in row and pd.notna(row["TPH"]) and row["TPH"] < tph_target:
        flags.append("TPH below target")
    if "Quality" in row and pd.notna(row["Quality"]) and row["Quality"] < qa_target:
        flags.append("Quality below target")
    if "adherence_ytd" in row and pd.notna(row["adherence_ytd"]) and row["adherence_ytd"] < 90:
        flags.append("Adherence < 90%")
    if "conformance_pct" in row and pd.notna(row["conformance_pct"]) and row["conformance_pct"] < 100:
        flags.append("Conformance < 100%")
    if "shrinkage_pct" in row and pd.notna(row["shrinkage_pct"]) and row["shrinkage_pct"] > 19:
        flags.append("Shrinkage > 19%")
    if "unaccounted_time" in row and pd.notna(row["unaccounted_time"]) and row["unaccounted_time"] > 0:
        flags.append("Unaccounted Time > 0")

    priority = 0.0
    if "TPH" in row and pd.notna(row["TPH"]) and row["TPH"] < tph_target and tph_target > 0:
        priority += w_tph * (tph_target - row["TPH"]) / tph_target
    if "Quality" in row and pd.notna(row["Quality"]) and row["Quality"] < qa_target and qa_target > 0:
        priority += w_qa * (qa_target - row["Quality"]) / qa_target
    return flags, round(priority, 3)

# ---------------------------- Variation Engine ---------------------------------
VARIANTS = {
    "Supportive": {
        "ww_qa_met": [
            "Maintained {qa:.2f}% QA — strong attention to accuracy.",
            "QA on target (≥{qa_target}%) — great consistency.",
            "Quality stayed high at {qa:.2f}%; keep the pre-submit checks."
        ],
        "ww_tph_met": [
            "TPH at {tph:.2f} meets the target.",
            "Throughput on track ({tph:.2f} ≥ {tph_target}).",
            "Good pace this week — TPH landed at {tph:.2f}."
        ],
        "ww_wfm_ok": [
            "WFM within acceptable range for most metrics.",
            "No critical WFM concerns observed.",
            "Overall schedule adherence looked stable."
        ],
        "wb_tph_miss": [
            "TPH at {tph:.2f} is below the {tph_target} target.",
            "Throughput dipped; aim to close the {gap:.2f} gap to target.",
            "Pacing slipped vs prior ({prev_tph:.2f} → {tph:.2f}); structured focus blocks recommended."
        ],
        "wb_qa_miss": [
            "Quality at {qa:.2f}% is below the {qa_target}% target.",
            "QA dipped to {qa:.2f}%, shy of the {qa_target}% goal.",
            "Quality misses noted this week ({qa:.2f}% < {qa_target}%)."
        ],
        "wb_missing": [
            "{label}: No Data Found."
        ],
        "wf_quality": [
            "Keep QA ≥{qa_target}% with consistent self-review.",
            "Sustain high QA (≥{qa_target}%) using a quick checklist before submits.",
            "Maintain quality discipline — target ≥{qa_target}%."
        ],
        "wf_tph": [
            "Lift TPH with two 30-min focus sprints daily; reduce context switches.",
            "Track hourly solves to spot slow periods and batch similar tasks.",
            "Increase throughput by streamlining repetitive steps and keyboard shortcuts."
        ],
        "wf_projects": [
            "Contribute 1 insight or project task this cycle.",
            "Share one best practice with peers and log it as an insight.",
            "Participate in ongoing projects to broaden impact."
        ],
        "wf_followup": [
            "Review progress in the next 1:1 and adjust the plan.",
            "Post weekly progress notes before the next coaching.",
            "We’ll revisit outcomes in the next 1:1."
        ],
        "opt_titles": [
            "📈 JPH (TPH)", "📈 Throughput (TPH)", "📈 Productivity (TPH)"
        ],
        "opt_titles_qa": [
            "✅ QA", "✅ Quality", "✅ Accuracy"
        ],
    },
    "Direct": {
        "ww_qa_met": [
            "QA met: {qa:.2f}% (≥{qa_target}%).",
            "Quality on target at {qa:.2f}%.",
            "QA stable at {qa:.2f}%."
        ],
        "ww_tph_met": [
            "TPH met: {tph:.2f} (≥{tph_target}).",
            "Throughput at {tph:.2f}, meets target.",
            "TPH compliant at {tph:.2f}."
        ],
        "ww_wfm_ok": [
            "WFM metrics within range.",
            "No major WFM exceptions.",
            "WFM steady."
        ],
        "wb_tph_miss": [
            "TPH {tph:.2f} < target {tph_target}.",
            "Close TPH gap of {gap:.2f}.",
            "TPH down from {prev_tph:.2f} to {tph:.2f}."
        ],
        "wb_qa_miss": [
            "QA {qa:.2f}% < target {qa_target}%.",
            "Raise QA to at least {qa_target}%.",
            "QA below standard this week."
        ],
        "wb_missing": ["{label}: No Data Found."],
        "wf_quality": [
            "Maintain QA ≥{qa_target}% with strict checklist.",
            "Hold QA ≥{qa_target}% via pre-submit validation.",
            "Keep QA on or above {qa_target}%."
        ],
        "wf_tph": [
            "Add two 30-min deep-work blocks; minimize context switching.",
            "Measure hourly solves; focus on the slowest hour.",
            "Batch similar tasks and use hotkeys to increase speed."
        ],
        "wf_projects": [
            "Deliver one insight or project task this cycle.",
            "Document and share one process improvement.",
            "Support an in-flight project this month."
        ],
        "wf_followup": [
            "Report progress in the next 1:1.",
            "Share weekly status before the coaching.",
            "We’ll review results in the next session."
        ],
        "opt_titles": ["📈 TPH", "📈 Throughput", "📈 Output"],
        "opt_titles_qa": ["✅ QA", "✅ Quality"],
    },
    "Celebratory": {
        "ww_qa_met": [
            "Fantastic — {qa:.2f}% QA keeps the bar high! 🎯",
            "Quality shining at {qa:.2f}% — love the consistency!",
            "QA at {qa:.2f}% — great craft!"
        ],
        "ww_tph_met": [
            "Nice pace — TPH hit {tph:.2f}! 🚀",
            "Throughput landed at {tph:.2f}; target met!",
            "Solid momentum with TPH {tph:.2f}!"
        ],
        "ww_wfm_ok": [
            "WFM looked solid overall.",
            "Scheduling hygiene on point — no big flags.",
            "WFM green for most checks."
        ],
        "wb_tph_miss": [
            "Let’s lift TPH from {tph:.2f} to {tph_target}+ — you’ve got this!",
            "Close a {gap:.2f} gap to target — small tweaks can do it!",
            "TPH slid ({prev_tph:.2f} → {tph:.2f}); we’ll rebound with focused sprints."
        ],
        "wb_qa_miss": [
            "Aim QA to {qa_target}%+; current is {qa:.2f}%.",
            "Quality dipped to {qa:.2f}% — we’ll tighten the checklist.",
            "QA below bar ({qa:.2f}% < {qa_target}%)."
        ],
        "wb_missing": ["{label}: No Data Found."],
        "wf_quality": [
            "Keep the quality streak — ≥{qa_target}% each week.",
            "Stay crisp on quality (≥{qa_target}%); checklist wins!",
            "Hold quality line at ≥{qa_target}%."
        ],
        "wf_tph": [
            "Two 30-min power sprints daily; batch & flow!",
            "Track hourly rhythm and smooth the dips.",
            "Speed up with macros/shortcuts on repeat steps."
        ],
        "wf_projects": [
            "Add one insight or project contribution this cycle.",
            "Share a tip with the squad as an insight.",
            "Jump into a project thread to amplify impact."
        ],
        "wf_followup": [
            "Swap notes in the next 1:1 and celebrate wins.",
            "Post weekly progress — we’ll iterate together.",
            "Next check-in: review actions and keep momentum."
        ],
        "opt_titles": ["📈 JPH (TPH)", "📈 Throughput Spark", "📈 Productivity Pulse"],
        "opt_titles_qa": ["✅ QA", "✅ Quality Glow"],
    }
}

def _seed_rng(agent_email: str, wb1: date, wb2: date, tone: str) -> random.Random:
    key = f"{agent_email}|{wb1.isoformat()}|{wb2.isoformat()}|{tone}"
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    st.session_state["variant_seed_preview"] = h[:8]
    return random.Random(int(h[:16], 16))

def _pick(rng: random.Random, items: List[str], variety: int) -> str:
    """Pick a variant respecting 'variety' (1=low, 2=med, 3=high)."""
    if not items: return ""
    pool = items[: max(1, min(len(items), {1: 2, 2: 4, 3: len(items)}.get(variety, len(items))))]
    return rng.choice(pool)

# ---------------------------- Note Builder -------------------------------------
def build_template_note(agent_name: str,
                        agent_email: str,
                        wbs: List[date],
                        metrics_rows: pd.DataFrame,
                        qa_rows_full: pd.DataFrame,
                        wfm_rows: pd.DataFrame,
                        tph_target: float,
                        qa_target: float,
                        tone: str = "Supportive",
                        variety: int = 2) -> str:
    week1 = min(wbs); week2 = max(wbs)

    def fmt_num(x):
        return int(x) if isinstance(x,(int,float)) and float(x).is_integer() else x

    rng = _seed_rng(agent_email, week1, week2, tone)
    V = VARIANTS.get(tone, VARIANTS["Supportive"])

    m_map = {r["wb"]: r for _, r in metrics_rows.iterrows()} if not metrics_rows.empty else {}
    w_map = {r["wb"]: r for _, r in wfm_rows.iterrows()} if not wfm_rows.empty else {}

    curr = m_map.get(week2, {})
    prev = m_map.get(week1, {}) if week1 != week2 else {}

    tph_curr = curr.get("TPH"); tph_prev = prev.get("TPH")
    qa_curr  = curr.get("Quality")

    a_curr = w_map.get(week2, {}).get("adherence_ytd")
    c_curr = w_map.get(week2, {}).get("conformance_pct")
    s_curr = w_map.get(week2, {}).get("shrinkage_pct")
    u_curr = w_map.get(week2, {}).get("unaccounted_time")

    # ---------- 1) GOAL ----------
    section1 = [
        "1. GOAL: What do you want to achieve?\n",
        f"({fmt_mmddyyyy(week1)} – {fmt_mmddyyyy(week2)})",
        f"TPH: target ≥{fmt_num(tph_target)}.",
        f"Quality: Target (≥{fmt_num(qa_target)}%).",
        "Project/Insights: No Specific Target",
    ]

    # ---------- 2) REALITY ----------
    ww = []
    if qa_curr is None or pd.isna(qa_curr):
        ww.append(V["wb_missing"][0].format(label="Quality"))
    elif qa_curr >= qa_target:
        ww.append(_pick(rng, V["ww_qa_met"], variety).format(qa=qa_curr, qa_target=fmt_num(qa_target)))

    if tph_curr is None or pd.isna(tph_curr):
        ww.append(V["wb_missing"][0].format(label="TPH"))
    elif tph_curr >= tph_target:
        ww.append(_pick(rng, V["ww_tph_met"], variety).format(tph=tph_curr, tph_target=fmt_num(tph_target)))

    if a_curr is None and c_curr is None and s_curr is None and u_curr is None:
        ww.append("WFM: No Data Found.")
    else:
        ww.append(_pick(rng, V["ww_wfm_ok"], variety))
    ww.append("Strong consistency in performance behavior and reliability during the week.")

    wb_lines = []
    if tph_curr is None or pd.isna(tph_curr):
        wb_lines.append(V["wb_missing"][0].format(label="TPH"))
    elif tph_curr < tph_target:
        gap = float(tph_target) - float(tph_curr)
        wb_lines.append(_pick(rng, V["wb_tph_miss"], variety).format(
            tph=tph_curr, tph_target=fmt_num(tph_target), gap=gap, prev_tph=(tph_prev if tph_prev is not None and not pd.isna(tph_prev) else 0.0)
        ))
        wb_lines.append("Needs to focus on improving throughput without compromising QA.")

    if qa_curr is None or pd.isna(qa_curr):
        wb_lines.append(V["wb_missing"][0].format(label="Quality"))
    elif qa_curr < qa_target:
        wb_lines.append(_pick(rng, V["wb_qa_miss"], variety).format(
            qa=qa_curr, qa_target=fmt_num(qa_target)
        ))

    wb_lines.append("No project or insight submissions this cycle — can explore contributing to ongoing initiatives.")

    section2 = [
        "\n2. REALITY: What went well? What could’ve done better?\n",
        f"({fmt_mmddyyyy(week1)} – {fmt_mmddyyyy(week2)})\n",
        "\nWhat went well:\n",
        *[f"\n{line}" for line in ww],
        "\n\nWhat could’ve been better:\n",
        *[f"\n{line}" for line in wb_lines],
    ]

    # ---------- 3) OPTIONS ----------
    options_intro = [
        "\n3. OPTIONS: What are the plans to address the gaps?\n",
        f"\n🧑 Agent: {agent_name or agent_email}\n"
    ]

    # 📈 JPH (TPH)
    jph_header = _pick(rng, V["opt_titles"], variety)
    jph_lines = ["", jph_header]
    for wb in wbs:
        r = m_map.get(wb, {})
        v = r.get("TPH", None)
        if v is None or pd.isna(v):
            jph_lines.append(f"WB {fmt_wb(wb)}: TPH — No Data Found")
        else:
            mark = "✅ Target Met" if v >= tph_target else ("TPH 0 (no activity)" if float(v) == 0.0 else "❌ Target Not Met")
            jph_lines.append(f"WB {fmt_wb(wb)}: {mark} – {v:.2f}")

    # ✅ QA
    qa_header = _pick(rng, V["opt_titles_qa"], variety)
    qa_lines = ["", qa_header]
    for wb in wbs:
        r = m_map.get(wb, {})
        qv = r.get("Quality", None)
        if qv is None or pd.isna(qv):
            qa_lines.append(f"WB {fmt_wb(wb)}: QA not available – disregarded")
        else:
            mark = "✅ Target Met" if qv >= qa_target else "❌ Target Not Met"
            qa_lines.append(f"WB {fmt_wb(wb)}: {mark} – {qv:.2f}%")

    # 📝 QA Markdowns
    qa_md_lines = ["", "📝 QA Markdowns"]
    if qa_rows_full.empty:
        for wb in wbs:
            qa_md_lines.append(f"WB {fmt_wb(wb)}: No QA rows")
    else:
        for wb in wbs:
            sub = qa_rows_full[(qa_rows_full["wb"] == wb)]
            if sub.empty or (
                sub.get("jira", pd.Series([""])).fillna("").eq("").all()
                and sub.get("notes", pd.Series([""])).fillna("").eq("").all()
            ):
                qa_md_lines.append(f"WB {fmt_wb(wb)}: No QA rows")
            else:
                qa_md_lines.append(f"\nWB {fmt_wb(wb)}:")
                for _, r in sub.fillna("").iterrows():
                    j = str(r.get("jira", "")).strip()
                    n = str(r.get("notes","")).strip()
                    line = "• "
                    if j: line += f"JIRA: {j} "
                    if n: line += f"— {n}"
                    qa_md_lines.append(line if line != "• " else "• (blank row)")

    # 📊 WFM per WB
    wfm_lines = ["", "📊 WFM Metrics"]
    for wb in wbs:
        r = w_map.get(wb, {})
        a = r.get("adherence_ytd"); c = r.get("conformance_pct")
        s = r.get("shrinkage_pct"); u = r.get("unaccounted_time")
        a_txt = f"{a:.1f}%" if a is not None and pd.notna(a) else "NA"
        c_txt = f"{c:.1f}%" if c is not None and pd.notna(c) else "NA"
        s_txt = f"{s:.1f}%" if s is not None and pd.notna(s) else "NA"
        u_txt = f"{u:.2f}"  if u is not None and pd.notna(u) else "NA"
        wfm_lines.append(
            f"WB {fmt_wb(wb)}: Adherence Total % - YTD: {a_txt} | Conformance %: {c_txt} | "
            f"Shrinkage %: {s_txt} | Unaccounted Time: {u_txt}"
        )

    section3 = [
        "".join(options_intro),
        "\n".join(jph_lines), "\n\n",
        "\n".join(qa_lines), "\n\n",
        "\n".join(qa_md_lines), "\n\n",
        "\n".join(wfm_lines)
    ]

    # ---------- 4) WAY FORWARD ----------
    wf_lines = []
    wf_lines.append(_pick(rng, V["wf_quality"], variety).format(qa_target=fmt_num(qa_target)))
    if tph_curr is None or pd.isna(tph_curr) or (tph_curr < tph_target):
        wf_lines.append(_pick(rng, V["wf_tph"], variety))
    wf_lines.append(_pick(rng, V["wf_projects"], variety))
    wf_lines.append(_pick(rng, V["wf_followup"], variety))

    section4 = [
        "\n\n4. WAY FORWARD: Employee Commitment\n",
        "\nAgent will:",
        "\n" + "\n".join(wf_lines)
    ]

    return "\n".join([
        "\n".join(section1),
        "".join(section2),
        "".join(section3),
        "".join(section4),
    ]).strip()

# ---------------------------- GOOGLE FORM PREFILL CONFIG -----------------------
# ✅ Base form URL WITHOUT '/viewform'
FORM_BASE_URL = "https://docs.google.com/forms/d/e/1FAIpQLSe7gQqrYan3PP-jnmPBpu-f2-3J0un1GYZ8G7luDyggz4I_2g"

# Exact option labels for Focus KPI (ensure they match your form)
FOCUS_KPI_LABELS = {
    "productivity": "Productivity (TPH, CPH, APH, Occupancy, Utilisation, AHT, ASA, Task delivery etc)",
    # "quality": "Quality (… exact label here …)",  # add if your form has it
}

DEFAULT_FUNCTION = "UTRIPS"     # change if needed
DEFAULT_DURATION = "00:30"      # HH:MM as per your sample
DATE_FMT_FORM = "%Y-%m-%d"      # YYYY-MM-DD as per your sample

# Field IDs from your new pre-filled link
FORM_ENTRY = {
    "agent_name":       "entry.2141380597",
    "agent_email":      "entry.1105049073",
    "focus_kpi":        "entry.1511418124",
    "goal":             "entry.1631664394",
    "reality":          "entry.1826357261",
    "options":          "entry.2060350561",
    "way_forward":      "entry.1178661287",
    "coaching_minutes": "entry.239462267",
    "disc_date":        "entry.1104252303",
    "followup_date":    "entry.662189366",
    "function":         "entry.496312782",
    "coaching_notes":   "entry.XXXXXXXXXX",
}

# ---------- NEW HELPERS TO KEEP URL SHORT ----------
def _clean_for_form(s: str) -> str:
    """Minify text for prefill links: strip, collapse whitespace, keep it readable."""
    if not s:
        return ""
    s = re.sub(r"[ \t]+", " ", s.strip())
    s = re.sub(r"\n{2,}", "\n", s)      # collapse extra blank lines
    s = s.replace("• ", "- ")           # simpler bullets save characters
    return s

def _cap(s: str, limit: int) -> str:
    """Hard cap for URL safety."""
    if not s:
        return ""
    return (s[: max(0, limit-1)] + "…") if len(s) > limit else s

def _extract_sections_from_note(note_text: str) -> dict:
    """Pull the 4 sections from the generated note to prefill form fields."""
    blocks = {
        "goal": "",
        "reality": "",
        "options": "",
        "way_forward": "",
        "coaching_notes": "",  # intentionally blank to avoid duplication
    }
    def grab(start_pat, end_pat):
        m = re.search(start_pat, note_text, flags=re.IGNORECASE)
        if not m: return ""
        start = m.end()
        end = len(note_text)
        if end_pat:
            m2 = re.search(end_pat, note_text[start:], flags=re.IGNORECASE)
            if m2: end = start + m2.start()
        return note_text[start:end].strip()

    blocks["goal"]        = grab(r"\b1\.\s*GOAL\b.*?\n", r"\b2\.\s*REALITY\b")
    blocks["reality"]     = grab(r"\b2\.\s*REALITY\b.*?\n", r"\b3\.\s*OPTIONS\b")
    blocks["options"]     = grab(r"\b3\.\s*OPTIONS\b.*?\n", r"\b4\.\s*WAY FORWARD\b")
    blocks["way_forward"] = grab(r"\b4\.\s*WAY FORWARD\b.*?\n", None)
    return blocks

def _prefill_focus_kpi(row) -> str:
    """Pick a Focus KPI option label based on latest misses (TPH preferred, else blank if unknown)."""
    try:
        tph = row.get("TPH", None)
        qa  = row.get("Quality", None)
    except Exception:
        tph, qa = None, None

    tph_target = st.session_state.get('tph_target', None)
    qa_target  = st.session_state.get('qa_target', None)

    if tph is not None and pd.notna(tph) and tph_target is not None and float(tph) < float(tph_target):
        return FOCUS_KPI_LABELS["productivity"]
    # if qa is not None and pd.notna(qa) and qa_target is not None and float(qa) < float(qa_target):
    #     return FOCUS_KPI_LABELS["quality"]
    return ""

# ---------- UPDATED, SHORTENED PREFILL URL ----------
def build_prefill_url(agent_name: str,
                      agent_email: str,
                      note_text: str,
                      last_row_for_agent: dict,
                      *,
                      default_minutes: str = DEFAULT_DURATION,
                      default_function: str = DEFAULT_FUNCTION) -> str:
    """Construct a *short* Google Forms prefilled URL that always opens."""
    today = date.today()
    follow = today + timedelta(days=7)
    ymd = lambda d: d.strftime(DATE_FMT_FORM)

    sections = _extract_sections_from_note(note_text)
    # caps tuned to keep URL < ~1900 chars
    GOAL_MAX, REAL_MAX, OPT_MAX, WF_MAX, NOTES_MAX = 280, 600, 600, 420, 1200

    goal_txt     = _cap(_clean_for_form(sections.get("goal", "")),        GOAL_MAX)
    reality_txt  = _cap(_clean_for_form(sections.get("reality", "")),     REAL_MAX)
    options_txt  = _cap(_clean_for_form(sections.get("options", "")),     OPT_MAX)
    wayf_txt     = _cap(_clean_for_form(sections.get("way_forward", "")), WF_MAX)
    notes_txt    = _cap(_clean_for_form(note_text),                       NOTES_MAX)

    focus_label = _prefill_focus_kpi(last_row_for_agent)  # short label

    # ⚙️ Build URL parameters
    params = {}
    def put(key, value):
        if value is None:
            return
        if isinstance(value, str) and value.strip() == "":
            return
        entry = FORM_ENTRY.get(key)
        if not entry:
            return
        params[entry] = value

    put("agent_name",       agent_name or agent_email)
    put("agent_email",      agent_email)
    put("focus_kpi",        focus_label)
    put("goal",             goal_txt)
    put("reality",          reality_txt)
    put("options",          options_txt)
    put("way_forward",      wayf_txt)
    put("coaching_minutes", default_minutes)
    put("disc_date",        ymd(today))
    put("followup_date",    ymd(follow))
    put("function",         default_function)
    put("coaching_notes",   notes_txt)  # ✅ add this safely

    qs = urlparse.urlencode(params, doseq=True)
    return f"{FORM_BASE_URL}/viewform?usp=pp_url&{qs}"


# ---------------------------- UI: TABS / CARDS ---------------------------------
st.markdown("<div class='card'><h2>🧭 Payal's Team</h2><div class='small'>Automate 1:1 notes — track, coach, improve.</div></div>", unsafe_allow_html=True)

# ✅ 5 tabs (unpack all five)
tab_cfg, tab_notes, tab_risk, tab_team, tab_perf = st.tabs(
    ["🧰 Configure", "🧾 Notes Preview", "🚨 At-Risk", "📊 Team Snapshot", "🚨 Performance Management"]
)

# ---------------------------- CONFIGURE (Accordion) ----------------------------
with tab_cfg:
    # Accordion 1: Input Date Range
    with st.expander("📅 Input Date Range", expanded=True):
        interval = st.selectbox("Date interval", ["Weekly", "Bi-weekly", "Monthly"], index=0)
        opts = build_interval_options(interval)
        labels = [lab for lab, _ in opts]
        idx_end = 0
        idx_start = min(1, len(labels) - 1)
        start_lab = st.selectbox("Start", labels, index=idx_start, key="wb_start_lab")
        end_lab   = st.selectbox("End", labels, index=idx_end, key="wb_end_lab")
        start_pair = dict(opts)[start_lab]
        end_pair   = dict(opts)[end_lab]
        wb_start = start_pair[0]
        wb_end   = end_pair[1]
        wb_list = mondays_between(wb_start, wb_end) if wb_start and wb_end else []
        st.caption("Weeks selected: " + (", ".join([wb.isoformat() for wb in wb_list]) if wb_list else "None"))

    # Accordion 2: Coaching Customization
    with st.expander("🎯 Coaching Customization — Targets & Weights", expanded=True):
        col_a, col_b = st.columns(2)
        with col_a:
            tph_target = st.number_input("TPH target ≥", value=10.0, step=0.5, help="Minimum acceptable TPH.")
            w_tph = st.number_input("Weight: TPH", value=1.0, step=0.1, help="Sorting weight for At-Risk (TPH).")
        with col_b:
            qa_target  = st.number_input("Quality target ≥", value=98.0, step=0.5, help="Minimum acceptable QA %.")
            w_qa  = st.number_input("Weight: Quality", value=1.0, step=0.1, help="Sorting weight for At-Risk (Quality).")
        # Save in session for Focus KPI logic
        st.session_state['tph_target'] = tph_target
        st.session_state['qa_target']  = qa_target

    # Accordion 3: Language
    with st.expander("🗣️ Language (optional)", expanded=False):
        tone = st.selectbox("Tone", ["Supportive", "Direct", "Celebratory"], index=0,
                            help="Affects wording variety; numbers stay identical.")
        variety = st.slider("Language variety", 1, 3, 2, help="1=low, 2=medium, 3=high variation")
        st.caption("Variant seed (per agent+range): " + st.session_state.get("variant_seed_preview", "—"))

    # Accordion 4: Uploads
    with st.expander("📂 Upload Files (all optional)", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            metrics_file = st.file_uploader("📈 Metrics — Agent Dashboard (CSV/XLSX)", type=["csv","xlsx"], key="metrics")
            st.markdown(f"<span class='badge {'badge-teal' if metrics_file else 'badge-muted'}'>{'✅ Loaded' if metrics_file else 'Optional'}</span>", unsafe_allow_html=True)
        with c2:
            qa_file = st.file_uploader("✅ QA audits (CSV/XLSX)", type=["csv","xlsx"], key="qa")
            st.markdown(f"<span class='badge {'badge-teal' if qa_file else 'badge-muted'}'>{'✅ Loaded' if qa_file else 'Optional'}</span>", unsafe_allow_html=True)
        with c3:
            adherence_file = st.file_uploader("⏱ Adherence raw (CSV/XLSX)", type=["csv","xlsx"], key="adh")
            st.markdown(f"<span class='badge {'badge-teal' if adherence_file else 'badge-muted'}'>{'✅ Loaded' if adherence_file else 'Optional'}</span>", unsafe_allow_html=True)
        c4, c5, _ = st.columns(3)
        with c4:
            shrinkage_file = st.file_uploader("🧩 Shrinkage aggregated (CSV/XLSX)", type=["csv","xlsx"], key="shr")
            st.markdown(f"<span class='badge {'badge-teal' if shrinkage_file else 'badge-muted'}'>{'✅ Loaded' if shrinkage_file else 'Optional'}</span>", unsafe_allow_html=True)
        with c5:
            conformance_file = st.file_uploader("🧭 Conformance raw (CSV/XLSX)", type=["csv","xlsx"], key="conf")
            st.markdown(f"<span class='badge {'badge-teal' if conformance_file else 'badge-muted'}'>{'✅ Loaded' if conformance_file else 'Optional'}</span>", unsafe_allow_html=True)

    # --- Bottom-centered Generate button ---
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    pad_l, col_btn, pad_r = st.columns([2, 1, 2])
    with col_btn:
        generate = st.button("🚀 Generate Notes")

# ---------------------------- PROCESSING ---------------------------------------
notes_df = pd.DataFrame()
risk_df  = pd.DataFrame()
summary = {"agents":0, "notes":0, "risk":0}
_metrics_df_for_charts = pd.DataFrame()

if 'wb_list' in locals() and generate:
    if not wb_list:
        st.warning("Please select a valid Week Beginning range.")
    else:
        with st.spinner("Reading files, merging data, and generating notes…"):
            # Read datasets (optional each)
            metrics_df = load_metrics(metrics_file.getvalue(), wb_list) if 'metrics_file' in locals() and metrics_file else pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality"])
            qa_df      = load_qa(qa_file.getvalue(), wb_list) if 'qa_file' in locals() and qa_file else pd.DataFrame(columns=["agent_email","wb","jira","notes","final"])
            adh_df     = load_adherence(adherence_file.getvalue(), wb_list) if 'adherence_file' in locals() and adherence_file else pd.DataFrame(columns=["agent_email","wb","adherence_ytd"])
            shr_df     = load_shrinkage(shrinkage_file.getvalue(), wb_list) if 'shrinkage_file' in locals() and shrinkage_file else pd.DataFrame(columns=["agent_email","wb","shrinkage_pct","unaccounted_time"])
            conf_df    = load_conformance(conformance_file.getvalue(), wb_list) if 'conformance_file' in locals() and conformance_file else pd.DataFrame(columns=["agent_email","wb","conformance_pct"])

            # Build base keys from whatever is present
            keys = []
            for d in [
                metrics_df[["agent_email","wb"]] if not metrics_df.empty else None,
                qa_df[["agent_email","wb"]] if not qa_df.empty else None,
                adh_df[["agent_email","wb"]] if not adh_df.empty else None,
                shr_df[["agent_email","wb"]] if not shr_df.empty else None,
                conf_df[["agent_email","wb"]] if not conf_df.empty else None
            ]:
                if d is not None and not d.empty: keys.append(d)

            if not keys:
                st.warning("No data uploaded. Please upload at least one file.")
            else:
                base = pd.concat(keys, ignore_index=True).drop_duplicates()
                out = base.copy()
                out = out.merge(metrics_df, on=["agent_email","wb"], how="left")
                out = out.merge(adh_df, on=["agent_email","wb"], how="left")
                out = out.merge(shr_df, on=["agent_email","wb"], how="left")
                out = out.merge(conf_df, on=["agent_email","wb"], how="left")
                out = out.merge(qa_df, on=["agent_email","wb"], how="left")
                if "agent_name" not in out.columns: out["agent_name"] = ""
                out["agent_name"] = out["agent_name"].fillna("")
                out = out.sort_values(["agent_email","wb"]).reset_index(drop=True)

                # Flags/Priority
                flag_cols, priorities = [], []
                for _, r in out.iterrows():
                    flags, pri = build_flags_and_priority(r, st.session_state['tph_target'], st.session_state['qa_target'], w_tph, w_qa)
                    flag_cols.append(", ".join(flags) if flags else "ok")
                    priorities.append(pri)
                out["flags"] = flag_cols
                out["priority"] = priorities

                # Build notes + prefilled form links
                notes_rows, at_risk_rows = [], []
                for agent_email, grp in out.groupby("agent_email", sort=False):
                    # Name best-effort
                    agent_name = ""
                    if "agent_name" in grp.columns:
                        cand = grp["agent_name"].dropna()
                        agent_name = next((x for x in cand if str(x).strip() != ""), "")

                    metrics_rows = grp[["wb","TPH","Quality"]] if set(["wb","TPH","Quality"]).issubset(grp.columns) else pd.DataFrame(columns=["wb","TPH","Quality"])
                    wfm_cols = [c for c in ["wb","adherence_ytd","conformance_pct","shrinkage_pct","unaccounted_time"] if c in grp.columns]
                    wfm_rows = grp[wfm_cols] if wfm_cols else pd.DataFrame(columns=["wb","adherence_ytd","conformance_pct","shrinkage_pct","unaccounted_time"])

                    note_text = build_template_note(
                        agent_name, agent_email, wb_list,
                        metrics_rows,
                        qa_df[qa_df["agent_email"]==agent_email] if not qa_df.empty else pd.DataFrame(),
                        wfm_rows, st.session_state['tph_target'], st.session_state['qa_target'],
                        tone=tone, variety=variety
                    )

                    # Recent row for Focus KPI etc.
                    try:
                        last_row = (grp.sort_values("wb").iloc[-1]).to_dict()
                    except Exception:
                        last_row = {}

                    form_url = build_prefill_url(
                        agent_name=agent_name,
                        agent_email=agent_email,
                        note_text=note_text,
                        last_row_for_agent=last_row,
                        default_minutes=DEFAULT_DURATION,
                        default_function=DEFAULT_FUNCTION
                    )

                    notes_rows.append({
                        "agent_email": agent_email,
                        "agent_name": agent_name,
                        "weeks": f"{fmt_mmddyyyy(min(wb_list))} – {fmt_mmddyyyy(max(wb_list))}",
                        "coaching_note": note_text,
                        "form_url": form_url,
                        "latest_TPH": float(last_row.get("TPH")) if isinstance(last_row.get("TPH"), (int,float)) or str(last_row.get("TPH")).replace('.','',1).isdigit() else None,
                        "latest_QA": float(last_row.get("Quality")) if isinstance(last_row.get("Quality"), (int,float)) or str(last_row.get("Quality")).replace('.','',1).isdigit() else None,
                        "flags": ", ".join(set(grp.get("flags", pd.Series(["ok"])).astype(str).tolist()))
                    })

                    # At-risk: include if TPH or QA miss (current behavior)
                    if not metrics_rows.empty:
                        for _, rr in grp.iterrows():
                            miss_tph = ("TPH" in rr and pd.notna(rr.get("TPH")) and rr.get("TPH") < st.session_state['tph_target'])
                            miss_qa  = ("Quality" in rr and pd.notna(rr.get("Quality")) and rr.get("Quality") < st.session_state['qa_target'])
                            if miss_tph or miss_qa:
                                at_risk_rows.append({
                                    "agent_email": agent_email,
                                    "agent_name": agent_name,
                                    "wb": rr["wb"].isoformat() if isinstance(rr["wb"], date) else str(rr["wb"]),
                                    "TPH": rr.get("TPH"),
                                    "Quality": rr.get("Quality"),
                                    "flag_tph": "Y" if miss_tph else "N",
                                    "flag_quality": "Y" if miss_qa else "N",
                                    "priority": rr.get("priority", 0.0)
                                })

                notes_df = pd.DataFrame(notes_rows)
                risk_df  = pd.DataFrame(at_risk_rows).sort_values(["wb","priority"], ascending=[True, False]) if at_risk_rows else pd.DataFrame(columns=["agent_email","agent_name","wb","TPH","Quality","flag_tph","flag_quality","priority"])
                summary = {"agents": out["agent_email"].nunique(), "notes": len(notes_rows), "risk": 0 if risk_df.empty else len(risk_df["agent_email"].unique())}

                _metrics_df_for_charts = metrics_df.copy() if not metrics_df.empty else pd.DataFrame()

# ---------------------------- NOTES PREVIEW ------------------------------------
with tab_notes:
    if notes_df.empty:
        st.info("Upload data and click **Generate Notes** to see previews here.")
    else:
        st.markdown(f"<div class='ribbon'>🧑‍🤝‍🧑 Agents processed: <b>{summary['agents']}</b> &nbsp;|&nbsp; 🧾 Notes generated: <b>{summary['notes']}</b> &nbsp;|&nbsp; 🚨 At-risk: <b>{summary['risk']}</b></div>", unsafe_allow_html=True)

        # Search/filter bar
        q = st.text_input("Search agent (name or email)")
        filt_df = notes_df.copy()
        if q:
            ql = q.strip().lower()
            filt_df = filt_df[filt_df["agent_email"].str.lower().str.contains(ql) | filt_df["agent_name"].str.lower().str.contains(ql)]

        for _, row in filt_df.iterrows():
            header = f"👤 {row['agent_name'] or row['agent_email']} — {row['weeks']}"
            agent_key_base = f"{row['agent_email']}|{row['weeks']}"
            disc_key = f"disc_{agent_key_base}"
            show_note_key = f"show_note_{agent_key_base}"

            with st.expander(header, expanded=False):
                # Prefilled Form link
                if "form_url" in row and row["form_url"]:
                    st.markdown(
                        f"<div class='mini-row link-btn'><a href='{row['form_url']}' target='_blank'>📝 Open prefilled form</a></div>",
                        unsafe_allow_html=True
                    )
                else:
                    st.caption("Form link unavailable")

                # Tiny summary chips
                chips = []
                chips.append(f"<span class='summary-chip'>WB: {row['weeks']}</span>")
                if pd.notna(row.get("latest_TPH")):
                    chips.append(f"<span class='summary-chip'>TPH: {row['latest_TPH']:.2f}</span>")
                if pd.notna(row.get("latest_QA")):
                    chips.append(f"<span class='summary-chip'>QA: {row['latest_QA']:.2f}%</span>")
                if str(row.get("flags","ok")) != "ok":
                    chips.append(f"<span class='summary-chip'>Flags: {row.get('flags')}</span>")
                st.markdown(" ".join(chips), unsafe_allow_html=True)
        st.download_button("⬇️ Download coaching_notes.csv",
                           data=notes_df.to_csv(index=False).encode("utf-8"),
                           file_name="coaching_notes.csv",
                           mime="text/csv")

# ---------------------------- AT-RISK ------------------------------------------
with tab_risk:
    if risk_df.empty:
        st.info("No agents at risk for the selected weeks (or no metrics data).")
    else:
        st.markdown("<h4 class='section-title'>🚨 At-Risk (sorted by priority)</h4>", unsafe_allow_html=True)
        show_df(risk_df, height=420)
        st.download_button("⬇️ Download at_risk_agents.csv",
                           data=risk_df.to_csv(index=False).encode("utf-8"),
                           file_name="at_risk_agents.csv",
                           mime="text/csv")

# ---------------------------- TEAM SNAPSHOT (Charts) ---------------------------
with tab_team:
    if notes_df.empty:
        st.info("Generate notes to see team snapshots.")
    else:
        st.markdown("<div class='card'><h4 class='section-title'>📊 Quick Snapshot</h4>", unsafe_allow_html=True)

        metrics_for_chart = pd.DataFrame()
        if 'metrics_file' in locals() and metrics_file:
            try:
                tmp_metrics = load_metrics(metrics_file.getvalue(), mondays_between(wb_start, wb_end))
                if not tmp_metrics.empty:
                    metrics_for_chart = tmp_metrics.copy()
            except Exception:
                pass

        if metrics_for_chart.empty:
            st.caption("No metrics file available for charts.")
        else:
            avg = (metrics_for_chart.groupby("wb")
                   .agg(Avg_TPH=("TPH", "mean"),
                        Avg_QA=("Quality", "mean"))
                   .reset_index()
                   .sort_values("wb"))
            avg["WB"] = avg["wb"].apply(fmt_wb)
            st.write("**Average TPH & QA by Week**")
            st.line_chart(avg.set_index("WB")[["Avg_TPH","Avg_QA"]])

            last_wb = avg["wb"].max()
            last_slice = metrics_for_chart[metrics_for_chart["wb"] == last_wb].copy()
            if not last_slice.empty:
                tph_hits = int((last_slice["TPH"] >= st.session_state['tph_target']).sum())
                tph_miss = int((last_slice["TPH"] <  st.session_state['tph_target']).sum())
                qa_hits  = int((last_slice["Quality"] >= st.session_state['qa_target']).sum())
                qa_miss  = int((last_slice["Quality"] <  st.session_state['qa_target']).sum())

                st.write("**Target Achievement (Last WB)**")
                bar_df = pd.DataFrame({
                    "Metric": ["TPH Met","TPH Miss","QA Met","QA Miss"],
                    "Count":  [tph_hits, tph_miss, qa_hits, qa_miss]
                })
                st.bar_chart(bar_df.set_index("Metric"))

                last_slice["Agent"] = last_slice["agent_email"].fillna("").replace("", "unknown")
                colA, colB = st.columns(2)
                with colA:
                    st.write("**TPH — Met (last WB)**")
                    met_tph = last_slice[last_slice["TPH"] >= st.session_state['tph_target']].sort_values("TPH", ascending=False)
                    if not met_tph.empty:
                        st.bar_chart(met_tph.set_index("Agent")[["TPH"]])
                    else:
                        st.caption("No agents met TPH target.")
                    st.write("**QA — Met (last WB)**")
                    met_qa = last_slice[last_slice["Quality"] >= st.session_state['qa_target']].sort_values("Quality", ascending=False)
                    if not met_qa.empty:
                        st.bar_chart(met_qa.set_index("Agent")[["Quality"]])
                    else:
                        st.caption("No QA met.")
                with colB:
                    st.write("**TPH — Missed (last WB)**")
                    miss_tph = last_slice[last_slice["TPH"] < st.session_state['tph_target']].sort_values("TPH")
                    if not miss_tph.empty:
                        st.bar_chart(miss_tph.set_index("Agent")[["TPH"]])
                    else:
                        st.caption("No TPH misses.")
                    st.write("**QA — Missed (last WB)**")
                    miss_qa = last_slice[last_slice["Quality"] < st.session_state['qa_target']].sort_values("Quality")
                    if not miss_qa.empty:
                        st.bar_chart(miss_qa.set_index("Agent")[["Quality"]])
                    else:
                        st.caption("No QA misses.")

        st.markdown("</div>", unsafe_allow_html=True)

# ---------------------------- PERFORMANCE MGMT (placeholder) -------------------
with tab_perf:
    st.info("Performance Management — coming soon. Add actions/escalations here.")
