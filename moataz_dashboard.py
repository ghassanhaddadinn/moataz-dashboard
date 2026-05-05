"""
Moataz Performance Dashboard
Fetches live data from Odoo 17 and renders dashboard.html
"""

import os
import sys
import json
import xmlrpc.client
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ODOO_URL      = os.environ.get("ODOO_URL",      "https://nestu.odoo.com")
ODOO_DB       = os.environ.get("ODOO_DB",       "odooerp-ae-nestu-health-main-12720997")
ODOO_USERNAME = os.environ.get("ODOO_USERNAME",  "")
ODOO_API_KEY  = os.environ.get("ODOO_API_KEY",   "")

USER_ID    = 23
COMPANY_ID = 2
TZ_AMMAN   = ZoneInfo("Asia/Amman")

TIER_LABELS = {
    "KA": "Key Account",
    "1":  "Tier 1",
    "2":  "Tier 2",
    "3":  "Tier 3",
    "NT": "Not Targeted",
}
COVERAGE_TIERS = {"KA", "1", "2", "3"}

# ─── TARGETED ACCOUNTS (official plan, 95 accounts) ───────────────────────────
# Tuple: (plan_name, tier_num, penetration_status, target_fy_jod)
TARGETED_ACCOUNTS = [
    ("Jo Pet Zone - Al-Weibdeh",                                         1, "NO",          3000),
    ("4 Pets (مؤسسة سعد حسان لبيع مستلزمات الحيوانات الأليفة)",         1, "Penetrated",  8000),
    ("Best Buddies",                                                       3, "Penetrated",  None),
    ("Pet Mart",                                                           2, "Penetrated",  None),
    ("جبل ارارت لتربية الحيوانات الاليفة - Abdoun",                        1, "Penetrated",  5000),
    ("Jo Pet Zone - Al-Sweifieh",                                          1, "NO",          3000),
    ("Mr.Pet/PetBuzz",                                                     1, "NO",          None),
    ("Asia Pet Shop",                                                      2, "NO",          1000),
    ("Amman Mart/Amman Pets Express",                                      1, "NO",          1000),
    ("Pet Express",                                                        1, "NO",          1500),
    ("Amman Pets",                                                         1, "NO",          None),
    ("Zoo Keeper",                                                         1, "Penetrated",  8000),
    ("Smart pet",                                                          2, "NO",          1200),
    ("Pet Panda",                                                          2, "NO",          None),
    ("Mimz Pet Mall",                                                      3, "NO",          1000),
    ("Pet Castle",                                                         2, "Penetrated",  4000),
    ("Green Island Pet Shop",                                              3, "Penetrated",  1000),
    ("Jaguar Pet Store",                                                   3, "NO",          1000),
    ("Lucky Pet Shop",                                                     2, "NO",          2000),
    ("Pet oasis",                                                          2, "NO",          1000),
    ("Pet Moon",                                                           2, "Penetrated",  2000),
    ("My Pets",                                                            2, "Penetrated",  None),
    ("Shmeisani Pet Shop",                                                 2, "NO",          1000),
    ("Pet House - Khalda",                                                 1, "Penetrated",  3000),
    ("Pet House - Tlaa' Al-Ali",                                           2, "Penetrated",  1000),
    ("Infinity Pet Store",                                                 2, "Penetrated",  2000),
    ("Pet Zone Khalda",                                                    2, "NO",          None),
    ("Nemo pet store",                                                     2, "NO",          1000),
    ("جبل ارارت لتربية الحيوانات الاليفة - Dabouq",                        1, "Penetrated",  6000),
    ("The Pet Hood",                                                       2, "Penetrated",  8000),
    ("Pure Pet Shop",                                                      2, "NO",          1500),
    ("The Black Wolf Dabouq",                                              2, "NO",          1000),
    ("Pet House - Al-Fuheis",                                              2, "NO",          1000),
    ("Jolly Pet Shop",                                                     2, "NO",          1000),
    ("Phoenix/Sadeen Pet shop",                                            2, "Penetrated",  3000),
    ("Adam's Pet Shop",                                                    2, "NO",          1000),
    ("Pets Galaxy",                                                        2, "Penetrated",  4500),
    ("WaterBox Aquatics",                                                  2, "Penetrated",  None),
    ("Whispered Natutre Pet Shop",                                         3, "NO",           600),
    ("Pet Mania",                                                          2, "Penetrated",  1000),
    ("Cheetah Pet Shop Saru",                                              3, "NO",          None),
    ("Xtreme Pet Center Saru",                                             3, "NO",          None),
    ("Pets Daily Pet Shop",                                                1, "NO",          2000),
    ("Nature Spirit Pet Shop",                                             3, "Penetrated",  None),
    ("Scooby Doo Abu Nsair",                                               3, "NO",          None),
    ("Pet Panda Shafa Badran",                                             2, "NO",          None),
    ("Paws&Claws Pet Store",                                               3, "NO",          1000),
    ("Nemo Pet Store Jubeiha",                                             2, "NO",          1000),
    ("ريش وفرو",                        2, "NO",          None),
    ("Sea horse 2",                                                        2, "Penetrated",  1200),
    ("Black Wolf Pet Shop",                                                2, "NO",           600),
    ("K Pet Shop",                                                         3, "Penetrated",  None),
    ("Victoria pet store",                                                 2, "NO",           600),
    ("Kanari Heba 1",                                                      1, "Penetrated",  1000),
    ("Kanari Heba Tabarbour 2",                                            1, "NO",           500),
    ("Canari hamoudh Al-Hashmi",                                           2, "Penetrated",  1500),
    ("كناري شوق Al Hashmi",        3, "NO",          None),
    ("PAWsitive Nuzha",                                                    3, "NO",          None),
    ("طيور المعتز",      3, "Penetrated",  None),
    ("Talia Pet Shop",                                                     2, "NO",          1000),
    ("Pet Stop",                                                           1, "Penetrated",  1000),
    ("Active Pet",                                                         1, "Penetrated",  2000),
    ("Pets Galaxy 2",                                                      3, "Penetrated",  1500),
    ("Castle Pet Marj Al Hamam",                                           1, "NO",          1500),
    ("Spider Pet Shop Marj Al Hamam",                                      2, "NO",          None),
    ("Pet Town",                                                           3, "NO",          1500),
    ("Pet Home Marj Al Hamam",                                             3, "NO",          None),
    ("Pets City Marj Al Hamam",                                            3, "NO",          None),
    ("Mera Pet Shop",                                                      3, "Penetrated",   500),
    ("Garfield pet shop",                                                  3, "NO",          None),
    ("AlSharis Pet Shop",                                                  3, "NO",          None),
    ("Oliver Pets Al Nakhil",                                              2, "NO",          None),
    ("Alpha Pet Shop",                                                     2, "NO",           600),
    ("Pet Rangers",                                                        2, "NO",          1000),
    ("Aleef Pet shop",                                                     3, "NO",           600),
    ("Pet Boulevard",                                                      2, "NO",          None),
    ("Fox pet shop",                                                       3, "NO",          1000),
    ("رفق لمستلزمات الحيوانات",  3, "NO",          1000),
    ("Paradise Pet Store",                                                 3, "NO",          None),
    ("JK9 Center",                                                         1, "NO",          None),
    ("BoosBoss Pet Shop",                                                  3, "NO",          None),
    ("أبو جرير لمستلزمات الحيوانات الأليفة",  3, "NO",          None),
    ("Scottish Shop",                                                      3, "NO",          None),
    ("Cat Cafe",                                                           2, "NO",          None),
    ("Jungle Pet Shop",                                                    3, "NO",          None),
    ("Animal Planet",                                                      3, "NO",          None),
    ("Sporty Pet",                                                         1, "NO",          1000),
    ("Paws Corner",                                                        3, "NO",          None),
    ("Ocean Pet Store",                                                    3, "NO",          None),
    ("عالم الكناري للحيوانات",  3, "NO",          None),
    ("Zoo Pet Store",                                                      3, "NO",          None),
    ("Star Pets",                                                          3, "NO",          None),
    ("محمد الحموي لاغذية الحيوانات و لوازمها 1",                           2, "Penetrated",  7000),
    ("محمد الحموي لاغذية الحيوانات و لوازمها 2",                           2, "Penetrated",  7000),
]

PLAN_TIER_LABELS = {1: "Tier 1", 2: "Tier 2", 3: "Tier 3"}

# ─── CONNECT ──────────────────────────────────────────────────────────────────
def connect_odoo():
    if not ODOO_USERNAME or not ODOO_API_KEY:
        sys.exit("[ERROR] ODOO_USERNAME and ODOO_API_KEY environment variables must be set.")

    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY, {})
    if not uid:
        sys.exit("[ERROR] Odoo authentication failed. Check credentials.")

    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    print(f"[AUTH] Connected as uid={uid}")
    return models, uid

# ─── FETCH ────────────────────────────────────────────────────────────────────
def fetch_invoices(models, uid):
    domain = [
        ["move_type",       "=",  "out_invoice"],
        ["state",           "=",  "posted"],
        ["invoice_user_id", "=",  USER_ID],
        ["company_id",      "=",  COMPANY_ID],
    ]
    fields = ["id", "invoice_date", "invoice_user_id", "amount_untaxed", "partner_id"]
    try:
        records = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            "account.move", "search_read",
            [domain], {"fields": fields, "limit": 0}
        )
    except Exception as e:
        sys.exit(f"[ERROR] fetch_invoices failed: {e}")
    print(f"[FETCH] {len(records)} invoices")
    return records

def fetch_accounts(models, uid):
    domain = [
        ["user_id",       "=",  USER_ID],
        ["company_id",    "in", [COMPANY_ID, False]],
        ["customer_rank", ">",  0],
    ]
    fields = ["id", "name", "user_id", "x_studio_tier_1", "customer_rank"]
    try:
        records = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            "res.partner", "search_read",
            [domain], {"fields": fields, "limit": 0}
        )
    except Exception as e:
        sys.exit(f"[ERROR] fetch_accounts failed: {e}")
    print(f"[FETCH] {len(records)} accounts")
    return records

def fetch_visits(models, uid):
    domain = [
        ["user_id",    "=", USER_ID],
        ["company_id", "=", COMPANY_ID],
    ]
    fields = ["id", "time_from", "client_visited_id", "user_id", "company_id"]
    try:
        records = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            "client.interaction", "search_read",
            [domain], {"fields": fields, "limit": 0}
        )
    except Exception as e:
        sys.exit(f"[ERROR] fetch_visits failed: {e}")
    print(f"[FETCH] {len(records)} visits")
    return records

def fetch_all_partners(models, uid):
    """Fetch ALL res.partner for user_id=23 — no customer_rank filter — for plan matching and tier lookup."""
    domain = [
        ["user_id",    "=",  USER_ID],
        ["company_id", "in", [COMPANY_ID, False]],
    ]
    fields = ["id", "name", "x_studio_tier_1"]
    try:
        records = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            "res.partner", "search_read",
            [domain], {"fields": fields, "limit": 0}
        )
    except Exception as e:
        sys.exit(f"[ERROR] fetch_all_partners failed: {e}")
    print(f"[FETCH] {len(records)} total partners (for plan matching)")
    return records

# ─── VALIDATE ─────────────────────────────────────────────────────────────────
def validate_data(invoices, accounts, visits):
    issues = []

    # Invoice anomalies
    missing_user   = sum(1 for r in invoices if not r.get("invoice_user_id"))
    missing_date   = sum(1 for r in invoices if not r.get("invoice_date"))
    missing_partner = sum(1 for r in invoices if not r.get("partner_id"))
    print(f"[ANOMALY] Invoices missing invoice_user_id: {missing_user}")
    print(f"[ANOMALY] Invoices missing invoice_date: {missing_date}")
    print(f"[ANOMALY] Invoices missing partner_id: {missing_partner}")

    # Visit anomalies
    missing_cvid = sum(1 for r in visits if not r.get("client_visited_id"))
    missing_tf   = sum(1 for r in visits if not r.get("time_from"))
    print(f"[ANOMALY] Visits missing client_visited_id: {missing_cvid}")
    print(f"[ANOMALY] Visits missing time_from: {missing_tf}")

    # Account anomalies
    rank_zero = sum(1 for r in accounts if r.get("customer_rank", 0) == 0)
    print(f"[ANOMALY] Accounts with customer_rank=0 excluded from coverage: {rank_zero}")

    # Revenue validation
    total_revenue = sum(r.get("amount_untaxed", 0) for r in invoices)
    grouped_rev = defaultdict(float)
    for r in invoices:
        d = r.get("invoice_date")
        if d:
            ym = d[:7]
            grouped_rev[ym] += r.get("amount_untaxed", 0)
    grouped_total = sum(grouped_rev.values())
    rev_diff = abs(total_revenue - grouped_total)
    status = "OK" if rev_diff < 0.01 else f"MISMATCH diff={rev_diff:.4f}"
    print(f"[VALIDATION] Revenue total={total_revenue:.2f} vs grouped={grouped_total:.2f} -> {status}")

    # Visit validation
    total_visits = len([r for r in visits if r.get("time_from")])
    grouped_v = defaultdict(int)
    for r in visits:
        tf = r.get("time_from")
        if tf:
            dt_utc = _parse_odoo_dt(tf)
            if dt_utc:
                ym = dt_utc.astimezone(TZ_AMMAN).strftime("%Y-%m")
                grouped_v[ym] += 1
    grouped_v_total = sum(grouped_v.values())
    v_status = "OK" if total_visits == grouped_v_total else f"MISMATCH"
    print(f"[VALIDATION] Visit total={total_visits} vs grouped={grouped_v_total} -> {v_status}")

    return {
        "total_revenue": total_revenue,
        "total_visits": len(visits),
        "missing_invoice_user": missing_user,
        "missing_invoice_date": missing_date,
        "missing_partner": missing_partner,
        "missing_visit_client": missing_cvid,
        "missing_visit_tf": missing_tf,
    }

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def _parse_odoo_dt(s):
    """Parse Odoo datetime string (UTC) -> aware datetime object."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None

def _ym_label(ym):
    """'2025-03' -> 'Mar 2025'"""
    try:
        return datetime.strptime(ym, "%Y-%m").strftime("%b %Y")
    except Exception:
        return ym

def _partner_id(val):
    if isinstance(val, (list, tuple)) and len(val) >= 1:
        return val[0]
    return None

def _partner_name(val):
    if isinstance(val, (list, tuple)):
        return val[1] if len(val) > 1 else str(val[0])
    if isinstance(val, str):
        return val
    return ""

def _pid(field_val):
    """Extract partner id from many2one field."""
    if isinstance(field_val, (list, tuple)):
        return field_val[0] if field_val else None
    return field_val if isinstance(field_val, int) else None

def _pname(field_val):
    """Extract partner name from many2one field."""
    if isinstance(field_val, (list, tuple)) and len(field_val) > 1:
        return field_val[1]
    return ""

# ─── BUILD MONTHLY STRUCTURES ─────────────────────────────────────────────────
def build_monthly_structures(invoices, accounts, visits):
    """
    Returns a dict of month-keyed structures for all downstream builders.
    """
    # ── Account lookup ──────────────────────────────────────────────────────
    account_map = {}  # partner_id -> {name, tier}
    coverage_partner_ids = set()
    for a in accounts:
        tier = a.get("x_studio_tier_1") or None
        account_map[a["id"]] = {
            "name": a.get("name", ""),
            "tier": tier,
        }
        if tier in COVERAGE_TIERS:
            coverage_partner_ids.add(a["id"])

    coverage_denom = len(coverage_partner_ids)

    # ── Monthly invoice aggregates ──────────────────────────────────────────
    # inv_month[ym] = {partner_id: revenue}
    inv_month = defaultdict(lambda: defaultdict(float))
    inv_ytd   = defaultdict(float)   # partner_id -> YTD revenue
    inv_ever  = set()                # partner_ids with any posted invoice (all time)

    for r in invoices:
        d = r.get("invoice_date")
        if not d:
            continue
        ym = d[:7]
        pid = _pid(r.get("partner_id"))
        if pid is None:
            continue
        amt = r.get("amount_untaxed", 0) or 0
        inv_month[ym][pid] += amt
        inv_ever.add(pid)

    # YTD (current year)
    current_year = datetime.now().year
    for r in invoices:
        d = r.get("invoice_date")
        if not d:
            continue
        if int(d[:4]) != current_year:
            continue
        pid = _pid(r.get("partner_id"))
        if pid is None:
            continue
        inv_ytd[pid] += r.get("amount_untaxed", 0) or 0

    # ── Monthly visit aggregates ────────────────────────────────────────────
    # vis_month[ym] = {partner_id: [visit dates]}
    vis_month = defaultdict(lambda: defaultdict(list))

    for r in visits:
        tf = r.get("time_from")
        if not tf:
            continue
        dt_utc = _parse_odoo_dt(tf)
        if not dt_utc:
            continue
        dt_local = dt_utc.astimezone(TZ_AMMAN)
        ym = dt_local.strftime("%Y-%m")
        pid = _pid(r.get("client_visited_id"))
        if pid is None:
            continue
        vis_month[ym][pid].append(dt_local)

    # ── Determine sorted months ─────────────────────────────────────────────
    all_months = sorted(set(list(inv_month.keys()) + list(vis_month.keys())))

    return {
        "account_map":          account_map,
        "coverage_denom":       coverage_denom,
        "coverage_partner_ids": coverage_partner_ids,
        "inv_month":            inv_month,
        "inv_ytd":              inv_ytd,
        "inv_ever":             inv_ever,
        "vis_month":            vis_month,
        "all_months":           all_months,
    }

# ─── TOP ACCOUNTS ─────────────────────────────────────────────────────────────
def build_top_accounts(ms, current_ym, top_n=25):
    inv_month  = ms["inv_month"]
    inv_ytd    = ms["inv_ytd"]
    vis_month  = ms["vis_month"]
    account_map = ms["account_map"]

    month_partners = inv_month.get(current_ym, {})
    vis_partners   = vis_month.get(current_ym, {})

    rows = []
    for pid, rev_mtd in month_partners.items():
        info = account_map.get(pid, {})
        visits_list = vis_partners.get(pid, [])
        last_visit = max(visits_list).strftime("%Y-%m-%d") if visits_list else ""
        rows.append({
            "name":       info.get("name", f"Partner {pid}"),
            "tier":       TIER_LABELS.get(info.get("tier") or "", "Untiered"),
            "rev_mtd":    rev_mtd,
            "rev_ytd":    inv_ytd.get(pid, 0),
            "visits_mtd": len(visits_list),
            "last_visit": last_visit,
        })

    rows.sort(key=lambda x: x["rev_mtd"], reverse=True)
    return rows[:top_n]

# ─── CONVERSION GAP ───────────────────────────────────────────────────────────
def build_conversion_gap(ms, current_ym):
    inv_month  = ms["inv_month"]
    vis_month  = ms["vis_month"]
    account_map = ms["account_map"]

    month_invoice_partners = set(inv_month.get(current_ym, {}).keys())
    vis_partners = vis_month.get(current_ym, {})

    rows = []
    for pid, visits_list in vis_partners.items():
        if pid in month_invoice_partners:
            continue
        info = account_map.get(pid, {})
        last_visit = max(visits_list).strftime("%Y-%m-%d") if visits_list else ""
        rows.append({
            "name":       info.get("name", f"Partner {pid}"),
            "tier":       TIER_LABELS.get(info.get("tier") or "", "Untiered"),
            "visits_mtd": len(visits_list),
            "last_visit": last_visit,
        })

    rows.sort(key=lambda x: x["visits_mtd"], reverse=True)
    return rows

# ─── TIER PERFORMANCE ─────────────────────────────────────────────────────────
def build_tier_performance(ms, accounts, current_ym):
    inv_month   = ms["inv_month"]
    inv_ytd     = ms["inv_ytd"]
    vis_month   = ms["vis_month"]
    account_map = ms["account_map"]

    month_inv = inv_month.get(current_ym, {})
    month_vis = vis_month.get(current_ym, {})

    tier_data = {}
    for tier_key in list(COVERAGE_TIERS) + ["NT", None]:
        label = TIER_LABELS.get(tier_key or "", "Untiered") if tier_key else "Untiered"
        tier_data[tier_key] = {
            "label":      label,
            "n_accounts": 0,
            "n_visited":  0,
            "rev_mtd":    0.0,
            "rev_ytd":    0.0,
        }

    for a in accounts:
        tier = a.get("x_studio_tier_1") or None
        pid  = a["id"]
        if tier not in tier_data:
            tier_data[tier] = {
                "label":      TIER_LABELS.get(tier or "", "Untiered"),
                "n_accounts": 0,
                "n_visited":  0,
                "rev_mtd":    0.0,
                "rev_ytd":    0.0,
            }
        tier_data[tier]["n_accounts"] += 1
        if pid in month_vis:
            tier_data[tier]["n_visited"] += 1
        if pid in month_inv:
            tier_data[tier]["rev_mtd"] += month_inv[pid]
        if pid in inv_ytd:
            tier_data[tier]["rev_ytd"] += inv_ytd[pid]

    rows = []
    for tier_key, d in tier_data.items():
        if d["n_accounts"] == 0:
            continue
        cov_pct = (
            round(d["n_visited"] / d["n_accounts"] * 100, 1)
            if tier_key in COVERAGE_TIERS and d["n_accounts"] > 0
            else None
        )
        rows.append({
            "tier":        d["label"],
            "n_accounts":  d["n_accounts"],
            "n_visited":   d["n_visited"],
            "coverage_pct": cov_pct,
            "rev_mtd":     d["rev_mtd"],
            "rev_ytd":     d["rev_ytd"],
            "sort_order":  ["Key Account","Tier 1","Tier 2","Tier 3","Not Targeted","Untiered"].index(d["label"])
                           if d["label"] in ["Key Account","Tier 1","Tier 2","Tier 3","Not Targeted","Untiered"] else 99,
        })

    rows.sort(key=lambda x: x["sort_order"])
    return rows

# ─── COHORT DATA ──────────────────────────────────────────────────────────────
def build_cohort_data(ms):
    inv_month  = ms["inv_month"]
    all_months = ms["all_months"]

    # active[ym] = set of partner_ids with revenue that month
    active = {ym: set(partners.keys()) for ym, partners in inv_month.items()}

    # first appearance per partner
    first_month = {}
    for ym in sorted(all_months):
        for pid in inv_month.get(ym, {}):
            if pid not in first_month:
                first_month[pid] = ym

    rows = []
    prev_active = set()
    for ym in all_months:
        cur_active = active.get(ym, set())
        new_pids      = cur_active - prev_active
        retained_pids = cur_active & prev_active
        churned_pids  = prev_active - cur_active

        # Filter new: only those whose first_month == ym
        true_new = {p for p in new_pids if first_month.get(p) == ym}
        reactivated = new_pids - true_new

        retention_pct = (
            round(len(retained_pids) / len(prev_active) * 100, 1)
            if prev_active else None
        )

        rows.append({
            "month":         _ym_label(ym),
            "ym":            ym,
            "active":        len(cur_active),
            "new":           len(true_new),
            "retained":      len(retained_pids),
            "churned":       len(churned_pids),
            "retention_pct": retention_pct,
        })
        prev_active = cur_active

    return rows

# ─── EXECUTION TABLE ──────────────────────────────────────────────────────────
def build_execution_table(ms, all_partners, current_ym):
    inv_month = ms["inv_month"]
    inv_ytd   = ms["inv_ytd"]
    inv_ever  = ms["inv_ever"]   # set of partner_ids with any posted invoice ever
    vis_month = ms["vis_month"]

    month_inv = inv_month.get(current_ym, {})
    month_vis = vis_month.get(current_ym, {})

    # Build lookups from all_partners (includes x_studio_tier_1)
    norm_map     = {}   # normalized_name -> partner_id
    partner_tier = {}   # partner_id -> raw tier key from Odoo
    for p in all_partners:
        norm = p["name"].strip().lower()
        norm_map[norm] = p["id"]
        raw_tier = p.get("x_studio_tier_1") or None
        partner_tier[p["id"]] = raw_tier

    def find_partner(plan_name):
        norm_plan = plan_name.strip().lower()
        if norm_plan in norm_map:
            return norm_map[norm_plan]
        for norm_p, pid in norm_map.items():
            if norm_plan in norm_p or norm_p in norm_plan:
                return pid
        return None

    rows = []
    pen_corrections = []   # accounts marked NO in plan but have live Odoo invoices

    for plan_name, tier_num, plan_penetration, target_fy in TARGETED_ACCOUNTS:
        pid        = find_partner(plan_name)
        odoo_match = pid is not None

        # ── Live penetration from Odoo invoice history ─────────────────────
        if odoo_match:
            live_penetrated = pid in inv_ever
        else:
            live_penetrated = False

        if live_penetrated and plan_penetration == "NO":
            pen_corrections.append(plan_name)

        penetration = "Penetrated" if live_penetrated else "NO"

        # ── Live tier from Odoo, fallback to plan tier ────────────────────
        if odoo_match:
            raw_tier = partner_tier.get(pid)
            odoo_tier_label = TIER_LABELS.get(raw_tier) if raw_tier else None
            tier_label = odoo_tier_label or PLAN_TIER_LABELS.get(tier_num, f"Tier {tier_num}")
        else:
            tier_label = PLAN_TIER_LABELS.get(tier_num, f"Tier {tier_num}")

        # ── Activity for current month ─────────────────────────────────────
        visits_list  = month_vis.get(pid, []) if pid else []
        visited_mtd  = len(visits_list) > 0
        invoiced_mtd = pid in month_inv if pid else False
        revenue_mtd  = month_inv.get(pid, 0.0) if pid else 0.0
        revenue_ytd  = inv_ytd.get(pid, 0.0) if pid else 0.0
        last_visit   = max(visits_list).strftime("%Y-%m-%d") if visits_list else ""

        if not odoo_match:
            sort_group = 5
        elif visited_mtd and invoiced_mtd:
            sort_group = 1
        elif visited_mtd and not invoiced_mtd:
            sort_group = 2
        elif not visited_mtd and invoiced_mtd:
            sort_group = 3
        else:
            sort_group = 4

        rows.append({
            "plan_name":       plan_name,
            "tier":            tier_label,
            "penetration":     penetration,
            "target_fy":       target_fy,
            "odoo_partner_id": pid,
            "odoo_match":      odoo_match,
            "visited_mtd":     visited_mtd,
            "invoiced_mtd":    invoiced_mtd,
            "revenue_mtd":     revenue_mtd,
            "revenue_ytd":     revenue_ytd,
            "last_visit_date": last_visit,
            "visit_count_mtd": len(visits_list),
            "sort_group":      sort_group,
        })

    rows.sort(key=lambda r: (r["sort_group"], -r["revenue_ytd"]))

    matched   = sum(1 for r in rows if r["odoo_match"])
    unmatched = len(rows) - matched
    pen_yes   = sum(1 for r in rows if r["penetration"] == "Penetrated")
    print(f"[PLAN] {len(rows)} targeted accounts: {matched} matched in Odoo, {unmatched} not found")
    print(f"[PLAN] {pen_yes} accounts Penetrated (have sales) derived live from Odoo")
    if pen_corrections:
        print(f"[PLAN] {len(pen_corrections)} accounts corrected NO -> Penetrated (plan outdated):")
        for name in pen_corrections:
            print(f"       * {name}")
    return rows

# ─── RENDER DASHBOARD ─────────────────────────────────────────────────────────
def render_dashboard(ms, invoices, accounts, visits, all_partners, validation_summary):
    account_map          = ms["account_map"]
    coverage_denom       = ms["coverage_denom"]
    coverage_partner_ids = ms["coverage_partner_ids"]
    inv_month            = ms["inv_month"]
    vis_month            = ms["vis_month"]
    all_months           = ms["all_months"]

    if not all_months:
        sys.exit("[ERROR] No data found. Cannot render dashboard.")

    current_ym  = all_months[-1]
    prev_ym     = all_months[-2] if len(all_months) > 1 else None
    last_6      = all_months[-6:]

    # KPIs
    rev_mtd = sum(inv_month.get(current_ym, {}).values())
    rev_prev = sum(inv_month.get(prev_ym, {}).values()) if prev_ym else 0
    mom_pct  = ((rev_mtd - rev_prev) / rev_prev * 100) if rev_prev else None

    vis_cur           = vis_month.get(current_ym, {})
    visits_mtd        = sum(len(v) for v in vis_cur.values())
    accts_visited     = len(vis_cur)
    accts_revenue     = len(inv_month.get(current_ym, {}))

    # Coverage: numerator = visited partners that are in coverage-tier set
    visited_in_coverage = len(set(vis_cur.keys()) & coverage_partner_ids)
    cov_pct = round(visited_in_coverage / coverage_denom * 100, 1) if coverage_denom else 0

    # Chart data
    chart_months = [_ym_label(m) for m in last_6]
    chart_rev    = [round(sum(inv_month.get(m, {}).values()), 2) for m in last_6]
    chart_vis    = [sum(len(v) for v in vis_month.get(m, {}).values()) for m in last_6]
    chart_cov    = [
        round(len(set(vis_month.get(m, {}).keys()) & coverage_partner_ids) / coverage_denom * 100, 1)
        if coverage_denom else 0
        for m in last_6
    ]

    cohort_data  = build_cohort_data(ms)
    cohort_last6 = cohort_data[-6:] if len(cohort_data) > 6 else cohort_data
    chart_cohort_months   = [r["month"] for r in cohort_last6]
    chart_cohort_new      = [r["new"] for r in cohort_last6]
    chart_cohort_retained = [r["retained"] for r in cohort_last6]
    chart_cohort_churned  = [r["churned"] for r in cohort_last6]

    top_accounts   = build_top_accounts(ms, current_ym)
    conv_gap       = build_conversion_gap(ms, current_ym)
    tier_perf      = build_tier_performance(ms, accounts, current_ym)
    cohort_rows    = cohort_data
    exec_rows      = build_execution_table(ms, all_partners, current_ym)

    exec_visited_count  = sum(1 for r in exec_rows if r["visited_mtd"])
    exec_invoiced_count = sum(1 for r in exec_rows if r["invoiced_mtd"])

    # Timestamps
    now_amman = datetime.now(tz=TZ_AMMAN)
    refresh_ts = now_amman.strftime("%d %b %Y, %H:%M")

    mom_sign = "▲" if (mom_pct or 0) >= 0 else "▼"
    mom_color = "#16a34a" if (mom_pct or 0) >= 0 else "#dc2626"
    mom_str   = f"{mom_sign} {abs(mom_pct):.1f}%" if mom_pct is not None else "N/A"

    is_partial = now_amman.day < 25
    partial_badge = "<span class='partial-badge'>&#9888; Partial month</span>" if is_partial else ""

    def fmt_jod(v):
        return f"{v:,.2f}"

    def fmt_pct(v):
        return f"{v:.1f}%" if v is not None else "—"

    # ── Table HTML helpers ─────────────────────────────────────────────────
    def top_accounts_rows():
        rows = []
        for r in top_accounts:
            rows.append(
                f"<tr>"
                f"<td>{r['name']}</td>"
                f"<td><span class='tier-badge'>{r['tier']}</span></td>"
                f"<td class='num'>{fmt_jod(r['rev_mtd'])}</td>"
                f"<td class='num'>{fmt_jod(r['rev_ytd'])}</td>"
                f"<td class='num'>{r['visits_mtd']}</td>"
                f"<td>{r['last_visit'] or '—'}</td>"
                f"</tr>"
            )
        return "\n".join(rows) if rows else "<tr><td colspan='6'>No data</td></tr>"

    def conv_gap_rows():
        rows = []
        for r in conv_gap:
            rows.append(
                f"<tr>"
                f"<td>{r['name']}</td>"
                f"<td><span class='tier-badge'>{r['tier']}</span></td>"
                f"<td class='num'>{r['visits_mtd']}</td>"
                f"<td>{r['last_visit'] or '—'}</td>"
                f"</tr>"
            )
        return "\n".join(rows) if rows else "<tr><td colspan='4'>No conversion gap accounts</td></tr>"

    def tier_perf_rows():
        rows = []
        for r in tier_perf:
            cov = fmt_pct(r["coverage_pct"])
            rows.append(
                f"<tr>"
                f"<td><span class='tier-badge'>{r['tier']}</span></td>"
                f"<td class='num'>{r['n_accounts']}</td>"
                f"<td class='num'>{r['n_visited']}</td>"
                f"<td class='num'>{cov}</td>"
                f"<td class='num'>{fmt_jod(r['rev_mtd'])}</td>"
                f"<td class='num'>{fmt_jod(r['rev_ytd'])}</td>"
                f"</tr>"
            )
        return "\n".join(rows) if rows else "<tr><td colspan='6'>No data</td></tr>"

    def cohort_rows_html():
        rows = []
        for r in cohort_rows:
            ret = fmt_pct(r["retention_pct"])
            rows.append(
                f"<tr>"
                f"<td>{r['month']}</td>"
                f"<td class='num'>{r['active']}</td>"
                f"<td class='num'>{r['new']}</td>"
                f"<td class='num'>{r['retained']}</td>"
                f"<td class='num'>{r['churned']}</td>"
                f"<td class='num'>{ret}</td>"
                f"</tr>"
            )
        return "\n".join(rows) if rows else "<tr><td colspan='6'>No data</td></tr>"

    def exec_table_rows():
        ROW_COLORS = {
            1: "background:#f0fdf4",   # visited + invoiced -> light green
            2: "background:#fefce8",   # visited only       -> light yellow
            3: "background:#eff6ff",   # invoiced only      -> light blue
            4: "background:#ffffff",   # no activity        -> white
            5: "background:#f9fafb",   # not in Odoo        -> light gray
        }
        out = []
        for i, r in enumerate(exec_rows, 1):
            style    = ROW_COLORS.get(r["sort_group"], "")
            vis_icon = "<span class='check-yes'>&#10003;</span>" if r["visited_mtd"]  else "<span class='check-no'>&#8212;</span>"
            inv_icon = "<span class='check-yes'>&#10003;</span>" if r["invoiced_mtd"] else "<span class='check-no'>&#8212;</span>"
            odoo_col = "<span class='odoo-yes'>&#10003;</span>"  if r["odoo_match"]   else "<span class='odoo-no'>?</span>"
            pen_cls  = "pen-yes" if r["penetration"] == "Penetrated" else "pen-no"
            pen_lbl  = r["penetration"] if r["penetration"] == "Penetrated" else "NO"
            target   = f"{r['target_fy']:,}" if r["target_fy"] else "&#8212;"
            rev_mtd  = fmt_jod(r["revenue_mtd"]) if r["revenue_mtd"] else "&#8212;"
            rev_ytd  = fmt_jod(r["revenue_ytd"]) if r["revenue_ytd"] else "&#8212;"
            lv       = r["last_visit_date"] or "&#8212;"
            out.append(
                f"<tr style='{style}'>"
                f"<td class='num-sm'>{i}</td>"
                f"<td>{r['plan_name']}</td>"
                f"<td><span class='tier-badge'>{r['tier']}</span></td>"
                f"<td><span class='pen-badge {pen_cls}'>{pen_lbl}</span></td>"
                f"<td class='num-sm'>{target}</td>"
                f"<td style='text-align:center'>{vis_icon}</td>"
                f"<td style='text-align:center'>{inv_icon}</td>"
                f"<td class='num-sm'>{rev_mtd}</td>"
                f"<td class='num-sm'>{rev_ytd}</td>"
                f"<td>{lv}</td>"
                f"<td style='text-align:center'>{odoo_col}</td>"
                f"</tr>"
            )
        return "\n".join(out) if out else "<tr><td colspan='11'>No data</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Moataz Performance Dashboard — NESTU Jordan</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --blue:     #3040C4;
    --blue-lt:  #e8eaf6;
    --bg:       #f8f9fc;
    --card:     #ffffff;
    --border:   #e2e5f1;
    --text:     #1a1f36;
    --muted:    #6b7280;
    --green:    #16a34a;
    --red:      #dc2626;
  }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); font-size: 14px; }}
  header {{ background: var(--blue); color: #fff; padding: 18px 28px; display: flex; align-items: center; justify-content: space-between; }}
  header h1 {{ font-size: 1.25rem; font-weight: 700; letter-spacing: .02em; }}
  header p  {{ font-size: .8rem; opacity: .8; margin-top: 2px; }}
  .main {{ max-width: 1400px; margin: 0 auto; padding: 24px 20px; }}

  /* KPI cards */
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-bottom: 24px; }}
  .kpi-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px; }}
  .kpi-card .label {{ font-size: .72rem; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin-bottom: 6px; }}
  .kpi-card .value {{ font-size: 1.55rem; font-weight: 700; color: var(--blue); line-height: 1.1; }}
  .kpi-card .sub   {{ font-size: .78rem; margin-top: 4px; }}

  /* Charts */
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }}
  @media (max-width: 900px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
  .chart-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 18px; }}
  .chart-card h3 {{ font-size: .85rem; font-weight: 600; color: var(--muted); margin-bottom: 12px; text-transform: uppercase; letter-spacing: .05em; }}
  .chart-card canvas {{ max-height: 220px; }}

  /* Tables */
  .section {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; margin-bottom: 20px; overflow: hidden; }}
  .section-header {{ background: var(--blue); color: #fff; padding: 12px 18px; font-size: .88rem; font-weight: 600; letter-spacing: .04em; display: flex; align-items: center; gap: 8px; }}
  .section-header .badge {{ background: rgba(255,255,255,.2); border-radius: 99px; padding: 2px 8px; font-size: .75rem; }}
  .table-wrap {{ overflow-x: auto; max-height: 420px; overflow-y: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: .82rem; }}
  thead th {{ position: sticky; top: 0; background: var(--blue-lt); color: var(--blue); font-weight: 600; padding: 9px 12px; text-align: left; white-space: nowrap; cursor: pointer; user-select: none; }}
  thead th:hover {{ background: #d0d5f5; }}
  thead th.num, td.num {{ text-align: right; }}
  tbody tr:nth-child(even) {{ background: var(--bg); }}
  tbody tr:hover {{ background: #eef0fb; }}
  tbody td {{ padding: 8px 12px; border-bottom: 1px solid var(--border); }}

  .tier-badge {{ display: inline-block; padding: 2px 7px; border-radius: 99px; font-size: .7rem; font-weight: 600;
    background: var(--blue-lt); color: var(--blue); white-space: nowrap; }}
  .partial-badge {{ display: inline-block; margin-left: 5px; padding: 1px 6px; border-radius: 99px; font-size: .65rem;
    font-weight: 600; background: #fef3c7; color: #92400e; border: 1px solid #fcd34d; white-space: nowrap;
    vertical-align: middle; }}

  /* Execution table row colouring — applied via inline style on <tr> */
  .exec-wrap {{ max-height: 600px; overflow-y: auto; overflow-x: auto; }}
  .check-yes {{ color: #16a34a; font-weight: 700; }}
  .check-no  {{ color: #dc2626; }}
  .odoo-yes  {{ color: #16a34a; font-weight: 700; text-align: center; }}
  .odoo-no   {{ color: #d97706; font-weight: 700; text-align: center; }}
  .pen-badge {{ display: inline-block; padding: 1px 6px; border-radius: 99px; font-size: .68rem; font-weight: 600; }}
  .pen-yes   {{ background: #dcfce7; color: #166534; }}
  .pen-no    {{ background: #fee2e2; color: #991b1b; }}
  .num-sm    {{ text-align: right; }}

  footer {{ text-align: center; color: var(--muted); font-size: .75rem; padding: 24px 0 32px; }}
</style>
</head>
<body>
<header>
  <div>
    <h1>Moataz Alazzeh — Sales Performance Dashboard</h1>
    <p>NESTU Jordan · Company ID 2 · Odoo 17</p>
  </div>
  <div style="text-align:right;font-size:.8rem;opacity:.85;">
    Current Month: <strong>{_ym_label(current_ym)}</strong>{partial_badge}
  </div>
</header>

<div class="main">

<!-- KPI HEADER -->
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="label">Revenue MTD{partial_badge}</div>
    <div class="value">{fmt_jod(rev_mtd)}</div>
    <div class="sub" style="color:var(--muted)">JOD</div>
  </div>
  <div class="kpi-card">
    <div class="label">MoM Change{partial_badge}</div>
    <div class="value" style="color:{mom_color}">{mom_str}</div>
    <div class="sub" style="color:var(--muted)">vs {_ym_label(prev_ym) if prev_ym else '—'}</div>
  </div>
  <div class="kpi-card">
    <div class="label">Coverage %{partial_badge}</div>
    <div class="value">{cov_pct}%</div>
    <div class="sub" style="color:var(--muted)">{visited_in_coverage} / {coverage_denom} coverage-tier accounts</div>
  </div>
  <div class="kpi-card">
    <div class="label">Accounts Visited MTD{partial_badge}</div>
    <div class="value">{accts_visited}</div>
    <div class="sub" style="color:var(--muted)">unique accounts</div>
  </div>
  <div class="kpi-card">
    <div class="label">Accounts w/ Revenue{partial_badge}</div>
    <div class="value">{accts_revenue}</div>
    <div class="sub" style="color:var(--muted)">posted invoices</div>
  </div>
  <div class="kpi-card">
    <div class="label">Visits Logged MTD{partial_badge}</div>
    <div class="value">{visits_mtd}</div>
    <div class="sub" style="color:var(--muted)">total visits</div>
  </div>
</div>

<!-- EXECUTION TABLE -->
<div class="section">
  <div class="section-header">
    Targeted Account Execution &mdash; {_ym_label(current_ym)}
    <span class="badge">{exec_visited_count}/95 visited</span>
    <span class="badge">{exec_invoiced_count}/95 invoiced</span>
    <span style="font-size:.75rem;opacity:.75;margin-left:4px">95 accounts from the official plan | Matched to live Odoo data</span>
  </div>
  <div class="exec-wrap">
    <table id="tblExec">
      <thead>
        <tr>
          <th class="num" onclick="sortTable('tblExec',0,true)">#</th>
          <th onclick="sortTable('tblExec',1)">Account Name</th>
          <th onclick="sortTable('tblExec',2)">Tier</th>
          <th onclick="sortTable('tblExec',3)">Penetration</th>
          <th class="num" onclick="sortTable('tblExec',4,true)">Target FY (JOD)</th>
          <th style="text-align:center" onclick="sortTable('tblExec',5)">Visited MTD</th>
          <th style="text-align:center" onclick="sortTable('tblExec',6)">Invoiced MTD</th>
          <th class="num" onclick="sortTable('tblExec',7,true)">Revenue MTD (JOD)</th>
          <th class="num" onclick="sortTable('tblExec',8,true)">Revenue YTD (JOD)</th>
          <th onclick="sortTable('tblExec',9)">Last Visit</th>
          <th style="text-align:center" onclick="sortTable('tblExec',10)">Odoo</th>
        </tr>
      </thead>
      <tbody>
{exec_table_rows()}
      </tbody>
    </table>
  </div>
</div>

<!-- CHARTS -->
<div class="chart-grid">
  <div class="chart-card">
    <h3>Monthly Revenue Trend (JOD)</h3>
    <canvas id="chartRev"></canvas>
  </div>
  <div class="chart-card">
    <h3>Monthly Visits Trend</h3>
    <canvas id="chartVis"></canvas>
  </div>
  <div class="chart-card">
    <h3>Coverage % Trend</h3>
    <canvas id="chartCov"></canvas>
  </div>
  <div class="chart-card">
    <h3>Activation &amp; Retention</h3>
    <canvas id="chartCohort"></canvas>
  </div>
</div>

<!-- TABLE A: Top Accounts -->
<div class="section">
  <div class="section-header">
    Top Accounts by Revenue MTD
    <span class="badge">{_ym_label(current_ym)}</span>
  </div>
  <div class="table-wrap">
    <table id="tblA">
      <thead>
        <tr>
          <th onclick="sortTable('tblA',0)">Account Name</th>
          <th onclick="sortTable('tblA',1)">Tier</th>
          <th class="num" onclick="sortTable('tblA',2,true)">Revenue MTD (JOD)</th>
          <th class="num" onclick="sortTable('tblA',3,true)">Revenue YTD (JOD)</th>
          <th class="num" onclick="sortTable('tblA',4,true)">Visits MTD</th>
          <th onclick="sortTable('tblA',5)">Last Visit</th>
        </tr>
      </thead>
      <tbody>
{top_accounts_rows()}
      </tbody>
    </table>
  </div>
</div>

<!-- TABLE B: Conversion Gap -->
<div class="section">
  <div class="section-header">
    Conversion Gap — Visited but No Invoice
    <span class="badge">{_ym_label(current_ym)}</span>
  </div>
  <div class="table-wrap">
    <table id="tblB">
      <thead>
        <tr>
          <th onclick="sortTable('tblB',0)">Account Name</th>
          <th onclick="sortTable('tblB',1)">Tier</th>
          <th class="num" onclick="sortTable('tblB',2,true)">Visits MTD</th>
          <th onclick="sortTable('tblB',3)">Last Visit</th>
        </tr>
      </thead>
      <tbody>
{conv_gap_rows()}
      </tbody>
    </table>
  </div>
</div>

<!-- TABLE C: Tier Performance -->
<div class="section">
  <div class="section-header">
    Tier Performance Summary
    <span class="badge">{_ym_label(current_ym)}</span>
  </div>
  <div class="table-wrap">
    <table id="tblC">
      <thead>
        <tr>
          <th onclick="sortTable('tblC',0)">Tier</th>
          <th class="num" onclick="sortTable('tblC',1,true)"># Accounts</th>
          <th class="num" onclick="sortTable('tblC',2,true)"># Visited MTD</th>
          <th class="num" onclick="sortTable('tblC',3,true)">Coverage %</th>
          <th class="num" onclick="sortTable('tblC',4,true)">Revenue MTD (JOD)</th>
          <th class="num" onclick="sortTable('tblC',5,true)">Revenue YTD (JOD)</th>
        </tr>
      </thead>
      <tbody>
{tier_perf_rows()}
      </tbody>
    </table>
  </div>
</div>

<!-- TABLE D: Cohort -->
<div class="section">
  <div class="section-header">Cohort — Activation &amp; Retention</div>
  <div class="table-wrap">
    <table id="tblD">
      <thead>
        <tr>
          <th onclick="sortTable('tblD',0)">Month</th>
          <th class="num" onclick="sortTable('tblD',1,true)">Active Accounts</th>
          <th class="num" onclick="sortTable('tblD',2,true)">New</th>
          <th class="num" onclick="sortTable('tblD',3,true)">Retained</th>
          <th class="num" onclick="sortTable('tblD',4,true)">Churned</th>
          <th class="num" onclick="sortTable('tblD',5,true)">Retention %</th>
        </tr>
      </thead>
      <tbody>
{cohort_rows_html()}
      </tbody>
    </table>
  </div>
</div>

</div><!-- /.main -->

<footer>
  Last refreshed: {refresh_ts} Asia/Amman &nbsp;|&nbsp; Source: Odoo NESTU Jordan (company_id=2)
</footer>

<script>
// ── Chart.js setup ─────────────────────────────────────────────────────────
const BLUE   = '#3040C4';
const BLUE2  = '#6875dc';
const GREEN  = '#16a34a';
const RED    = '#dc2626';
const GRAY   = '#9ca3af';

const labels6   = {json.dumps(chart_months)};
const dataRev   = {json.dumps(chart_rev)};
const dataVis   = {json.dumps(chart_vis)};
const dataCov   = {json.dumps(chart_cov)};
const cLabels   = {json.dumps(chart_cohort_months)};
const cNew      = {json.dumps(chart_cohort_new)};
const cRetained = {json.dumps(chart_cohort_retained)};
const cChurned  = {json.dumps(chart_cohort_churned)};

Chart.defaults.font.family = 'system-ui, sans-serif';
Chart.defaults.font.size   = 12;

new Chart(document.getElementById('chartRev'), {{
  type: 'bar',
  data: {{
    labels: labels6,
    datasets: [{{ label: 'Revenue (JOD)', data: dataRev, backgroundColor: BLUE, borderRadius: 4 }}]
  }},
  options: {{
    plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, ticks: {{ callback: v => v.toLocaleString() }} }} }}
  }}
}});

new Chart(document.getElementById('chartVis'), {{
  type: 'line',
  data: {{
    labels: labels6,
    datasets: [{{ label: 'Visits', data: dataVis, borderColor: BLUE, backgroundColor: 'rgba(48,64,196,.1)', tension: .35, fill: true, pointRadius: 4 }}]
  }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
}});

new Chart(document.getElementById('chartCov'), {{
  type: 'line',
  data: {{
    labels: labels6,
    datasets: [{{ label: 'Coverage %', data: dataCov, borderColor: GREEN, backgroundColor: 'rgba(22,163,74,.1)', tension: .35, fill: true, pointRadius: 4 }}]
  }},
  options: {{
    plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, max: 100, ticks: {{ callback: v => v + '%' }} }} }}
  }}
}});

new Chart(document.getElementById('chartCohort'), {{
  type: 'bar',
  data: {{
    labels: cLabels,
    datasets: [
      {{ label: 'New',      data: cNew,      backgroundColor: GREEN,  borderRadius: 3 }},
      {{ label: 'Retained', data: cRetained, backgroundColor: BLUE,   borderRadius: 3 }},
      {{ label: 'Churned',  data: cChurned,  backgroundColor: RED,    borderRadius: 3 }},
    ]
  }},
  options: {{
    plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ x: {{ stacked: true }}, y: {{ stacked: false, beginAtZero: true }} }}
  }}
}});

// ── Sort tables ─────────────────────────────────────────────────────────────
const sortState = {{}};
function sortTable(id, col, numeric=false) {{
  const tbl  = document.getElementById(id);
  const tbody = tbl.querySelector('tbody');
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  const key   = id + '_' + col;
  sortState[key] = !sortState[key];
  const asc = sortState[key];
  rows.sort((a, b) => {{
    const av = a.cells[col]?.innerText.replace(/[,%]/g, '').trim() || '';
    const bv = b.cells[col]?.innerText.replace(/[,%]/g, '').trim() || '';
    if (numeric) return asc ? (parseFloat(av)||0) - (parseFloat(bv)||0)
                            : (parseFloat(bv)||0) - (parseFloat(av)||0);
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  }});
  rows.forEach(r => tbody.appendChild(r));
  // update header indicators
  tbl.querySelectorAll('thead th').forEach((th,i) => {{
    th.textContent = th.textContent.replace(/ [▲▼]$/,'');
    if (i === col) th.textContent += asc ? ' ▲' : ' ▼';
  }});
}}
</script>
</body>
</html>"""

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OUTPUT] Dashboard written -> {out_path}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Moataz Performance Dashboard — starting")
    print("=" * 60)

    models, uid = connect_odoo()

    invoices    = fetch_invoices(models, uid)
    accounts    = fetch_accounts(models, uid)
    visits      = fetch_visits(models, uid)
    all_partners = fetch_all_partners(models, uid)

    print("-" * 40)
    validation_summary = validate_data(invoices, accounts, visits)

    print("-" * 40)
    ms = build_monthly_structures(invoices, accounts, visits)
    print(f"[BUILD] {len(ms['all_months'])} months of data: {ms['all_months'][0] if ms['all_months'] else 'none'} -> {ms['all_months'][-1] if ms['all_months'] else 'none'}")
    print(f"[BUILD] Coverage denominator: {ms['coverage_denom']} accounts (tiers KA/1/2/3)")

    print("-" * 40)
    render_dashboard(ms, invoices, accounts, visits, all_partners, validation_summary)

    print("=" * 60)
    print("Done. Open dashboard.html in a browser.")
    print("=" * 60)

    # FUTURE: load_targets(month) -> monthly targets per account from Odoo or CSV
    # FUTURE: route_adherence(month) -> planned vs actual visits from routing plan
    # FUTURE: margin_analysis(invoice_ids) -> requires cost field access

if __name__ == "__main__":
    main()
