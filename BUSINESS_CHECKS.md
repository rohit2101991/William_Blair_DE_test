# Business checks: non-negative columns and stakeholder ideas

## Assessment data note (recheck vs instructions)

The take-home **instructions** state that the CSVs were sourced from multiple systems and **intentionally** include issues (nulls, referential breaks, date logic, duplicates, casing, etc.).

On the **five files shipped in this repository** (`data/*.csv`, same checksums as `~/Downloads/Engineer Take Home Assesment/`), we **re-ran a broad automated audit** (FKs, duplicate keys, negative amounts, `deal_year` vs `announce_date`, `deal_quarter` vs announce month, percentile ordering in sector multiples, CSV column counts per row, ID format patterns, string sentinels like `NULL`/`NaN`, closed-without-close, days-to-close vs dates, sponsor rows in financials, duplicate fiscal grain, etc.). **No violations of those classes were detected** in this extract.

**Conclusion:** Either (a) this copy is a **clean / regenerated** teaching subset while the rubric text describes the **general** expectation, or (b) remaining issues are **too subtle** for these probes alone. The pipeline still implements **detection and handling paths** (quarantine, asset checks, business rules) so that when messy rows appear, they surface in Dagster rather than failing silently.

---

## 1. Columns where “null or ≥ 0” is implemented (staging asset checks)

Rule of thumb: **counts, deal sizes, reported revenue/EBITDA/cash/assets, market cap, and headline multiples** are usually non-negative in this dataset’s units. **Margins, growth, net debt,** and **some strategic fields** can legitimately be negative or are not simple sign checks.

| Raw file | Column | Why non-negative? | Implemented check |
|----------|--------|-------------------|---------------------|
| `acquirers.csv` | `employee_count` | Headcount cannot be negative. | `stg_acquirers_non_negative_metrics` |
| `acquirers.csv` | `annual_revenue_mm` | Revenue in $MM is reported as a non-negative magnitude. | same |
| `targets.csv` | `employee_count` | Headcount cannot be negative. | `stg_targets_non_negative_metrics` |
| `targets.csv` | `ltm_revenue_mm` | LTM revenue in $MM as non-negative magnitude. | same |
| `transactions.csv` | `deal_size_mm` | Enterprise value in $MM (also quarantined if &lt; 0 in staging). | `stg_transactions_non_negative_metrics` |
| `transactions.csv` | `ev_ebitda_multiple` | Multiple as reported; negative would be non-physical for this use case. | same |
| `transactions.csv` | `ev_revenue_multiple` | Same. | same |
| `transactions.csv` | `synergy_pct_of_deal` | Synergy as % of deal; negative synergy treated as invalid for this rule. | same |
| `transactions.csv` | `num_bidders` | Count of bidders cannot be negative. | same |
| `transactions.csv` | `days_to_close` | Business days from announce to close cannot be negative. | same |
| `acquirer_financials.csv` | `employee_count` | Year-end headcount. | `stg_acquirer_financials_non_negative_metrics` |
| `acquirer_financials.csv` | `revenue_mm` | Annual revenue $MM. | same |
| `acquirer_financials.csv` | `ebitda_mm` | EBITDA $MM (losses sometimes modeled differently; here non-negative). | same |
| `acquirer_financials.csv` | `cash_on_hand_mm` | Cash position $MM. | same |
| `acquirer_financials.csv` | `total_assets_mm` | Total assets $MM. | same |
| `acquirer_financials.csv` | `market_cap_mm` | Market cap when present. | same |
| `sector_multiples.csv` | `median_ev_ebitda`, `p25_ev_ebitda`, `p75_ev_ebitda` | Published comp multiples for the sector. | `stg_sector_multiples_non_negative_metrics` |
| `sector_multiples.csv` | `median_ev_revenue`, `p25_ev_revenue`, `p75_ev_revenue` | Same. | same |
| `sector_multiples.csv` | `deal_count` | Count of comps in the bucket cannot be negative. | same |

**Code:** `william_blair_de/assets/business_checks.py` (loaded next to `checks.py` in `definitions.py`).

---

## 2. Raw columns where a simple “≥ 0” rule is **not** applied (and why)

| Raw file | Column | Reason |
|----------|--------|--------|
| `acquirers.csv` | `founded_date` | Date, not a signed magnitude. |
| `acquirers.csv` | `acquirer_name`, `acquirer_type`, `primary_sector_focus`, `headquarters`, `ticker_or_status`, `geographic_reach` | Categorical / text. |
| `targets.csv` | `ebitda_margin_pct` | Can be negative (loss-making). |
| `targets.csv` | `revenue_growth_pct` | Can be negative (shrinking revenue). |
| `targets.csv` | `target_name`, `sector`, `sub_sector`, `geography`, `ownership_type`, `founded_date` | Text / date. |
| `transactions.csv` | `announce_date`, `close_date`, `deal_year`, `deal_quarter`, `deal_type`, `financing_type`, `outcome`, `strategic_rationale_tags`, `transaction_id`, `target_id`, `acquirer_id` | Dates, enums, keys, tags. |
| `acquirer_financials.csv` | `net_debt_mm` | Often ≥ 0 but **can be negative** (net cash / negative net debt). |
| `acquirer_financials.csv` | `ebitda_margin_pct` | Can be negative. |
| `acquirer_financials.csv` | `fiscal_year` | Integer year; better as **range** check (e.g. 1990–2035) than sign. |
| `sector_multiples.csv` | `sector`, `fiscal_year`, `fiscal_quarter` | Dimensions, not non-negative magnitudes. |

**Optional future checks (not coded):** `fiscal_year` in a plausible window; `synergy_pct_of_deal` ≤ 100 if synergies are interpreted as capped at deal value.

---

## 3. Stakeholder-style “business sense” checks (cross-field / semantic)

These go beyond null/type/sign: they test **whether combinations make real-world sense**. Examples are illustrative; some need reference tables or parsed addresses.

| Idea | Example | What you need |
|------|---------|----------------|
| **Geography consistency** | Country = India but “state” = California | Parse `headquarters` / `geography` into country vs region, or join to a **geo reference** table (country → valid subdivisions). |
| **Deal timing vs labels** | `outcome = Closed` but `close_date` null | Already covered by asset check `stg_transactions_closed_requires_close_date`. |
| **Calendar vs fiscal** | `deal_year` ≠ year(`announce_date`) | Compare `deal_year` to calendar year of announce (allow Q4 fiscal quirks with a tolerance rule). |
| **Strategic vs financials** | `acquirer_type = Financial Sponsor` but rows exist in `acquirer_financials` | Data dictionary says financials are strategic-only; flag sponsor IDs in financials. |
| **Multiple vs deal type** | `Minority Investment` but EV/EBITDA looks like full control | Reference matrix of **expected multiple ranges by deal_type** (soft bounds, WARN not ERROR). |
| **Sector / industry chain** | `sub_sector` not typical for `sector` | Controlled vocabulary or parent-child sector tree. |
| **Size vs employee count** | Tiny `employee_count` but huge `ltm_revenue_mm` | Ratio bands by industry (needs benchmarks). |
| **Closed deal math** | `days_to_close` inconsistent with `announce_date` and `close_date` | Recompute business days and compare to column within tolerance. |
| **Financing vs deal type** | `All Equity` financing on an LBO | Business matrix of allowed `financing_type` × `deal_type`. |
| **Ticker vs public** | `ticker_or_status = Private` but `market_cap_mm` populated | Cross-column rules on `acquirers` / financials. |

These are strong **IB / data product** differentiators but require **domain rules**, **reference data**, or **parsing**—implement as separate assets or checks when rules are agreed with stakeholders.
