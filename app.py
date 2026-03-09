# -*- coding: utf-8 -*-
"""
Flask TRY NIM Dashboard

Features
- Multi-source Excel selection via dropdown
- Per-source available date dropdowns
- 4 charts:
    1) NIM Waterfall
    2) Pricing Drivers
    3) Weight Changes (same drivers/order as Economic Mix)
    4) Economic Mix Drivers
- Economic Mix benchmark:
    * Assets benchmark   = weighted avg rate of asset-side detail universe
    * Liabilities bench  = weighted avg rate of liability-side detail universe

Run
    pip install -r requirements.txt
    python app.py
    ...ttt
"""

import json
import re
from typing import Optional, Tuple, Dict, Literal, Set, List

import numpy as np
import pandas as pd
from flask import Flask, render_template, request
import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder


# =========================
# Constants
# =========================
DAYS = 30.0
DAY_COUNT = 360.0
DCF = DAYS / DAY_COUNT
ANNUALIZE = 1.0 / DCF  # 12

DecompMethod = Literal["start_rate_end_bal", "midpoint"]
RepoPickSide = Literal["Liabilities_first", "Assets_first", "Liabilities_only", "Assets_only"]

CURRENCY_RE = re.compile(r"(?i)^\s*total\s+(.+?)\s+book\s*$")


# =========================
# Data sources (edit paths)
# =========================
DATA_SOURCES: Dict[str, str] = {
    "Simulation Scenario 1": "insert_data_scn1.xlsx",
    "Simulation Scenario 2": "insert_data_scn2.xlsx",
    "Realized NII": "insert_data_realized.xlsx",
    "Comparison": "insert_data_comparison.xlsx",
}
DEFAULT_SOURCE = "Simulation Scenario 1"
SHEET_NAME = 0


# =========================
# Dashboard config
# =========================
TITLE_PREFIX = "TRY"
TOP_N_WF2 = 5
TOP_N_WF3 = 5  # kept for backward compatibility; not used
TOP_N_WF4 = 5
REPO_PREFER: RepoPickSide = "Liabilities_first"  # unused now, kept for compatibility
REPO_PRODUCT_NAME = "Repurchase agreements"      # unused now, kept for compatibility
Y_MIN_FLOOR = 350
Y_MIN_SPAN = 80
Y_PAD_RATIO = 0.15

ASSETS_DETAIL_ITEMS = [
    "Banks",
    "Investment securities",
    "Personal Need",
    "Commercial Finance",
    "Auto",
    "Mortgage",
    "Credit cards",
    "Overdrafts",
    "Other Cash loans",
    "Other Assets",
    "FX Swap",
    "IRS",
    "CCS",
    "Trade Swaps",
]

LIAB_DETAIL_ITEMS = [
    "Repurchase agreements",
    "Bank deposits",
    "-Customer time deposits",
    "-Customer demand deposits",
    "-Bond Issued",
    "Funds borrowed",
    "Other Liabilities & Equity",
    "FX Swap",
    "IRS",
    "CCS",
    "Trade Swaps",
]

# FX NIM detail universe (product names must match Excel)
FX_ASSETS_DETAIL_ITEMS = [
    "Banks",
    "Investment securities",
    "Loans in installments",
    "Cash loans",
    "Other Assets",
    "FX Swap",
    "CCS",
    "IRS",
    "Trade Swaps",
    "Hedge & Liquidity Swaps",
]

FX_LIAB_DETAIL_ITEMS = [
    "Bank deposits",
    "-Customer time deposits",
    "-Customer demand deposits",
    "Bonds Issued",
    "Whole Sale Funding",
    "Repurchase agreements",
    "Other Liabilities",
    "-FX Swap",
    "-CCS",
    "-IRS",
    "IRS",
    "Trade Swaps",
    "Hedge & Liquidity Swaps",
]


# =========================
# Helpers
# =========================
def _wavg(x: pd.Series, w: pd.Series) -> float:
    x = x.astype(float)
    w = w.astype(float)
    s = w.sum()
    if s == 0 or np.isnan(s):
        return float(x.mean())
    return float((x * w).sum() / s)


def _bps(x_decimal: float) -> int:
    return int(round(float(x_decimal) * 10000.0))


def _fmt_int(x: float) -> str:
    return f"{int(round(float(x))):,}"


def _pick_col(df: pd.DataFrame, preferred: str, fallback: str) -> str:
    if preferred in df.columns:
        return preferred
    if fallback in df.columns:
        return fallback
    raise ValueError(f"Kolon bulunamadı: '{preferred}' veya '{fallback}'")


def _auto_y_range(values_bps, pad_ratio=0.15, min_span=80, min_floor=None):
    v = np.asarray([float(x) for x in values_bps], dtype=float)
    vmin, vmax = float(np.nanmin(v)), float(np.nanmax(v))
    span = max(vmax - vmin, float(min_span))
    pad = span * float(pad_ratio)
    y0, y1 = vmin - pad, vmax + pad
    if min_floor is not None:
        y0 = max(y0, float(min_floor))
    if y1 <= y0:
        y0, y1 = vmin - pad, vmax + pad
    return [y0, y1]


def _date_str(dt: pd.Timestamp) -> str:
    return pd.Timestamp(dt).strftime("%Y-%m-%d")


def _split_total_and_detail(contrib_df: pd.DataFrame, assets_detail_items, liab_detail_items):
    df = contrib_df.copy()
    df["BS_TYPE"] = df["BS_TYPE"].astype(str).str.strip()
    df["PRODUCT_NAME"] = df["PRODUCT_NAME"].astype(str).str.strip()

    total_mask = (
        df["PRODUCT_NAME"].str.lower().isin(["total try book", "total fx book"])
        & df["BS_TYPE"].isin(["Assets", "Liabilities"])
    )
    df_total = df.loc[total_mask].copy().reset_index(drop=True)

    aset = {str(x).strip().lower() for x in assets_detail_items}
    lset = {str(x).strip().lower() for x in liab_detail_items}

    detail_mask = (
        (df["BS_TYPE"].eq("Assets") & df["PRODUCT_NAME"].str.lower().isin(aset))
        | (df["BS_TYPE"].eq("Liabilities") & df["PRODUCT_NAME"].str.lower().isin(lset))
    )
    df_detail = df.loc[detail_mask].copy().reset_index(drop=True)
    return df_total, df_detail


# =========================
# 1) Excel Reader
# =========================
class InsertDataExcelReader:
    @staticmethod
    def _is_date_like(x) -> bool:
        if pd.isna(x):
            return False
        if isinstance(x, pd.Timestamp):
            return True
        try:
            pd.to_datetime(x, errors="raise")
            return True
        except Exception:
            return False

    @staticmethod
    def _metric_to_std(metric_cell) -> Optional[str]:
        if pd.isna(metric_cell):
            return None
        s = str(metric_cell).strip().upper()
        if "BAL" in s:
            return "BALANCE"
        if "YIELD" in s or "RATE" in s:
            return "INTEREST_RATE"
        return None

    @staticmethod
    def _to_float_series(s: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(s):
            return s.astype(float)

        x = s.astype(str)
        x = (
            x.str.replace("\u00A0", " ", regex=False)
             .str.strip()
             .str.replace("'", "", regex=False)
             .str.replace(" ", "", regex=False)
        )

        both = x.str.contains(r"\.", regex=True) & x.str.contains(",", regex=False)
        x.loc[both] = x.loc[both].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

        only_comma = x.str.contains(",", regex=False) & ~x.str.contains(r"\.", regex=True)
        x.loc[only_comma] = x.loc[only_comma].str.replace(",", ".", regex=False)

        return pd.to_numeric(x, errors="coerce")

    @classmethod
    def _build_mappings(cls, date_row: pd.Series, metric_row: pd.Series, start_col: int):
        mappings = []
        current_date = None

        ncols = len(date_row)
        for offset in range(ncols):
            col_idx = start_col + offset
            dcell = date_row.iloc[offset]
            if cls._is_date_like(dcell):
                current_date = pd.to_datetime(dcell, errors="coerce")

            metric_std = cls._metric_to_std(metric_row.iloc[offset])
            if metric_std is None:
                continue
            if current_date is None or pd.isna(current_date):
                continue

            mappings.append((col_idx, current_date, metric_std))
        return mappings

    @classmethod
    def read_insert_data(cls, path: str, sheet_name=0) -> pd.DataFrame:
        raw = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")

        records = []
        current_bs_type = None
        current_currency = None
        current_mappings = None

        i = 0
        while i < raw.shape[0]:
            a = raw.iat[i, 0]
            a_str = "" if pd.isna(a) else str(a).strip()

            row_dates_full = raw.iloc[i, 1:]
            is_date_header = sum(cls._is_date_like(v) for v in row_dates_full.values) >= 1

            if is_date_header and (i + 1) < raw.shape[0]:
                metric_row_full = raw.iloc[i + 1, 1:]
                metric_hits = sum(cls._metric_to_std(v) is not None for v in metric_row_full.values)

                if metric_hits >= 2:
                    bs = raw.iat[i + 1, 0]
                    current_bs_type = None if pd.isna(bs) else str(bs).strip()

                    start_offset = None
                    for j, v in enumerate(row_dates_full.values):
                        if cls._is_date_like(v):
                            start_offset = j
                            break
                    if start_offset is None:
                        start_offset = 0

                    date_row = row_dates_full.iloc[start_offset:]
                    metric_row = metric_row_full.iloc[start_offset:]
                    start_col = 1 + start_offset
                    current_mappings = cls._build_mappings(date_row, metric_row, start_col=start_col)
                    i += 2
                    continue

            if current_mappings is None or not a_str:
                i += 1
                continue

            m = CURRENCY_RE.match(a_str)
            if m:
                current_currency = m.group(1).strip()

            product_name = a_str
            per_date: Dict[pd.Timestamp, Dict[str, object]] = {}
            for col_idx, sim_date, metric_std in current_mappings:
                if col_idx >= raw.shape[1]:
                    continue
                per_date.setdefault(sim_date, {})[metric_std] = raw.iat[i, col_idx]

            for sim_date, mm in per_date.items():
                records.append({
                    "CURRENCY": current_currency,
                    "BS_TYPE": current_bs_type,
                    "PRODUCT_NAME": product_name,
                    "SIM_DATE": sim_date,
                    "BALANCE": mm.get("BALANCE"),
                    "INTEREST_RATE": mm.get("INTEREST_RATE"),
                })
            i += 1

        df = pd.DataFrame(records)
        if df.empty:
            return df

        df["PRODUCT_NAME"] = (
            df["PRODUCT_NAME"].astype(str).str.replace("\u00A0", " ", regex=False).str.strip()
        )
        df["SIM_DATE"] = pd.to_datetime(df["SIM_DATE"], errors="coerce")
        df["BALANCE"] = cls._to_float_series(df["BALANCE"])
        df["INTEREST_RATE"] = cls._to_float_series(df["INTEREST_RATE"])
        df = df.dropna(subset=["BALANCE", "INTEREST_RATE"], how="all")
        return df


# =========================
# 2) Decomposition Engine
# =========================
class NIMDecompositionEngine:
    EXCLUDE_DEFAULT = {"TRY NIM", "FX NIM", "NII (excluding other NII) - 30 day adj"}

    @staticmethod
    def _compute_approx_try_nim_from_detail(df: pd.DataFrame, d0: pd.Timestamp, d1: pd.Timestamp) -> Dict[str, float]:
        """
        Fallback when explicit 'TRY NIM' row is missing in the Excel.
        Approximate TRY NIM by aggregating TRY Assets/Liabilities detail rows.
        """
        df2 = df.copy()
        if "SIM_DATE_DT" not in df2.columns:
            df2["SIM_DATE_DT"] = NIMDecompositionEngine.parse_sim_date(df2["SIM_DATE"])

        mask = (
            df2["CURRENCY"].astype(str).str.upper().str.strip().eq("TRY")
            & df2["BS_TYPE"].astype(str).str.strip().isin(["Assets", "Liabilities"])
            & df2["SIM_DATE_DT"].isin([d0, d1])
        )
        tmp = df2.loc[mask, ["BS_TYPE", "BALANCE", "INTEREST_RATE", "SIM_DATE_DT"]].dropna(
            subset=["BALANCE", "INTEREST_RATE"], how="any"
        )
        if tmp.empty:
            raise ValueError("TRY NIM hesaplanamadı: TRY Assets/Liabilities detay verisi yok.")

        def _nim_for_date(dt: pd.Timestamp) -> float:
            t = tmp.loc[tmp["SIM_DATE_DT"] == dt]
            if t.empty:
                raise ValueError(f"TRY NIM hesaplanamadı: {dt.date()} için TRY detay verisi yok.")
            sign = np.where(t["BS_TYPE"].astype(str).str.strip().eq("Assets"), 1.0, -1.0)
            bal = t["BALANCE"].astype(float).to_numpy()
            rate = t["INTEREST_RATE"].astype(float).to_numpy()
            nii = float((sign * bal * rate * DCF).sum())

            denom = NIMDecompositionEngine.get_total_try_assets_balance(df2, dt)
            if denom == 0:
                raise ValueError(f"TRY NIM hesaplanamadı: {dt.date()} için denominator 0.")
            nim = (nii / float(denom)) * ANNUALIZE
            return float(nim)

        nim0 = _nim_for_date(d0)
        nim1 = _nim_for_date(d1)
        return {
            "nim_start": nim0,
            "nim_end": nim1,
            "nim_change": nim1 - nim0,
            "nim_source": "computed_from_detail",
        }

    @staticmethod
    def parse_sim_date(s: pd.Series) -> pd.Series:
        dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
        if dt.isna().any():
            dt = dt.fillna(pd.to_datetime(s, errors="coerce", format="%d/%m/%Y"))
        if dt.isna().any():
            dt = dt.fillna(pd.to_datetime(s, errors="coerce", dayfirst=True))
        return dt

    @staticmethod
    def get_reported_try_nim(df: pd.DataFrame, d0: pd.Timestamp, d1: pd.Timestamp) -> Dict[str, float]:
        name = (
            df["PRODUCT_NAME"].astype(str).str.replace("\u00A0", " ", regex=False).str.strip().str.upper()
        )
        mask = name.eq("TRY NIM") & df["SIM_DATE_DT"].isin([d0, d1])
        tmp = df.loc[mask, ["SIM_DATE_DT", "INTEREST_RATE"]].dropna(subset=["INTEREST_RATE"])

        s0 = tmp.loc[tmp["SIM_DATE_DT"] == d0, "INTEREST_RATE"]
        s1 = tmp.loc[tmp["SIM_DATE_DT"] == d1, "INTEREST_RATE"]

        if not s0.empty and not s1.empty:
            nim0, nim1 = float(s0.iloc[0]), float(s1.iloc[0])
            return {
                "nim_start": nim0,
                "nim_end": nim1,
                "nim_change": nim1 - nim0,
                "nim_source": "reported_row",
            }

        # Fallback: compute TRY NIM directly from TRY Assets/Liabilities detail
        return NIMDecompositionEngine._compute_approx_try_nim_from_detail(df, d0, d1)

    @staticmethod
    def get_total_try_assets_balance(df: pd.DataFrame, dt: pd.Timestamp) -> float:
        mask = (
            df["CURRENCY"].astype(str).str.upper().str.strip().eq("TRY")
            & df["BS_TYPE"].astype(str).str.strip().eq("Assets")
            & df["PRODUCT_NAME"].astype(str).str.strip().eq("Total TRY book")
            & (df["SIM_DATE_DT"] == dt)
        )
        tmp = df.loc[mask, "BALANCE"].dropna()
        if tmp.empty:
            cand = df[
                df["CURRENCY"].astype(str).str.upper().str.strip().eq("TRY")
                & df["BS_TYPE"].astype(str).str.strip().eq("Assets")
                & (df["SIM_DATE_DT"] == dt)
            ][["PRODUCT_NAME", "BALANCE", "INTEREST_RATE"]].copy()
            raise ValueError(
                f"Denominator için TRY/Assets/'Total TRY book' BALANCE bulunamadı: {dt.date()}\n"
                f"Bu tarihte TRY/Assets ürün örnekleri (ilk 15):\n{cand.head(15).to_string(index=False)}"
            )
        return float(tmp.iloc[0])

    @staticmethod
    def get_reported_fx_nim(df: pd.DataFrame, d0: pd.Timestamp, d1: pd.Timestamp) -> Dict[str, float]:
        """
        Read 'FX NIM' row from the Excel (already parsed into df).
        """
        name = (
            df["PRODUCT_NAME"].astype(str).str.replace("\u00A0", " ", regex=False).str.strip().str.upper()
        )
        mask = name.eq("FX NIM") & df["SIM_DATE_DT"].isin([d0, d1])
        tmp = df.loc[mask, ["SIM_DATE_DT", "INTEREST_RATE"]].dropna(subset=["INTEREST_RATE"])

        s0 = tmp.loc[tmp["SIM_DATE_DT"] == d0, "INTEREST_RATE"]
        s1 = tmp.loc[tmp["SIM_DATE_DT"] == d1, "INTEREST_RATE"]

        if not s0.empty and not s1.empty:
            nim0, nim1 = float(s0.iloc[0]), float(s1.iloc[0])
            return {
                "nim_start": nim0,
                "nim_end": nim1,
                "nim_change": nim1 - nim0,
                "nim_source": "reported_row",
            }

        raise ValueError("Reported FX NIM satırı bulunamadı (PRODUCT_NAME == 'FX NIM').")

    @staticmethod
    def get_total_fx_assets_balance(df: pd.DataFrame, dt: pd.Timestamp) -> float:
        """
        Total FX Assets balance from 'Total FX book' row, for FX NIM denominator.
        """
        mask = (
            df["CURRENCY"].astype(str).str.upper().str.strip().eq("FX")
            & df["BS_TYPE"].astype(str).str.strip().eq("Assets")
            & df["PRODUCT_NAME"].astype(str).str.strip().eq("Total FX book")
            & (df["SIM_DATE_DT"] == dt)
        )
        tmp = df.loc[mask, "BALANCE"].dropna()
        if tmp.empty:
            cand = df[
                df["CURRENCY"].astype(str).str.upper().str.strip().eq("FX")
                & df["BS_TYPE"].astype(str).str.strip().eq("Assets")
                & (df["SIM_DATE_DT"] == dt)
            ][["PRODUCT_NAME", "BALANCE", "INTEREST_RATE"]].copy()
            raise ValueError(
                f"Denominator için FX/Assets/'Total FX book' BALANCE bulunamadı: {dt.date()}\n"
                f"Bu tarihte FX/Assets ürün örnekleri (ilk 15):\n{cand.head(15).to_string(index=False)}"
            )
        return float(tmp.iloc[0])

    @classmethod
    def decompose_try_nim_change(
        cls,
        df: pd.DataFrame,
        date_0: str,
        date_1: str,
        *,
        decomp_method: DecompMethod = "midpoint",
        exclude_products: Optional[Set[str]] = None,
    ) -> Tuple[Dict[str, float], pd.DataFrame]:
        df = df.copy()
        df["SIM_DATE_DT"] = cls.parse_sim_date(df["SIM_DATE"])

        d0 = pd.to_datetime(date_0)
        d1 = pd.to_datetime(date_1)

        nim_info = cls.get_reported_try_nim(df, d0, d1)
        reported_delta = float(nim_info["nim_change"])

        denom_start = cls.get_total_try_assets_balance(df, d0)
        denom_end = cls.get_total_try_assets_balance(df, d1)
        if denom_end == 0:
            raise ValueError("Denominator (end_total_try_assets) 0 olamaz.")

        df2 = df[
            df["CURRENCY"].astype(str).str.upper().str.strip().eq("TRY")
            & df["BS_TYPE"].astype(str).str.strip().isin(["Assets", "Liabilities"])
            & df["SIM_DATE_DT"].isin([d0, d1])
        ].copy()
        if df2.empty:
            raise ValueError("Seçilen tarihlerde TRY (Assets/Liabilities) kalem verisi yok.")

        exclude = set(exclude_products or set()) | set(cls.EXCLUDE_DEFAULT)
        df2["PRODUCT_NAME"] = df2["PRODUCT_NAME"].astype(str).str.strip()
        df2["BS_TYPE"] = df2["BS_TYPE"].astype(str).str.strip()
        df2 = df2[~df2["PRODUCT_NAME"].isin(exclude)].copy()

        key = ["BS_TYPE", "PRODUCT_NAME"]
        wide = (
            df2.groupby(key + ["SIM_DATE_DT"], as_index=False)[["BALANCE", "INTEREST_RATE"]]
            .sum()
            .pivot_table(index=key, columns="SIM_DATE_DT", values=["BALANCE", "INTEREST_RATE"], aggfunc="sum")
        )
        wide.columns = [f"{m}_{pd.Timestamp(dt).strftime('%Y-%m-%d')}" for (m, dt) in wide.columns]
        wide = wide.reset_index()

        bal0_col = f"BALANCE_{d0.strftime('%Y-%m-%d')}"
        bal1_col = f"BALANCE_{d1.strftime('%Y-%m-%d')}"
        r0_col = f"INTEREST_RATE_{d0.strftime('%Y-%m-%d')}"
        r1_col = f"INTEREST_RATE_{d1.strftime('%Y-%m-%d')}"

        for c in [bal0_col, bal1_col, r0_col, r1_col]:
            if c not in wide.columns:
                wide[c] = 0.0
        wide[[bal0_col, bal1_col, r0_col, r1_col]] = wide[[bal0_col, bal1_col, r0_col, r1_col]].fillna(0.0)

        sign = np.where(wide["BS_TYPE"].eq("Assets"), 1.0, -1.0)
        b0 = wide[bal0_col].astype(float).to_numpy()
        b1 = wide[bal1_col].astype(float).to_numpy()
        r0 = wide[r0_col].astype(float).to_numpy()
        r1 = wide[r1_col].astype(float).to_numpy()

        nii0 = sign * b0 * r0 * DCF
        nii1 = sign * b1 * r1 * DCF
        d_nii = nii1 - nii0

        if decomp_method == "start_rate_end_bal":
            denom = denom_start
            bal_eff = sign * (b1 - b0) * r0 * DCF
            rate_eff = sign * b1 * (r1 - r0) * DCF
        elif decomp_method == "midpoint":
            avg_r = 0.5 * (r0 + r1)
            avg_b = 0.5 * (b0 + b1)
            bal_eff = sign * (b1 - b0) * avg_r * DCF
            rate_eff = sign * (r1 - r0) * avg_b * DCF
            denom = (denom_start + denom_end) / 2
        else:
            raise ValueError(f"Unknown decomp_method: {decomp_method}")

        residual = d_nii - (bal_eff + rate_eff)

        out = wide[["BS_TYPE", "PRODUCT_NAME"]].copy()
        out["BALANCE_t0"] = b0
        out["BALANCE_t1"] = b1
        out["RATE_t0"] = r0
        out["RATE_t1"] = r1

        out["dNII_total"] = d_nii
        out["dNII_balance_effect"] = bal_eff
        out["dNII_rate_effect"] = rate_eff
        out["dNII_residual"] = residual

        out["dNIM_total_raw"] = (out["dNII_total"] / denom) * ANNUALIZE
        out["dNIM_balance_raw"] = (out["dNII_balance_effect"] / denom) * ANNUALIZE
        out["dNIM_rate_raw"] = (out["dNII_rate_effect"] / denom) * ANNUALIZE
        out["dNIM_residual_raw"] = (out["dNII_residual"] / denom) * ANNUALIZE

        mask_a = (out["BS_TYPE"] == "Assets") & (out["PRODUCT_NAME"] == "Total TRY book")
        mask_l = (out["BS_TYPE"] == "Liabilities") & (out["PRODUCT_NAME"] == "Total TRY book")
        calc_delta_raw = float(out.loc[mask_a, "dNIM_total_raw"].sum() + out.loc[mask_l, "dNIM_total_raw"].sum())

        k = 1
        out["scale_k"] = k
        out["dNIM_total"] = out["dNIM_total_raw"] * k
        out["dNIM_balance"] = out["dNIM_balance_raw"] * k
        out["dNIM_rate"] = out["dNIM_rate_raw"] * k
        out["dNIM_residual"] = out["dNIM_residual_raw"] * k

        explained = float(out["dNIM_total"].sum())
        nim_info = dict(nim_info)
        nim_info.update({
            "denom_end_total_try_assets": float(denom_end),
            "annualize_factor": float(ANNUALIZE),
            "calculated_delta_raw": float(calc_delta_raw),
            "scale_k_delta": float(k) if not (isinstance(k, float) and np.isnan(k)) else k,
            "explained_delta_scaled": float(explained),
            "recon_gap_after_scaling": float(reported_delta - explained),
        })

        out = out.sort_values("dNIM_total", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
        return nim_info, out

    @classmethod
    def decompose_fx_nim_change(
        cls,
        df: pd.DataFrame,
        date_0: str,
        date_1: str,
        *,
        decomp_method: DecompMethod = "midpoint",
        exclude_products: Optional[Set[str]] = None,
    ) -> Tuple[Dict[str, float], pd.DataFrame]:
        """
        FX NIM decomposition, analogous to TRY NIM decomposition but using FX data.
        """
        df = df.copy()
        df["SIM_DATE_DT"] = cls.parse_sim_date(df["SIM_DATE"])

        d0 = pd.to_datetime(date_0)
        d1 = pd.to_datetime(date_1)

        nim_info = cls.get_reported_fx_nim(df, d0, d1)
        reported_delta = float(nim_info["nim_change"])

        denom_start = cls.get_total_fx_assets_balance(df, d0)
        denom_end = cls.get_total_fx_assets_balance(df, d1)
        if denom_end == 0:
            raise ValueError("FX denominator (end_total_fx_assets) 0 olamaz.")

        df2 = df[
            df["CURRENCY"].astype(str).str.upper().str.strip().eq("FX")
            & df["BS_TYPE"].astype(str).str.strip().isin(["Assets", "Liabilities"])
            & df["SIM_DATE_DT"].isin([d0, d1])
        ].copy()
        if df2.empty:
            raise ValueError("Seçilen tarihlerde FX (Assets/Liabilities) kalem verisi yok.")

        exclude = set(exclude_products or set()) | set(cls.EXCLUDE_DEFAULT)
        df2["PRODUCT_NAME"] = df2["PRODUCT_NAME"].astype(str).str.strip()
        df2["BS_TYPE"] = df2["BS_TYPE"].astype(str).str.strip()
        df2 = df2[~df2["PRODUCT_NAME"].isin(exclude)].copy()

        key = ["BS_TYPE", "PRODUCT_NAME"]
        wide = (
            df2.groupby(key + ["SIM_DATE_DT"], as_index=False)[["BALANCE", "INTEREST_RATE"]]
            .sum()
            .pivot_table(index=key, columns="SIM_DATE_DT", values=["BALANCE", "INTEREST_RATE"], aggfunc="sum")
        )
        wide.columns = [f"{m}_{pd.Timestamp(dt).strftime('%Y-%m-%d')}" for (m, dt) in wide.columns]
        wide = wide.reset_index()

        bal0_col = f"BALANCE_{d0.strftime('%Y-%m-%d')}"
        bal1_col = f"BALANCE_{d1.strftime('%Y-%m-%d')}"
        r0_col = f"INTEREST_RATE_{d0.strftime('%Y-%m-%d')}"
        r1_col = f"INTEREST_RATE_{d1.strftime('%Y-%m-%d')}"

        for c in [bal0_col, bal1_col, r0_col, r1_col]:
            if c not in wide.columns:
                wide[c] = 0.0
        wide[[bal0_col, bal1_col, r0_col, r1_col]] = wide[[bal0_col, bal1_col, r0_col, r1_col]].fillna(0.0)

        sign = np.where(wide["BS_TYPE"].eq("Assets"), 1.0, -1.0)
        b0 = wide[bal0_col].astype(float).to_numpy()
        b1 = wide[bal1_col].astype(float).to_numpy()
        r0 = wide[r0_col].astype(float).to_numpy()
        r1 = wide[r1_col].astype(float).to_numpy()

        nii0 = sign * b0 * r0 * DCF
        nii1 = sign * b1 * r1 * DCF
        d_nii = nii1 - nii0

        if decomp_method == "start_rate_end_bal":
            denom = denom_start
            bal_eff = sign * (b1 - b0) * r0 * DCF
            rate_eff = sign * b1 * (r1 - r0) * DCF
        elif decomp_method == "midpoint":
            avg_r = 0.5 * (r0 + r1)
            avg_b = 0.5 * (b0 + b1)
            bal_eff = sign * (b1 - b0) * avg_r * DCF
            rate_eff = sign * (r1 - r0) * avg_b * DCF
            denom = (denom_start + denom_end) / 2
        else:
            raise ValueError(f"Unknown decomp_method: {decomp_method}")

        residual = d_nii - (bal_eff + rate_eff)

        out = wide[["BS_TYPE", "PRODUCT_NAME"]].copy()
        out["BALANCE_t0"] = b0
        out["BALANCE_t1"] = b1
        out["RATE_t0"] = r0
        out["RATE_t1"] = r1

        out["dNII_total"] = d_nii
        out["dNII_balance_effect"] = bal_eff
        out["dNII_rate_effect"] = rate_eff
        out["dNII_residual"] = residual

        out["dNIM_total_raw"] = (out["dNII_total"] / denom) * ANNUALIZE
        out["dNIM_balance_raw"] = (out["dNII_balance_effect"] / denom) * ANNUALIZE
        out["dNIM_rate_raw"] = (out["dNII_rate_effect"] / denom) * ANNUALIZE
        out["dNIM_residual_raw"] = (out["dNII_residual"] / denom) * ANNUALIZE

        mask_a = (out["BS_TYPE"] == "Assets") & (out["PRODUCT_NAME"] == "Total FX book")
        mask_l = (out["BS_TYPE"] == "Liabilities") & (out["PRODUCT_NAME"] == "Total FX book")
        calc_delta_raw = float(out.loc[mask_a, "dNIM_total_raw"].sum() + out.loc[mask_l, "dNIM_total_raw"].sum())

        k = 1
        out["scale_k"] = k
        out["dNIM_total"] = out["dNIM_total_raw"] * k
        out["dNIM_balance"] = out["dNIM_balance_raw"] * k
        out["dNIM_rate"] = out["dNIM_rate_raw"] * k
        out["dNIM_residual"] = out["dNIM_residual_raw"] * k

        explained = float(out["dNIM_total"].sum())
        nim_info = dict(nim_info)
        nim_info.update({
            "denom_end_total_fx_assets": float(denom_end),
            "annualize_factor": float(ANNUALIZE),
            "calculated_delta_raw": float(calc_delta_raw),
            "scale_k_delta": float(k) if not (isinstance(k, float) and np.isnan(k)) else k,
            "explained_delta_scaled": float(explained),
            "recon_gap_after_scaling": float(reported_delta - explained),
        })

        out = out.sort_values("dNIM_total", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
        return nim_info, out

    @staticmethod
    def mix_contrib_weights(df_detail: pd.DataFrame) -> pd.DataFrame:
        df = df_detail.copy()
        df["BS_TYPE"] = df["BS_TYPE"].astype(str).str.strip()
        df["PRODUCT_NAME"] = df["PRODUCT_NAME"].astype(str).str.strip()
        df = df[~df["PRODUCT_NAME"].isin(["Total TRY book", "Total FX book"])].copy()

        b0 = df["BALANCE_t0"].astype(float)
        b1 = df["BALANCE_t1"].astype(float)

        tot_a0 = b0[df["BS_TYPE"].eq("Assets")].sum() or np.nan
        tot_a1 = b1[df["BS_TYPE"].eq("Assets")].sum() or np.nan
        tot_l0 = b0[df["BS_TYPE"].eq("Liabilities")].sum() or np.nan
        tot_l1 = b1[df["BS_TYPE"].eq("Liabilities")].sum() or np.nan

        df["w0"] = np.where(df["BS_TYPE"].eq("Assets"), b0 / tot_a0, b0 / tot_l0)
        df["w1"] = np.where(df["BS_TYPE"].eq("Assets"), b1 / tot_a1, b1 / tot_l1)
        df["dw"] = (df["w1"] - df["w0"]).astype(float)

        sign = np.where(df["BS_TYPE"].eq("Assets"), 1.0, -1.0)
        df["avg_rate"] = 0.5 * (df["RATE_t0"].astype(float) + df["RATE_t1"].astype(float))
        df["avg_balance"] = 0.5 * (df["BALANCE_t0"].astype(float) + df["BALANCE_t1"].astype(float))
        df["d_balance"] = (df["BALANCE_t1"].astype(float) - df["BALANCE_t0"].astype(float))

        df["mix_bps_raw"] = (sign * df["dw"] * df["avg_rate"] * 10000.0).astype(float)
        df["label"] = df["BS_TYPE"] + " | " + df["PRODUCT_NAME"]
        return df[["label", "mix_bps_raw", "d_balance", "avg_rate", "avg_balance", "dw", "BS_TYPE", "PRODUCT_NAME"]].copy()

    @staticmethod
    def repo_benchmark_mix(df_detail: pd.DataFrame, repo_product_name: str, repo_prefer: RepoPickSide) -> pd.DataFrame:
        """
        Economic mix benchmark:
          - Assets benchmark  = weighted avg rate of Assets side (detail universe)
          - Liab benchmark    = weighted avg rate of Liabilities side (detail universe)

        spread:
          - Assets      : avg_rate - bench_asset
          - Liabilities : bench_liab - avg_rate

        repo_mix_bps_raw (name kept for backward compatibility):
          dw * spread * 10000
        """
        df = df_detail.copy()
        df["BS_TYPE"] = df["BS_TYPE"].astype(str).str.strip()
        df["PRODUCT_NAME"] = df["PRODUCT_NAME"].astype(str).str.strip()
        df = df[~df["PRODUCT_NAME"].isin(["Total TRY book", "Total FX book"])].copy()

        req = {"BALANCE_t0", "BALANCE_t1", "RATE_t0", "RATE_t1"}
        miss = req - set(df.columns)
        if miss:
            raise ValueError(f"Economic mix için eksik kolonlar: {sorted(miss)}")

        b0 = df["BALANCE_t0"].astype(float)
        b1 = df["BALANCE_t1"].astype(float)

        tot_a0 = b0[df["BS_TYPE"].eq("Assets")].sum() or np.nan
        tot_a1 = b1[df["BS_TYPE"].eq("Assets")].sum() or np.nan
        tot_l0 = b0[df["BS_TYPE"].eq("Liabilities")].sum() or np.nan
        tot_l1 = b1[df["BS_TYPE"].eq("Liabilities")].sum() or np.nan

        df["w0"] = np.where(df["BS_TYPE"].eq("Assets"), b0 / tot_a0, b0 / tot_l0)
        df["w1"] = np.where(df["BS_TYPE"].eq("Assets"), b1 / tot_a1, b1 / tot_l1)
        df["dw"] = (df["w1"] - df["w0"]).astype(float)

        df["avg_rate"] = 0.5 * (df["RATE_t0"].astype(float) + df["RATE_t1"].astype(float))
        df["avg_balance"] = 0.5 * (df["BALANCE_t0"].astype(float) + df["BALANCE_t1"].astype(float))
        df["d_balance"] = (df["BALANCE_t1"].astype(float) - df["BALANCE_t0"].astype(float))

        a = df[df["BS_TYPE"].eq("Assets")].copy()
        l = df[df["BS_TYPE"].eq("Liabilities")].copy()

        if a.empty:
            raise ValueError("Economic mix benchmark için Assets tarafında detail satırı yok.")
        if l.empty:
            raise ValueError("Economic mix benchmark için Liabilities tarafında detail satırı yok.")

        bench_asset = _wavg(a["avg_rate"], a["avg_balance"])
        bench_liab = _wavg(l["avg_rate"], l["avg_balance"])

        df["repo_rate_avg"] = np.where(df["BS_TYPE"].eq("Assets"), bench_asset, bench_liab)
        df["spread"] = np.where(
            df["BS_TYPE"].eq("Assets"),
            df["avg_rate"] - bench_asset,
            bench_liab - df["avg_rate"],
        )

        df["repo_mix_bps_raw"] = (df["dw"] * df["spread"] * 10000.0).astype(float)
        df["label"] = df["BS_TYPE"] + " | " + df["PRODUCT_NAME"]

        out = df.loc[:, [
            "label", "repo_mix_bps_raw", "d_balance", "avg_rate", "avg_balance", "dw",
            "spread", "repo_rate_avg", "BS_TYPE", "PRODUCT_NAME"
        ]].copy()
        return out


# =========================
# 3) Plotter
# =========================
class NIMWaterfallPlotter:
    @staticmethod
    def _waterfall(fig_title: str, x, y, measures, customdata, hovertemplate, y_min_floor, y_min_span, y_pad_ratio):
        fig = go.Figure(
            go.Waterfall(
                measure=measures,
                x=x,
                y=y,
                customdata=customdata,
                hovertemplate=hovertemplate,
                connector={"line": {"width": 1}},
            )
        )
        fig.update_layout(title=fig_title, yaxis_title="bps", showlegend=False)
        fig.update_yaxes(range=_auto_y_range(y, pad_ratio=y_pad_ratio, min_span=y_min_span, min_floor=y_min_floor))
        return fig

    @classmethod
    def plot_all(
        cls,
        nim_info: dict,
        df_total: pd.DataFrame,
        df_detail: pd.DataFrame,
        *,
        title_prefix: str = "TRY",
        top_n_wf2: int = 5,
        top_n_wf3: int = 5,   # backward compatibility; not used
        top_n_wf4: int = 5,
        repo_prefer: RepoPickSide = "Liabilities_first",
        repo_product_name: str = "Repurchase agreements",
        y_min_floor: int = 350,
        y_min_span: int = 80,
        y_pad_ratio: float = 0.15,
    ):
        det_rate_col = _pick_col(df_detail, "dNIM_rate", "dNIM_rate_raw")

        start_nim = float(nim_info["nim_start"])
        end_nim = float(nim_info["nim_end"])

        pricing_decimal = float(df_detail[det_rate_col].sum())
        mix_decimal = (end_nim - start_nim) - pricing_decimal

        all_bps_values = [_bps(start_nim), _bps(end_nim), _bps(start_nim + mix_decimal),]
        y_min_floor = min(all_bps_values) - 30

        # WF1
        wf1_x = ["Start NIM", "Mix / Interaction", "Pricing (rate, detailed)", "End NIM"]
        wf1_measures = ["absolute", "relative", "relative", "total"]
        wf1_y = [_bps(start_nim), _bps(mix_decimal), _bps(pricing_decimal), _bps(end_nim)]

        level = []
        run = None
        for m, v in zip(wf1_measures, wf1_y):
            if m == "absolute":
                run = int(v)
            elif m == "relative":
                run = int(run + int(v))
            else:
                run = int(v)
            level.append(int(run))

        total_change = level[-1] - level[0]
        custom1 = []
        for i, name in enumerate(wf1_x):
            if name == "End NIM":
                custom1.append([f"<br>NIM Level: {level[i]} bps<br>Start→End ΔNIM: {total_change:+d} bps"])
            elif wf1_measures[i] == "relative":
                custom1.append([f"<br>Start Level: {level[i-1]} bps<br>End Level: {level[i]} bps<br>Δ (bar): {wf1_y[i]:+d} bps"])
            else:
                custom1.append([f"<br>NIM Level: {level[i]} bps"])

        hover1 = "<b>%{x}</b><br>%{y} bps%{customdata[0]}<extra></extra>"
        fig1 = cls._waterfall(
            f"{title_prefix} NIM Waterfall (bps): Mix vs Pricing",
            wf1_x, wf1_y, wf1_measures, custom1, hover1,
            y_min_floor=y_min_floor, y_min_span=y_min_span, y_pad_ratio=y_pad_ratio
        )

        # WF2
        det = df_detail.copy()
        det["label"] = det["BS_TYPE"].astype(str).str.strip() + " | " + det["PRODUCT_NAME"].astype(str).str.strip()
        det["rate_contrib_bps"] = det[det_rate_col].astype(float) * 10000.0

        rows = []
        for lbl, g in det.groupby("label"):
            contrib = float(g["rate_contrib_bps"].sum())
            r0 = _wavg(g["RATE_t0"], g["BALANCE_t0"])
            r1 = _wavg(g["RATE_t1"], g["BALANCE_t1"])
            bal0 = float(g["BALANCE_t0"].sum())
            bal1 = float(g["BALANCE_t1"].sum())
            avg_bal = 0.5 * (bal0 + bal1)
            rows.append({
                "label": lbl,
                "rate_contrib_bps": contrib,
                "drate_bps": int(round((r1 - r0) * 10000.0)),
                "avg_balance": avg_bal
            })

        det_g = pd.DataFrame(rows).sort_values("rate_contrib_bps", key=lambda s: s.abs(), ascending=False)
        top2 = det_g.head(int(top_n_wf2)).copy()
        other2 = det_g.iloc[int(top_n_wf2):]
        other_sum2 = float(other2["rate_contrib_bps"].sum())
        other_avg_bal2 = float(other2["avg_balance"].dropna().sum()) if not other2.empty else np.nan

        baseline2_nim = start_nim + mix_decimal
        baseline2_bps = _bps(baseline2_nim)
        end2_bps = _bps(end_nim)
        seg2 = end2_bps - baseline2_bps

        wf2_x = ["After Mix"] + top2["label"].tolist() + ["Other Items", "End NIM"]
        wf2_y = [baseline2_bps] + [int(round(v)) for v in top2["rate_contrib_bps"]] + [int(round(other_sum2))] + [end2_bps]
        wf2_measures = ["absolute"] + ["relative"] * len(top2) + ["relative"] + ["total"]

        custom2 = [[f"<br>NIM Level: {baseline2_bps} bps", "", "", ""]]
        for _, r in top2.iterrows():
            custom2.append([
                "",
                f"<br>Contribution: {int(round(r['rate_contrib_bps'])):+d} bps",
                f"<br>ΔRate: {int(r['drate_bps']):+d} bps",
                f"<br>Avg Balance: {_fmt_int(r['avg_balance'])}"
            ])
        custom2.append([
            "",
            f"<br>Contribution: {int(round(other_sum2)):+d} bps",
            "",
            f"<br>Avg Balance: {_fmt_int(other_avg_bal2)}" if np.isfinite(other_avg_bal2) else ""
        ])
        custom2.append([f"<br>NIM Level: {end2_bps} bps<br>After Mix→End ΔNIM: {seg2:+d} bps", "", "", ""])

        hover2 = "<b>%{x}</b>%{customdata[0]}%{customdata[1]}%{customdata[2]}%{customdata[3]}<extra></extra>"
        fig2 = cls._waterfall(
            f"{title_prefix} NIM Pricing Drivers (Top {len(top2)} + Other, bps) — baseline = After Mix",
            wf2_x, wf2_y, wf2_measures, custom2, hover2,
            y_min_floor=y_min_floor, y_min_span=y_min_span, y_pad_ratio=y_pad_ratio
        )

        # WF4
        target_mix_bps = _bps(mix_decimal)
        repo_df = NIMDecompositionEngine.repo_benchmark_mix(df_detail, repo_product_name, repo_prefer)
        raw4 = float(repo_df["repo_mix_bps_raw"].sum())
        scale4 = 0.0 if abs(raw4) < 1e-12 else (float(target_mix_bps) / raw4)
        repo_df["repo_mix_bps"] = repo_df["repo_mix_bps_raw"] * scale4
        repo_df = repo_df.sort_values("repo_mix_bps", key=lambda s: s.abs(), ascending=False)

        top4 = repo_df.head(int(top_n_wf4)).copy()
        other4 = repo_df.iloc[int(top_n_wf4):]
        other_sum4 = float(other4["repo_mix_bps"].sum())
        other_dbal4 = float(other4["d_balance"].dropna().sum()) if not other4.empty else np.nan
        other_avgb4 = float(other4["avg_balance"].dropna().sum()) if not other4.empty else np.nan
        other_dw4 = float(other4["dw"].dropna().sum()) if not other4.empty else np.nan

        baseline4_bps = _bps(start_nim)
        end4_bps = _bps(start_nim + mix_decimal)
        seg4 = end4_bps - baseline4_bps

        wf4_x = ["Start NIM"] + top4["label"].tolist() + ["Other Items", "After Mix"]
        wf4_y = [baseline4_bps] + [int(round(v)) for v in top4["repo_mix_bps"]] + [int(round(other_sum4))] + [end4_bps]
        wf4_measures = ["absolute"] + ["relative"] * len(top4) + ["relative"] + ["total"]

        custom4 = [[f"<br>NIM Level: {baseline4_bps} bps", "", "", ""]]
        for _, r in top4.iterrows():
            custom4.append([
                "",
                f"<br>Contribution: {int(round(r['repo_mix_bps'])):+d} bps",
                f"<br>ΔBalance: {_fmt_int(r['d_balance'])}" if pd.notna(r["d_balance"]) else "",
                f"<br>Avg Balance: {_fmt_int(r['avg_balance'])}" if pd.notna(r["avg_balance"]) else "",
            ])
        custom4.append([
            "",
            f"<br>Contribution: {int(round(other_sum4)):+d} bps",
            f"<br>ΔBalance: {_fmt_int(other_dbal4)}" if np.isfinite(other_dbal4) else "",
            f"<br>Avg Balance: {_fmt_int(other_avgb4)}" if np.isfinite(other_avgb4) else "",
        ])
        custom4.append([f"<br>NIM Level: {end4_bps} bps<br>Start→After Mix ΔNIM: {seg4:+d} bps", "", "", ""])

        hover4 = "<b>%{x}</b>%{customdata[0]}%{customdata[1]}%{customdata[2]}%{customdata[3]}<extra></extra>"
        fig4 = cls._waterfall(
            f"{title_prefix} Economic Mix Drivers (Side benchmark, Top {len(top4)} + Other, bps) — baseline = Start",
            wf4_x, wf4_y, wf4_measures, custom4, hover4,
            y_min_floor=y_min_floor, y_min_span=y_min_span, y_pad_ratio=y_pad_ratio
        )

        # Weight Change chart
        weight_rows = []
        for _, r in top4.iterrows():
            bs = str(r.get("BS_TYPE", "")).strip()
            bench_label = "Asset avg. rate" if bs == "Assets" else ("Liability avg. rate" if bs == "Liabilities" else "Benchmark avg. rate")
            bench_rate = float(r.get("repo_rate_avg", np.nan))
            weight_rows.append({
                "label": r["label"],
                "dw_pct": float(r["dw"]) * 100.0,
                "avg_rate": float(r.get("avg_rate", np.nan)),
                "bench_label": bench_label,
                "bench_rate": bench_rate,
            })
        if np.isfinite(other_dw4):
            weight_rows.append({
                "label": "Other Items",
                "dw_pct": float(other_dw4) * 100.0,
                "avg_rate": np.nan,
                "bench_label": "Benchmark avg. rate",
                "bench_rate": np.nan,
            })
        weight_df = pd.DataFrame(weight_rows)

        dw_vals = weight_df["dw_pct"].astype(float).to_numpy()
        vmin = float(np.nanmin(dw_vals)) if len(dw_vals) else -1.0
        vmax = float(np.nanmax(dw_vals)) if len(dw_vals) else 1.0
        span = max(vmax - vmin, 0.5)
        pad = max(0.25, 0.25 * span)
        y0, y1 = vmin - pad, vmax + pad

        fig3 = go.Figure(
            go.Bar(
                x=weight_df["label"],
                y=weight_df["dw_pct"],
                text=[f"{v:+.2f}%" for v in weight_df["dw_pct"]],
                textposition="outside",
                customdata=weight_df[["avg_rate", "bench_label", "bench_rate"]].to_numpy(),
                hovertemplate=(
                    "<b>%{x}</b>"
                    "<br>ΔWeight: %{y:.2f}%"
                    "<br>Avg Rate: %{customdata[0]:.2%}"
                    "<br>%{customdata[1]}: %{customdata[2]:.2%}"
                    "<extra></extra>"
                ),
                marker_color=["green" if v > 0 else "red" for v in weight_df["dw_pct"]],
            )
        )
        fig3.update_layout(
            title=f"{title_prefix} Weight Changes (Top {len(top4)} + Other)",
            yaxis_title="Δ Weight (%)",
            xaxis_title="Product",
            showlegend=False,
            margin=dict(l=60, r=30, t=70, b=140),
            uniformtext_minsize=10,
            uniformtext_mode="hide",
        )
        fig3.update_yaxes(range=[y0, y1], automargin=True)
        fig3.update_xaxes(tickangle=45, automargin=True)
        fig3.update_traces(cliponaxis=False)

        return fig1, fig2, fig3, fig4


# =========================
# Flask app
# =========================
app = Flask(__name__)
DF_CACHE: Dict[str, pd.DataFrame] = {}
DATES_CACHE: Dict[str, List[str]] = {}


def _json_response(payload: dict, status: int = 200):
    return app.response_class(
        response=json.dumps(payload, cls=PlotlyJSONEncoder),
        status=status,
        mimetype="application/json",
    )


def _load_data_once(source_name: str):
    if source_name not in DATA_SOURCES:
        raise ValueError(f"Unknown data source: {source_name}")

    if source_name in DF_CACHE and source_name in DATES_CACHE:
        return

    path = DATA_SOURCES[source_name]
    df = InsertDataExcelReader.read_insert_data(path, sheet_name=SHEET_NAME)
    if df.empty:
        raise ValueError(f"Excel'den veri okunamadı (df boş): {source_name} -> {path}")

    df["SIM_DATE"] = pd.to_datetime(df["SIM_DATE"], errors="coerce")
    dates = sorted([d for d in df["SIM_DATE"].dropna().unique()])
    available_dates = [_date_str(pd.Timestamp(d)) for d in dates]

    DF_CACHE[source_name] = df
    DATES_CACHE[source_name] = available_dates


def _build_figs_for_dates(source_name: str, date_0: str, date_1: str, nim_type: str = "TRY"):
    _load_data_once(source_name)
    df_src = DF_CACHE[source_name]

    nim_type_norm = str(nim_type or "TRY").strip().upper()

    d0 = pd.to_datetime(date_0)
    d1 = pd.to_datetime(date_1)
    if d0 == d1:
        raise ValueError("date_0 ve date_1 aynı olamaz.")

    if nim_type_norm == "TRY":
        nim_info, contrib_df = NIMDecompositionEngine.decompose_try_nim_change(
            df=df_src,
            date_0=date_0,
            date_1=date_1,
            decomp_method="midpoint",
        )
        assets_items = ASSETS_DETAIL_ITEMS
        liab_items = LIAB_DETAIL_ITEMS
        title_prefix = "TRY"
    elif nim_type_norm == "FX":
        nim_info, contrib_df = NIMDecompositionEngine.decompose_fx_nim_change(
            df=df_src,
            date_0=date_0,
            date_1=date_1,
            decomp_method="midpoint",
        )
        assets_items = FX_ASSETS_DETAIL_ITEMS
        liab_items = FX_LIAB_DETAIL_ITEMS
        title_prefix = "FX"
    else:
        raise ValueError(f"Bilinmeyen NIM tipi: {nim_type}")

    df_total, df_detail = _split_total_and_detail(
        contrib_df,
        assets_detail_items=assets_items,
        liab_detail_items=liab_items,
    )

    fig1, fig2, fig3, fig4 = NIMWaterfallPlotter.plot_all(
        nim_info=nim_info,
        df_total=df_total,
        df_detail=df_detail,
        title_prefix=title_prefix,
        top_n_wf2=TOP_N_WF2,
        top_n_wf3=TOP_N_WF3,
        top_n_wf4=TOP_N_WF4,
        repo_prefer=REPO_PREFER,
        repo_product_name=REPO_PRODUCT_NAME,
        y_min_floor=Y_MIN_FLOOR,
        y_min_span=Y_MIN_SPAN,
        y_pad_ratio=Y_PAD_RATIO,
    )
    return nim_info, fig1, fig2, fig3, fig4


@app.route("/")
def index():
    _load_data_once(DEFAULT_SOURCE)
    available_dates = DATES_CACHE[DEFAULT_SOURCE]

    if len(available_dates) >= 2:
        d0 = available_dates[-2]
        d1 = available_dates[-1]
    elif len(available_dates) == 1:
        d0 = available_dates[0]
        d1 = available_dates[0]
    else:
        d0, d1 = "", ""

    return render_template(
        "index.html",
        data_sources=list(DATA_SOURCES.keys()),
        default_source=DEFAULT_SOURCE,
        available_dates=available_dates,
        default_date_0=d0,
        default_date_1=d1,
        title_prefix=TITLE_PREFIX,
    )


@app.route("/api/dates", methods=["GET"])
def api_dates():
    source = request.args.get("source", DEFAULT_SOURCE).strip()
    try:
        _load_data_once(source)
        return _json_response({"ok": True, "dates": DATES_CACHE[source]})
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, status=500)


@app.route("/api/waterfalls", methods=["GET"])
def api_waterfalls():
    source = request.args.get("source", DEFAULT_SOURCE).strip()
    date_0 = request.args.get("date_0", "").strip()
    date_1 = request.args.get("date_1", "").strip()
    nim_type = request.args.get("nim_type", "TRY").strip()

    if not date_0 or not date_1:
        return _json_response({"ok": False, "error": "date_0 ve date_1 zorunlu."}, status=400)

    try:
        _load_data_once(source)
        avail = set(DATES_CACHE[source])
        if date_0 not in avail or date_1 not in avail:
            return _json_response({
                "ok": False,
                "error": f"Seçilen tarihler bu kaynakta yok. source={source}, date_0={date_0}, date_1={date_1}",
            }, status=400)

        nim_info, fig1, fig2, fig3, fig4 = _build_figs_for_dates(source, date_0, date_1, nim_type=nim_type)
        return _json_response({
            "ok": True,
            "nim_info": nim_info,
            "figs": {
                "wf1": fig1.to_plotly_json(),
                "wf2": fig2.to_plotly_json(),
                "wf3": fig3.to_plotly_json(),
                "wf4": fig4.to_plotly_json(),
            },
        })
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, status=500)


@app.route("/health")
def health():
    return "ok", 200


if __name__ == "__main__":
    _load_data_once(DEFAULT_SOURCE)
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
