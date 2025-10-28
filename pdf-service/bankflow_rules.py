# bankflow_rules.py
# Reglas BankFlow Pro: clasificación fiscal + expansión de remesas.

from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, List, Optional
import pandas as pd
import unicodedata
from datetime import datetime, timedelta

# =========================
# Utilidades de texto y parseo
# =========================


def _norm_text(s: str) -> str:
    s = str(s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s


def _contains_any(s: str, keywords: list[str]) -> bool:
    s = _norm_text(s)
    return any(k in s for k in keywords)


def _to_date_ddmmyyyy(txt: str) -> str:
    if not txt or str(txt).strip() == "":
        return ""
    t = str(txt).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    try:
        d = pd.to_datetime(t, dayfirst=True, errors="coerce")
        if pd.notna(d):
            return d.strftime("%d/%m/%Y")
    except Exception:
        pass
    return t


def _to_float_eu(txt) -> Optional[float]:
    if txt is None:
        return None
    s = str(txt).strip().replace("€", "").replace("EUR", "").replace(" ", "")
    # 1.234,56 -> 1234.56
    if "." in s and "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        try:
            return float(s.replace(".", "").replace(",", "."))
        except Exception:
            return None


def _redondea2(x: float) -> float:
    return float(f"{x:.2f}")

# =========================
# Configuración de reglas
# =========================
TIPO_RULES: list[tuple[list[str], str, float, float]] = [
    (["notar", "gestor", "gestoria", "notaria"], "Profesional (notaría/gestoría)", 0.21, 0.15),
    (["abogado", "abogados", "cuatrecasas", "bufete"], "Servicios legales", 0.21, 0.00),
    (["csc"], "Colaborador CSC", 0.21, 0.19),
    (["constructora", "obra", "contrata"], "Constructora", 0.00, 0.00),
    (["comision", "comisiones", "gasto bancario", "gastos bancarios", "comision bancaria", "comision banco"], "Comisión bancaria", 0.00, 0.00),
    (["traspaso", "transferencia interna", "entre cuentas", "internal transfer"], "Traspaso", 0.00, 0.00),
]

IVA_CERO_FORZADO = [
    "transferencia internacional", "ltd.", "gmbh", "licencia", "licencias",
    "comision", "comisiones", "retenciones e ing. a cta. ggee", "ppl", "eniv","drawdown",
    "constructora",
]

REMESA_HINTS = ["remesa", "transferencias", "norma 19", "csb19", "cuaderno 19"]


@dataclass
class Clasificacion:
    tipo: str
    iva_pct: float
    irpf_pct: float
    es_traspaso: bool
    es_comision_banco: bool
    es_remesa: bool


def _clasificar(concepto: str) -> Clasificacion:
    txt = _norm_text(concepto)
    es_remesa = _contains_any(txt, REMESA_HINTS)

    tipo = "General"
    iva = 0.21
    irpf = 0.0
    es_traspaso = False
    es_comision_banco = False

    for keys, tipo_name, iva_pct, irpf_pct in TIPO_RULES:
        if _contains_any(txt, keys):
            tipo = tipo_name
            iva = iva_pct
            irpf = irpf_pct
            if "traspaso" in tipo_name.lower():
                es_traspaso = True
            if "comision" in tipo_name.lower():
                es_comision_banco = True
            break

    # Excepción: DALUX siempre lleva IVA 21 %
    if "dalux" in txt:
      iva = 0.21
    elif _contains_any(txt, IVA_CERO_FORZADO):
      iva = 0.0


    return Clasificacion(
        tipo=tipo or "",
        iva_pct=iva,
        irpf_pct=irpf,
        es_traspaso=es_traspaso,
        es_comision_banco=es_comision_banco,
        es_remesa=es_remesa,
    )

# =========================
# Cálculo fiscal por línea
# =========================


def _calcula_linea(concepto: str, importe: float, disable_fixed_commission: bool = False) -> tuple[str, float, float, float, float]:
    """
    Devuelve: (tipo, comision_fija, iva, irpf, total_calculado)
    """
    clas = _clasificar(concepto)

    if clas.es_traspaso:
        return ("Traspaso", 0.0, 0.0, 0.0, _redondea2(importe))

    comision = 0.0
    if not disable_fixed_commission and not clas.es_remesa and not clas.es_comision_banco:
        # Jamás aplicar comisión positiva.
        # También evitar comisión si el concepto contiene ENIV o Drawdown.
        txt_concepto = _norm_text(concepto)
        if "eniv" in txt_concepto or "drawdown" in txt_concepto:
            comision = 0.0
        elif importe < 0:
            comision = -1.0
        else:
            comision = 0.0


    tipo = clas.tipo or ("Comisión bancaria" if clas.es_comision_banco else "General")

    iva_pct = float(clas.iva_pct or 0.0)
    irpf_pct = float(clas.irpf_pct or 0.0)

    imponible = importe - comision
    denom = 1.0 + iva_pct - irpf_pct
    if abs(denom) < 1e-9:
        base = imponible
        iva = 0.0
        irpf = 0.0
    else:
        base = imponible / denom
        iva = base * iva_pct
        irpf = - base * irpf_pct

    base_r = _redondea2(base)
    iva_r = _redondea2(iva)
    irpf_r = _redondea2(irpf)
    com_r = _redondea2(comision)
    
    importe_r = _redondea2(importe)
    total_calc = base_r + iva_r + irpf_r + com_r
    delta = _redondea2(importe_r - total_calc)

    if abs(delta) > 0 and abs(delta) <= 0.02:
        iva_r = _redondea2(iva_r + delta)
    elif abs(delta) > 0.02 and abs(base_r) > 0:
        base_r = _redondea2(base_r + delta)

    # Nuevo cálculo: Importe Neto = |Importe| - |IVA| - |IRPF|
    importe_neto = _redondea2(abs(importe_r) - abs(iva_r) - abs(irpf_r))

    return (tipo, com_r, iva_r, irpf_r, importe_neto)


# =========================
# API pública — Reglas generales
# =========================

def apply_accounting_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica reglas a DataFrame con columnas:
    Fecha | Concepto | Tipo | Importe | Comisión | IVA | IRPF | Importe Neto
    """
    if df is None or df.empty:
        return df

    # --- Volvemos a "Total" ---
    required = ["Fecha", "Concepto", "Tipo", "Importe", "Comisión", "IVA", "IRPF", "Importe Neto"]

    # Alias temporal por compatibilidad
    if "Importe Neto" in df.columns and "Total" not in df.columns:
        df["Total"] = df["Importe Neto"]

    for c in required:
        if c not in df.columns:
            raise ValueError(f"apply_accounting_rules: falta la columna '{c}'")

    out = df.copy()

    tipos, coms, ivas, irpfs, totales = [], [], [], [], []
    for _, row in out.iterrows():
        concepto = str(row.get("Concepto", "")) or ""
        try:
            importe = float(row.get("Importe", 0.0) or 0.0)
        except Exception:
            importe = _to_float_eu(row.get("Importe", "0")) or 0.0


        tipo, com, iva, irpf, total = _calcula_linea(concepto, importe)
        tipos.append(tipo)
        coms.append(com)
        ivas.append(iva)
        irpfs.append(irpf)
        totales.append(total) # <-- Guardamos el Total

    out["Tipo"] = tipos
    out["Comisión"] = coms
    out["IVA"] = ivas
    out["IRPF"] = irpfs
    out["Importe Neto"] = totales # <-- Asignamos el nuevo Importe Neto

    return out

# =========================
# API pública — Desglose de remesas
# =========================


def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = [_norm_text(c) for c in df.columns]
    mapping = dict(zip(cols, df.columns))
    for cand in candidates:
        for norm_col, raw_col in mapping.items():
            if cand in norm_col:
                return raw_col
    return None


def _normalize_detalle(detalle_df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas normalizadas para detalle remesas:
    Fecha | Proveedor | Concepto | Importe
    """
    if detalle_df is None or detalle_df.empty:
        return pd.DataFrame(columns=["Fecha", "Proveedor", "Concepto", "Importe"])

    df = detalle_df.copy()
    col_fecha = _find_col(df, ["fecha", "f. operacion", "f operacion", "fecha valor", "fecha envio", "fecha envío"])
    col_conc = _find_col(df, ["concepto", "descripcion", "detalle", "concept"])
    col_imp = _find_col(df, ["importe", "amount", "importe eur", "sum"])
    col_prov = _find_col(df, ["proveedor", "beneficiario", "ordenante", "cliente", "destinatario", "nombre"])

    if col_prov is None:
        col_prov = col_conc

    rows = []
    for _, r in df.iterrows():
        fecha = _to_date_ddmmyyyy(r.get(col_fecha, "")) if col_fecha else ""
        concepto = str(r.get(col_conc, "") or "") if col_conc else ""
        proveedor = str(r.get(col_prov, "") or "") if col_prov else ""
        
        imp = _to_float_eu(r.get(col_imp, "")) if col_imp else None
        
        if imp is None:
            try:
                imp = float(r.get(col_imp, 0.0))
                if imp == 0.0: continue
            except Exception:
                continue

        desc = proveedor if proveedor else concepto
        if proveedor and concepto and proveedor.strip().lower() not in concepto.strip().lower():
            desc = f"{proveedor} – {concepto}"
        elif not proveedor and concepto:
            desc = concepto
        elif proveedor and not concepto:
            desc = proveedor

        rows.append({
            "Fecha": fecha,
            "Proveedor": proveedor,
            "Concepto": desc or concepto,
            "Importe": float(imp or 0.0),
        })

    return pd.DataFrame(rows, columns=["Fecha", "Proveedor", "Concepto", "Importe"])


def _in_range_ddmm(date_str: str, pivot_str: str, days: int = 1) -> bool:
    try:
        d = datetime.strptime(date_str, "%d/%m/%Y")
        p = datetime.strptime(pivot_str, "%d/%m/%Y")
        return (p - timedelta(days=days)) <= d <= (p + timedelta(days=days))
    except Exception:
        return False


def expand_remesas(extract_df: pd.DataFrame, detalle_df: Optional[pd.DataFrame]) -> tuple[pd.DataFrame, List[str]]:
    """
    Detecta remesas en extracto y sustituye por N líneas del detalle.
    """
    if extract_df is None or extract_df.empty:
        return extract_df, []

    out_rows = []
    avisos: List[str] = []

    det_norm = _normalize_detalle(detalle_df) if (detalle_df is not None and not detalle_df.empty) else pd.DataFrame(columns=["Fecha", "Proveedor", "Concepto", "Importe"])

    for idx, r in extract_df.reset_index(drop=True).iterrows():
        fecha = str(r.get("Fecha", "") or "")
        concepto = str(r.get("Concepto", "") or "")
        importe = float(r.get("Importe", 0.0) or 0.0)
        
        clas_remesa = _clasificar(concepto)
        
        if clas_remesa.es_remesa and not det_norm.empty:
            det_win = pd.DataFrame() 

            has_detail_dates = "Fecha" in det_norm.columns and not det_norm["Fecha"].str.strip().eq("").all()

            if has_detail_dates:
                det_win = det_norm[det_norm["Fecha"].apply(lambda d: _in_range_ddmm(d, fecha, 1))].copy()
            else:
                det_win = det_norm.copy()

            if det_win.empty:
                avisos.append(f"Remesa sin detalle coincidente ({fecha}): '{concepto}'")
                out_rows.append(r.to_dict())
                continue

            suma_det = float(det_win["Importe"].sum() if not det_win.empty else 0.0)
            target = _redondea2(importe)
            suma_det_signed = _redondea2(abs(suma_det) * (1 if target >= 0 else -1))

            if abs(target - suma_det_signed) <= 0.02: 
                for _, drow in det_win.iterrows():
                    imp_det = float(drow["Importe"] or 0.0)
                    imp_det = abs(imp_det) * (1 if target >= 0 else -1)

                    tipo, com, iva, irpf, total = _calcula_linea(str(drow["Concepto"]), imp_det, disable_fixed_commission=True)

                    out_rows.append({
                        "Fecha": fecha,
                        "Concepto": str(drow["Concepto"]),
                        "Tipo": tipo,
                        "Importe": _redondea2(imp_det),
                        "Comisión": 0.0,
                        "IVA": _redondea2(iva),
                        "IRPF": _redondea2(irpf),
                        "Total": _redondea2(total), # <-- Volvemos a "Total"
                    })
            else:
                avisos.append(
                    f"No cuadra remesa {fecha}: '{concepto}'. Extracto={target:.2f}, Detalle={suma_det_signed:.2f}"
                )
                out_rows.append(r.to_dict())
        else:
            out_rows.append(r.to_dict())

    # --- Volvemos a "Total" ---
    expanded_df = pd.DataFrame(out_rows, columns=["Fecha", "Concepto", "Tipo", "Importe", "Comisión", "IVA", "IRPF", "Importe Neto"])

    return expanded_df, avisos

# =========================
# Facade — todo en uno (opcional)
# =========================


def process_bankflow(extract_df: pd.DataFrame, detalle_df: Optional[pd.DataFrame] = None) -> tuple[pd.DataFrame, List[str]]:
    """
    Pipeline completo:
    1) Aplica reglas contables a todos los movimientos.
    2) Expande remesas con el detalle (si cuadra por fecha ±1 día y suma).
    Devuelve (df_final, avisos).
    """
    # 1. Aplicar reglas a todo (IVA, IRPF, Comisión Fija, Total)
    base = apply_accounting_rules(extract_df)
    
    # 2. Expandir remesas (recalcula todo sin comisión fija y con su Total)
    final, avisos = expand_remesas(base, detalle_df)
    
    return final, avisos