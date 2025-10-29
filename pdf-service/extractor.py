import re
from typing import Optional, Dict, Any, List
import json, os

# =========================
# Proveedores conocidos (palabras clave -> nombre completo)
# =========================
KNOWN_SUPPLIERS: dict[str, str] = {}

try:
    # Carga desde un archivo externo "proveedores.json" si existe
    path_json = os.path.join(os.path.dirname(__file__), "proveedores.json")
    if os.path.exists(path_json):
        with open(path_json, "r", encoding="utf-8") as f:
            KNOWN_SUPPLIERS = json.load(f)
except Exception:
    KNOWN_SUPPLIERS = {}


# =========================
# Utilidades texto / truncado
# =========================

def _clean_text(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\x0c", " ")
    txt = txt.replace("\r", "\n")
    txt = re.sub(r"[ \t]+", " ", txt)
    return txt.strip()

def _truncate(s: Optional[str], maxlen: int = 22) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    if len(s) <= maxlen:
        return s
    # deja siempre longitud total = maxlen
    suffix = " (...)"
    return s[: maxlen - len(suffix)] + suffix

def _clean_and_shorten_concept(s: Optional[str], maxlen: int = 22) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s{2,}", " ", s).strip()
    # evita que el concepto “se coma” importes/columnas
    s = re.sub(r"\b\d[\d\s\.,]*(?:€|eur|euros?)\b.*", "", s, flags=re.I)
    return _truncate(s, maxlen)

def _sanitize_concept_line(line: str) -> Optional[str]:
    """
    Descarta encabezados/etiquetas (CONCEPTO/IMPORTE…), razones sociales y líneas
    con texto espaciado tipo 'C O N C E P T O', además de fechas, totales, % y €.
    """
    s = line.strip(" :-•·\t")
    if not s:
        return None

    # 0) Encabezados evidentes o texto espaciado en mayúsculas (p. ej. 'C O N C E P T O')
    if re.search(r"\b(CONCEPTO|IMPORTE|CANTIDAD|DESCRIPCI[ÓO]N)\b", s, re.I):
        return None
    if re.fullmatch(r"(?:[A-ZÁÉÍÓÚÜÑ]\s*){3,}", s):  # letras separadas por espacios
        return None
    if s.isupper() and len(s) <= 40 and not re.search(r"[a-záéíóúüñ]", s):  # todo mayúsculas corto
        return None

    # 1) Evita razones sociales / datos fiscales
    if re.search(r"\b(S\.?L\.?|S\.?A\.?|CIF|C\.I\.F\.?|NIF|VAT)\b", s, re.I):
        return None

    # 2) Fechas
    if re.search(r"\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}", s):  # dd/mm/yyyy, etc.
        return None

    # 3) Totales/etiquetas e importes
    if re.search(r"\b(base|iva|i\.?v\.?a\.?|irpf|retenci[oó]n|total|importe|suman)\b", s, re.I):
        return None
    if "%" in s or "€" in s:
        return None

    return s

def _line_before(label: str, text: str) -> Optional[str]:
    """Devuelve la línea no vacía inmediatamente anterior a la primera coincidencia del label."""
    m = re.search(rf"(?m)^\s*.*{label}.*$", text, flags=re.I)
    if not m:
        return None
    head = text[:m.start()].splitlines()
    for ln in reversed(head):
        ln = ln.strip()
        if not ln:
            continue
        cand = _sanitize_concept_line(ln)
        if cand:
            return cand
    return None

# =========================
# Números / importes
# =========================

CUR = r"(?:€|EUR|Euros?)"
_M_AMT = r"([\(\-]?\s*\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{2})?\s*\)?)"

def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "")
    # 1.234.567,89 -> 1234567.89 ; 2,500.00 -> 2500.00 ; 525,00 -> 525.00
    if "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def _clean_amount(m: str) -> Optional[float]:
    if not m:
        return None
    mm = re.search(r"[\-\(]?\s*[\d\.,\s]+", m)
    if not mm:
        return None
    val = _to_float(mm.group(0))
    if val is None:
        return None
    if "(" in m and ")" in m:
        val = -abs(val)
    return val

def _find_amount_after(label: str, text: str) -> Optional[float]:
    # evita porcentajes (21 %) como importes
    pat = rf"{label}[^\n\r]*?{_M_AMT}(?!\s*%)\s*(?:{CUR})?"
    m = re.search(pat, text, flags=re.I)
    return _clean_amount(m.group(1)) if m else None

def _find_amount_line_start(label: str, text: str) -> Optional[float]:
    """
    Línea que empieza con `label` y contiene un importe; ignora porcentajes (ej. '21 %').
    Nota: sin \b tras label para casar 'TOTAL I.V.A.' / 'TOTAL IVA'.
    """
    # Captura el primer importe de la línea cuyo label coincide, excluyendo valores seguidos de '%'
    pat = rf"(?m)^\s*{label}.*?{_M_AMT}(?!\s*%)\s*(?:{CUR})?"
    m = re.search(pat, text, flags=re.I)
    return _clean_amount(m.group(1)) if m else None



def _find_amount_below(label: str, text: str) -> Optional[float]:
    """
    Busca una línea que contenga `label` y examina 1–3 líneas siguientes.
    Devuelve el importe más pequeño encontrado (evita confundir la base con el IVA),
    ignorando líneas con '%'.
    """
    m = re.search(rf"(?m)^\s*.*{label}.*$", text, flags=re.I)
    if not m:
        return None
    tail = text[m.end():].splitlines()
    found: List[float] = []
    for i in range(min(3, len(tail))):
        line = tail[i]
        if "%" in line:
            continue
        for g in re.findall(_M_AMT, line):
            amt = _clean_amount(g)
            if amt is not None:
                found.append(amt)
    if not found:
        return None
    return min(found)

def _find_all_amounts(text: str) -> List[float]:
    nums = re.findall(r"(?<![\d,\.])\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?", text)
    vals: List[float] = []
    for r in nums:
        v = _to_float(r)
        if v and v > 0.01:
            vals.append(v)
    return vals

# =========================
# Fechas
# =========================

_MONTHS_EN = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,"apr":4,"april":4,
    "may":5,"jun":6,"june":6,"jul":7,"july":7,"aug":8,"august":8,"sep":9,"sept":9,
    "september":9,"oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}
_MONTHS_ES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,"abr":4,"abril":4,
    "may":5,"jun":6,"junio":6,"jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"sept":9,
    "septiembre":9,"oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12
}

def _fmt_dmy(d: int, m: int, y: int) -> str:
    return f"{d:02d}/{m:02d}/{y:04d}"

def _parse_date(text: str) -> Optional[str]:
    t = text.lower()
    m = re.search(r"\b(\d{1,2})[\/\-](\d{1,2})[\/\-](20\d{2})\b", t)
    if m:
        return _truncate(_fmt_dmy(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    m = re.search(r"\b(20\d{2})[\/\-](\d{1,2})[\/\-](\d{1,2})\b", t)
    if m:
        return _truncate(_fmt_dmy(int(m.group(3)), int(m.group(2)), int(m.group(1))))
    m = re.search(r"\b(\d{1,2})[.\-](\d{1,2})[.\-](20\d{2})\b", t)
    if m:
        return _truncate(_fmt_dmy(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    m = re.search(r"\b(\d{1,2})\s+([a-z]{3,9})\s+(20\d{2})\b", t)
    if m:
        mon = _MONTHS_EN.get(m.group(2)[:3], None)
        if mon:
            return _truncate(_fmt_dmy(int(m.group(1)), mon, int(m.group(3))))
    m = re.search(r"\b(\d{1,2})\s+de\s+([a-záéíóú]{3,12})\s+(?:de|del\s+año)\s+(20\d{2})\b", t, flags=re.I)
    if m:
        name = m.group(2)
        mon = None
        for k, v in _MONTHS_ES.items():
            if name.startswith(k):
                mon = v
                break
        if mon:
            return _truncate(_fmt_dmy(int(m.group(1)), mon, int(m.group(3))))
    return None

# =========================
# Proveedor/Invoice simples
# =========================

def _guess_supplier(text: str) -> Optional[str]:
    U = text.upper()

    # 1) Marcas conocidas (prioridad: si aparece, devolvemos esto)
    KNOWN = [
        ("TAUW", "TAUW IBERIA"),
        ("GEOTECNIA", "GEOTECNIA"),
        ("MOMENTUM ARQUITECTURA", "MOMENTUM ARQUITECTURA S.L."),
        ("MOMENTUM REAL ESTATE", "MOMENTUM REAL ESTATE S.L."),
    ]
    for needle, nice in KNOWN:
        if needle in U:
            return nice

    # 2) Heurística: buscar la primera línea con S.L./S.A. que NO parezca
    # un bloque de dirección del destinatario (calle, nº, piso, CP, ciudad…).
    lines = [ln.strip() for ln in text.splitlines()]
    detected_name = None

    for i, ln in enumerate(lines):
        m = re.match(r"^([A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s\.\-&]+(?:S\.L\.|S\.A\.))\b", ln)
        if not m:
            continue

        nxt = " ".join(lines[i+1:i+3])
        if re.search(r"\b(C\/|C\.|CALLE|AVENIDA|AVDA|CL|Pº|PASEO|Nº|NUM|PISO|PLANTA|MADRID|BARCELONA|VALENCIA|\d{2}\s*\d{3}|ESPAÑA)\b",
                     nxt, flags=re.I):
            continue

        detected_name = re.sub(r"\s+", " ", m.group(1)).strip(" .-")
        detected_name = re.split(r"\b(CIF|C\.I\.F\.?|NIF|VAT)\b", detected_name, 1, flags=re.I)[0]
        return detected_name

    # 3) Si no se detectó proveedor por heurística, buscar coincidencia parcial en lista externa
    if not detected_name and KNOWN_SUPPLIERS:
        for needle, full in KNOWN_SUPPLIERS.items():
            if needle.lower() in text.lower():
                return full

    return None


def _guess_invoice(text: str) -> Optional[str]:
    for p in [
        r"factura\s*(?:nº|n\.|no|number|#)?\s*[:\-]?\s*([A-Z0-9\/\.\-]*\d[A-Z0-9\/\.\-]*)",
        r"\bno\.?\s*[:\-]?\s*([A-Z0-9\/\.\-]*\d[A-Z0-9\/\.\-]*)",
    ]:
        m = re.search(p, text, flags=re.I)
        if m:
            cand = m.group(1).strip(" .-")
            if re.search(r"\bEUROS?\b", cand, re.I):
                continue
            ctx = text[max(m.start()-30, 0): m.end()+30]
            if re.search(r"TOTAL\s+EUROS", ctx, re.I):
                continue
            return cand
    return None

# =========================
# Extracción principal
# =========================

def extract_fields_from_text(text: str, filename: str = "") -> Dict[str, Any]:
    t = _clean_text(text)

    # --- Proveedor / Invoice (versiones "full") ---
    supplier_full = _guess_supplier(t)          # SIN _truncate
    invoice_full  = _guess_invoice(t)           # SIN _truncate
    fecha         = _parse_date(t)              # dd/mm/yyyy (ya es corta por naturaleza)

    # --- Concepto (full) ---
    concepto_full = None
    m = re.search(r"(?im)^\s*concepto\b[^\n]*\n(.+?)(?:\n\s*(BASE|IVA|I\.?V\.?A\.?|TOTAL|IMPORTE)\b|$)", t)
    if m:
        lines = [ln for ln in m.group(1).splitlines() if ln.strip()]
        for ln in lines:
            cand = _sanitize_concept_line(ln)
            if cand:
                concepto_full = cand
                break
    if not concepto_full:
        m2 = re.search(r"(?i)(refacturaci[oó]n|arquitectura|estudio|trabajos?|acquisition fee|fee|proyecto|project)[^\n]{0,120}", t)
        if m2:
            cand = _sanitize_concept_line(m2.group(0))
            if cand:
                concepto_full = cand
    if not concepto_full:
        for lab in [r"Base\s+imponible", r"TOTAL\s+EUROS", r"\bTotal\b", r"Importe\s+total"]:
            concepto_full = _line_before(lab, t)
            if concepto_full:
                break

    # --- Importes (igual que antes) ---
    labels_total = [
    "Total", "Total factura", "Importe total", "Total bruto", "Total a pagar",
    "Total amount", "TOTAL EUROS", "SUMAN", "TOTAL FACTURA", "TOTAL A PAGAR",
    "Total Factura Euros"  # <- aparece así en tu PDF
    ]

    labels_base  = ["Base imponible", "Base", "Subtotal", "Neto", "Taxable base", "TOTAL BASE"]

    labels_iva   = [
        "IVA",
        r"I\.?\s*V\.?\s*A\.?",          # I.V.A / IVA con puntos/espacios
        r"TOTAL\s+IVA",
        r"TOTAL\s+I\.?\s*V\.?\s*A\.?",  # TOTAL I.V.A.
        "VAT",
        "Impuesto",
        "TOTAL I.V.A.",                 # literal exacto por si acaso
        "TIPO DE I.V.A."                # (por si algún proveedor pone solo el %)
    ]


    labels_irpf  = ["IRPF", "Retención", "Withholding"]

    total_bruto = None
    for L in labels_total:
        total_bruto = _find_amount_line_start(L, t) or _find_amount_after(L, t)
        if total_bruto is not None:
            break

    neto_base = None
    for L in labels_base:
        neto_base = _find_amount_line_start(L, t) or _find_amount_after(L, t)
        if neto_base is not None:
            break

    iva_eur = None

    # Captura directa en la misma línea (soporta I.V.A. / TOTAL I.V.A.)
    if iva_eur is None:
        m_iva = re.search(
            rf"(?im)^\s*(?:total\s+)?i\W*v\W*a\W*[:\-]?\s*{_M_AMT}(?!\s*%)\s*(?:{CUR})?\s*$",
            t
        )
        if m_iva:
            iva_eur = _clean_amount(m_iva.group(1))

    # Si no lo pillamos arriba, probamos con las heurísticas habituales
    if iva_eur is None:
        for L in labels_iva:
            iva_eur = (_find_amount_line_start(L, t) or
                       _find_amount_after(L, t) or
                       _find_amount_below(L, t))
            if iva_eur is not None:
                break


    irpf = None
    for L in labels_irpf:
        irpf = _find_amount_line_start(L, t) or _find_amount_after(L, t)
        if irpf is not None:
            break

    # --- Fallback 1: si hay % de IVA cerca pero no valor en €, calcula desde Base ---
    if iva_eur is None and neto_base is not None:
        pct_match = re.search(
            r"(?:IVA|I\.?\s*V\.?\s*A\.?|TIPO\s+DE\s+I\.?\s*V\.?\s*A\.?)"
            r"[^\n\r]{0,60}?(\d{1,2}(?:[.,]\d{1,2})?)\s*%",
            t, flags=re.I
        )
        if pct_match:
            try:
                pct_str = pct_match.group(1).replace(",", ".")
                rate = float(pct_str) / 100.0
                iva_eur = round(neto_base * rate, 2)
            except Exception:
                pass

    # --- Fallback 2: si SIGUE sin haber IVA y Neto != Bruto, aplica 21% (regla negocio) ---
    if iva_eur is None and neto_base is not None:
        if total_bruto is not None:
            # Solo si realmente hay diferencia entre Total y Neto
            if abs(total_bruto - neto_base) > 0.05:
                iva_eur = round(total_bruto - neto_base - (irpf or 0.0), 2)
                # Si la diferencia no era consistente (negativa rara), fuerza 21%
                if iva_eur is None or iva_eur < 0:
                    iva_eur = round(neto_base * 0.21, 2)
                    total_bruto = round(neto_base + iva_eur + (irpf or 0.0), 2)
        else:
            # No hay Total pero hay Neto: compón Total al 21%
            iva_eur = round(neto_base * 0.21, 2)
            total_bruto = round(neto_base + iva_eur + (irpf or 0.0), 2)



    # === Reglas de coherencia (igual que tenías) ===
    EPS = 0.05
    if iva_eur is not None and total_bruto is not None and iva_eur >= total_bruto - EPS:
        iva_eur = None

    if total_bruto is None and (neto_base is not None or iva_eur is not None or irpf is not None):
        total_bruto = round((neto_base or 0.0) + (iva_eur or 0.0) + (irpf or 0.0), 2)

    if neto_base is None and total_bruto is not None:
        neto_base = round(total_bruto - (iva_eur or 0.0) - (irpf or 0.0), 2)

    if (total_bruto is not None and neto_base is not None and
        (irpf is None or abs(irpf) < EPS) and
        (iva_eur is None or abs(total_bruto - neto_base - (irpf or 0.0) - iva_eur) > EPS)):
        iva_eur = round(total_bruto - neto_base - (irpf or 0.0), 2)

    if total_bruto is not None:
        if neto_base is not None and (irpf is not None):
            iva_calc = round(total_bruto - neto_base - irpf, 2)
            if iva_eur is None or abs((iva_eur or 0.0) - iva_calc) > EPS:
                iva_eur = iva_calc
        elif neto_base is not None and iva_eur is None:
            iva_eur = round(total_bruto - neto_base - (irpf or 0.0), 2)

    if iva_eur is not None and iva_eur < 0 and abs(iva_eur) < EPS:
        iva_eur = 0.0
    if irpf is not None and irpf < 0 and abs(irpf) < EPS:
        irpf = 0.0

    if total_bruto is not None and neto_base is not None:
        dif = round(total_bruto - (neto_base + (iva_eur or 0.0) + (irpf or 0.0)), 2)
        if abs(dif) <= EPS:
            if iva_eur is not None:
                iva_eur = round((iva_eur or 0.0) + dif, 2)
            elif irpf is not None:
                irpf = round((irpf or 0.0) + dif, 2)
            else:
                neto_base = round(neto_base + dif, 2)

    # === Construimos salida con FULL + SHORT (short solo para vista previa) ===
    fields: Dict[str, Any] = {
        # FULL (para Excel)
        "Proveedor_full": supplier_full,
        "Invoice_full": invoice_full,
        "Concepto_full": concepto_full,

        # SHORT (para vista previa; límite 22)
        "Proveedor": _truncate(supplier_full, 22),
        "Fecha": _truncate(fecha, 22),
        "Invoice": _truncate(invoice_full, 22),
        "Concepto": _clean_and_shorten_concept(concepto_full, 22),

        # Importes
        "Importe bruto": total_bruto,
        "IVA": iva_eur,
        "IRPF": irpf,
        "Neto": neto_base,
    }

    return fields

# =========================
# Por páginas (para app.py)
# =========================
def extract_from_pages(pages_texts: List[str], filename: str) -> Dict[str, Any]:
    text_full = "\n".join(pages_texts or [])
    return extract_fields_from_text(text_full, filename)
