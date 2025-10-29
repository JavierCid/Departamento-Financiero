# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from urllib.parse import quote
import io
import json
import pandas as pd
import pdfplumber
from extractor import extract_from_pages
from datetime import datetime
from bankflow_rules import process_bankflow  # ← Usamos el pipeline completo

# =========================
# App + CORS (versión estable local)
# =========================
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# 🔧 Aceptar cualquier origen en local (localhost / 127.0.0.1)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:7252",
        "https://localhost:7252",
        "http://127.0.0.1:7252",
        "https://127.0.0.1:7252",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition", "X-Preview"],
)


@app.options("/{full_path:path}")
async def cors_preflight(full_path: str):
    return Response(status_code=204)


# =========================


def _clip(text: str | None, maxlen: int = 27) -> str:
    s = "" if text is None else str(text)
    return s if len(s) <= maxlen else s[: maxlen - 3] + "..."


def _short_name(filename: str, maxlen: int = 27) -> str:
    if not filename:
        return ""
    if len(filename) <= maxlen:
        return filename
    parts = filename.rsplit(".", 1)
    if len(parts) == 2:
        stem, ext = parts[0], "." + parts[1]
    else:
        stem, ext = filename, ""
    keep = maxlen - len(ext) - 3
    if keep < 1:
        return "..." + ext
    return stem[:keep] + "..." + ext


def _fmt_eur(v):
    try:
        if v is None or v == "" or pd.isna(v):
            return "—"
        n = float(v)
        return f"{n:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v) if v is not None else "—"

# ==== Helpers BankFlow Pro ====


def _read_tabular(upload: UploadFile) -> pd.DataFrame:
    """
    Lee CSV o Excel y devuelve un DataFrame (todo en str).
    CSV: prueba separador ';' y luego ','.
    Excel: detecta la fila de encabezados por contenido,
           para saltar metadatos (logo, titular, cuenta, etc.).
    """
    name = (upload.filename or "").lower()
    raw = upload.file.read()
    bio = io.BytesIO(raw)

    if name.endswith(".csv"):
        try:
            df = pd.read_csv(bio, sep=";", dtype=str, encoding="utf-8", engine="python")
        except Exception:
            bio.seek(0)
            df = pd.read_csv(bio, sep=",", dtype=str, encoding="utf-8", engine="python")
        return df.fillna("")
    else:
        # Excel con posibles filas de metadatos arriba
        try:
            xls = pd.ExcelFile(bio, engine="openpyxl")
        except Exception as e:
            raise RuntimeError(f"Lectura Excel falló: {type(e).__name__}: {e}")

        import unicodedata

        def _norm_cell(x: str) -> str:
            s = str(x or "").strip().lower()
            s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
            return s.replace("\n", " ").replace("\r", " ")

        def _find_header_row(df_nohdr: pd.DataFrame) -> int | None:
            """
            Busca la fila de cabecera detectando patrones:
            1. Extracto: (fecha + concepto + importe)
            2. Remesa: (importe + (beneficiario/proveedor/nombre o concepto))
            """
            MAX_SCAN = min(30, len(df_nohdr))
            for i in range(MAX_SCAN):
                row_vals = [_norm_cell(v) for v in df_nohdr.iloc[i].tolist()]
                if not any(row_vals):
                    continue
                
                # --- Detectar palabras clave ---
                has_fecha = any("fecha" in v for v in row_vals)
                has_concepto = any("concepto" in v for v in row_vals)
                has_importe = any("importe" in v for v in row_vals)
                
                # Palabras clave de Remesa (Beneficiario/Proveedor)
                has_beneficiario = any(
                    any(k in v for k in ["beneficiario", "proveedor", "nombre", "destinatario"]) 
                    for v in row_vals
                )
                
                # --- Comprobar patrones ---
                
                # Patrón 1: Extracto bancario
                if has_fecha and has_concepto and has_importe:
                    return i
                    
                # Patrón 2: Detalle de remesa
                if has_importe and (has_beneficiario or has_concepto):
                    return i
                    
            return None # No se encontró ninguna cabecera conocida

        # Recorre hojas y detecta cabecera por contenido
        for sheet in xls.sheet_names:
            try:
                tmp = xls.parse(sheet_name=sheet, header=None, dtype=str)
            except Exception:
                continue

            hdr_row = _find_header_row(tmp)
            if hdr_row is not None:
                try:
                    # Lee el archivo usando la fila de cabecera detectada
                    df = xls.parse(sheet_name=sheet, header=hdr_row, dtype=str)
                    if df.shape[1] >= 2: # Solo necesita 2+ columnas
                        return df.fillna("") # Devuelvefillna("") aquí
                except Exception:
                    continue

        # Fallback: primera hoja tal cual (probablemente fallará pero es el último recurso)
        try:
            return xls.parse(xls.sheet_names[0], dtype=str).fillna("")
        except Exception as e:
            raise RuntimeError(f"Excel parse sin cabecera también falló: {type(e).__name__}: {e}")


def _norm_colnames(df: pd.DataFrame) -> pd.DataFrame:
    import unicodedata

    def norm(s):
        s = str(s or "").strip().lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        s = s.replace("\n", " ").replace("\r", " ")
        return s
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    return df


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in df.columns:
        for cand in candidates:
            if cand in c:
                return c
    return None


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


def _to_float_eu(txt: str) -> float | None:
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


def _fmt_eu(v: float | None) -> str:
    if v is None or pd.isna(v):
        return ""
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# =========================
# Parser PDFs (pdf2excel)
# =========================

def parse_pdf_to_df(pdf_bytes: bytes, nombre_archivo: str) -> pd.DataFrame:
    import pytesseract
    from pdf2image import convert_from_bytes
    import os

    # --- Configuración rutas OCR (opcional si no están en PATH) ---
    tess = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    if os.path.exists(tess):
        pytesseract.pytesseract.tesseract_cmd = tess

    POPPLER_BIN = r"C:\tools\poppler\Library\bin"  # ajusta si usaste otra ruta
    use_poppler = os.path.isdir(POPPLER_BIN)

    # 1) Intento de extracción de texto nativo (PDF con texto embebido)
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages_texts = []
        for p in pdf.pages:
            t = p.extract_text() or ""
            # Si la página viene vacía o con muy poco texto, forzamos OCR de esa página
            if not t.strip() or len(t.strip()) < 40:
                try:
                    img = convert_from_bytes(
                        pdf_bytes,
                        first_page=p.page_number,
                        last_page=p.page_number,
                        poppler_path=POPPLER_BIN if use_poppler else None,
                        dpi=300
                    )[0]
                    ocr_text = pytesseract.image_to_string(
                        img,
                        lang="spa+eng",
                        config="--oem 1 --psm 6"
                    )

                    if ocr_text.strip():
                        pages_texts.append(ocr_text)
                        continue
                except Exception as e:
                    print(f"[WARN] OCR page {p.page_number} failed: {e}")
            # Si había texto suficiente, nos quedamos con el de pdfplumber
            if t.strip():
                pages_texts.append(t)

    # 2) Si aun así no hay nada, OCR al documento completo como último recurso
    if not pages_texts:
        try:
            images = convert_from_bytes(
                pdf_bytes,
                poppler_path=POPPLER_BIN if use_poppler else None,
                dpi=300
            )
            for img in images:
                ocr_text = pytesseract.image_to_string(
                    img,
                    lang="spa+eng",
                    config="--oem 1 --psm 6"
                )

                if ocr_text.strip():
                    pages_texts.append(ocr_text)
        except Exception as e:
            print(f"[WARN] OCR fallback failed: {e}")

    # 3) Parseo de campos
    fields = extract_from_pages(pages_texts, nombre_archivo)

    proveedor = fields.get("Proveedor_full") or fields.get("Proveedor")
    invoice = (
        fields.get("Invoice_full") or fields.get("Invoice")
        or fields.get("Factura") or fields.get("Nº factura")
    )
    concepto = fields.get("Concepto_full") or fields.get("Concepto")

    row = {
        "Proveedor": proveedor,
        "Fecha": fields.get("Fecha"),
        "Invoice": invoice,
        "Concepto": concepto,
        "Neto": fields.get("Neto"),
        "IVA": fields.get("IVA"),
        "IRPF": fields.get("IRPF"),
        "Importe Bruto": (
            fields.get("Importe bruto")
            or fields.get("Total Bruto")
            or fields.get("Bruto")
        ),
    }

    cols = ["Proveedor", "Fecha", "Invoice", "Concepto", "Neto", "IVA", "IRPF", "Importe Bruto"]
    return pd.DataFrame([row], columns=cols)


# =========================
# Endpoint PDF -> Excel
# =========================


@app.post("/api/pdf2excel")
async def pdf2excel(file: List[UploadFile] = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="Sube al menos un PDF")

    dfs: list[pd.DataFrame] = []
    for f in file:
        if not f.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"'{f.filename}' no es un PDF")

        contenido = await f.read()
        if not contenido:
            raise HTTPException(status_code=400, detail=f"'{f.filename}' está vacío")

        try:
            df = parse_pdf_to_df(contenido, f.filename)
            df["Archivo"] = f.filename
            df["ArchivoPreview"] = _short_name(f.filename, 27)
            dfs.append(df)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error leyendo '{f.filename}': {e}")

    df_total = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

    # Generar Excel
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        cols = [
            "Archivo", "Proveedor", "Fecha", "Invoice", "Concepto",
            "Neto", "IVA", "IRPF", "Importe Bruto",
        ]
        df_excel = df_total.reindex(columns=cols)
        df_excel.to_excel(w, index=False, sheet_name="Facturas")

        from openpyxl.styles import Font, Alignment, PatternFill, Border
        from openpyxl.utils import get_column_letter

        wb = w.book
        ws = wb["Facturas"]

        euro_fmt = '#,##0.00 [$€-40C]'
        for col in ["F", "G", "H", "I"]:
            for cell in ws[col]:
                if isinstance(cell.value, (int, float)):
                    cell.number_format = euro_fmt

        ws.sheet_view.showGridLines = False

        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill("solid", fgColor="4F81BD")
        center_align = Alignment(horizontal="center", vertical="center")

        for cell in ws[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_align
            cell.border = Border()

        for col_idx in range(1, 10):  # A..I
            col_letter = get_column_letter(col_idx)
            max_len = 0
            for cell in ws[col_letter]:
                v = cell.value
                if v is None:
                    l = 0
                elif isinstance(v, (int, float)):
                    l = len(f"{v:,.2f}")
                else:
                    l = len(str(v))
                if l > max_len:
                    max_len = l

                if cell.row != 1:
                    cell.border = Border()
                    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

            ws.column_dimensions[col_letter].width = max(10, min(max_len + 2, 50))

    xlsx_bytes = out.getvalue()

    # Vista previa
    preview_rows = []
    for _, r in df_total.head(50).iterrows():
        preview_rows.append({
            "Archivo": _clip(r.get("ArchivoPreview"), 27),
            "OCR": "—",
            "Proveedor": _clip(r.get("Proveedor"), 27),
            "Fecha": r.get("Fecha"),
            "Invoice": r.get("Invoice"),
            "Concepto": _clip(r.get("Concepto"), 30),
            "Total Neto": _fmt_eur(r.get("Neto")),
            "IVA €": _fmt_eur(r.get("IVA")),
            "IRPF": _fmt_eur(r.get("IRPF")),
            "Total Bruto": _fmt_eur(r.get("Importe Bruto")),
        })

    preview = {"Filas": int(len(df_total)), "Muestra": preview_rows}

    out_name = "Facturas procesadas.xlsx"
    content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    headers = {
        "Content-Disposition": f'attachment; filename="{out_name}"; filename*=UTF-8\'\'{quote(out_name)}',
        "X-Preview": json.dumps(preview, ensure_ascii=True),
    }

    return Response(content=xlsx_bytes, media_type=content_type, headers=headers)

# =========================
# Endpoint BankFlow Pro
# =========================


@app.post("/api/bankflowpro")
async def bankflowpro(
    extracto: UploadFile = File(...),
    detalle_remesas: UploadFile | None = File(None),
):
    # 1) Leer el extracto (CSV/XLSX)
    try:
        ext_df = _read_tabular(extracto)
        ext_df = _norm_colnames(ext_df)
    except Exception as e:
        return Response(
            content=f"Error leyendo el extracto: {e}",
            media_type="text/plain",
            status_code=400,
        )

    # 2) Detectar columnas mínimas (por contenido)
    col_fecha = _find_col(ext_df, ["fecha operacion", "fecha de operacion", "fecha", "fecha valor"])
    col_concepto = _find_col(ext_df, [
        "concepto", "descripcion", "descripción", "detalle", "concepto ampliado",
        "detalle del movimiento", "observaciones"
    ])

    # Importe puede venir de formas distintas:
    col_importe = _find_col(ext_df, ["importe", "importe eur", "importe operacion", "amount", "importe operación"])

    # Alternativas por doble columna:
    col_cargo = _find_col(ext_df, ["cargo", "debe", "debito", "débito", "debit"])
    col_abono = _find_col(ext_df, ["abono", "haber", "credito", "crédito", "credit"])

    # Columna de signo / tipo
    col_signo = _find_col(ext_df, ["signo", "d/c", "tipo movimiento", "tipo mov", "movimiento"])

    tengo_importe = bool(col_importe or (col_cargo or col_abono))
    if not (col_fecha and col_concepto and tengo_importe):

        return Response(
            content="No se detectaron columnas mínimas (Fecha/Concepto/Importe) en el extracto.",
            media_type="text/plain",
            status_code=400,
        )

    # 3) Construir salida base (Volvemos a usar "Total")
    out_rows = []
    
    for _, r in ext_df.iterrows():
        fecha = _to_date_ddmmyyyy(r.get(col_fecha, "")) if col_fecha else ""
        concepto = str(r.get(col_concepto, "") or "") if col_concepto else ""

        # --- Importe robusto ---
        imp = 0.0
        if col_cargo or col_abono:
            cargo = _to_float_eu(r.get(col_cargo, "")) if col_cargo else 0.0
            abono = _to_float_eu(r.get(col_abono, "")) if col_abono else 0.0
            imp = (abono or 0.0) - (cargo or 0.0)
        elif col_importe:
            imp = _to_float_eu(r.get(col_importe, "")) or 0.0
            if col_signo:
                s = str(r.get(col_signo, "") or "").strip().lower()
                neg = s in {"d", "debe", "cargo", "-", "debito", "débito"}
                pos = s in {"h", "haber", "abono", "+", "credito", "crédito"}
                if neg and imp > 0:
                    imp = -imp
                if pos and imp < 0:
                    imp = -imp
        
        out_rows.append({
            "Fecha": fecha,
            "Concepto": concepto,
            "Tipo": "",
            "Importe": imp,
            "Comisión": 0.0,
            "IVA": 0.0,
            "IRPF": 0.0,
            "Importe Neto": imp, # <-- Nueva columna final es Importe Neto

        })

    # --- Leer detalle remesas (si existe) ---
    rem_df = None
    avisos = []
    if detalle_remesas:
        try:
            rem_df = _read_tabular(detalle_remesas)
            rem_df = _norm_colnames(rem_df)
        except Exception as e:
            avisos.append(f"Aviso: No se pudo leer el detalle de remesas: {e}")

    # --- DataFrame con "Total" ---
    out_df = pd.DataFrame(out_rows, columns=["Fecha", "Concepto", "Tipo", "Importe", "Comisión", "IVA", "IRPF", "Importe Neto"])


    # 3.1) Aplicar pipeline completo (Reglas + Remesas)
    out_df, avisos_bankflow = process_bankflow(out_df, rem_df)
    avisos.extend(avisos_bankflow)

        # 4) X-Preview (máx. 50 filas)
    preview_rows = []

    for _, r in out_df.head(50).iterrows():
        concepto_preview = str(r.get("Concepto", "") or "")
        if len(concepto_preview) > 30:
            concepto_preview = concepto_preview[:30] + "…"

        tipo_str = str(r.get("Tipo", "") or "")

        # parseo robusto (EU) para números en preview
        def _to_num(v):
            if isinstance(v, (int, float)):
                return float(v)
            return _to_float_eu(v) or 0.0

        imp      = _to_num(r.get("Importe", 0.0))
        com_val  = _to_num(r.get("Comisión", r.get("Comision", 0)))  # ← tilde y fallback
        iva_val  = _to_num(r.get("IVA", 0.0))
        irpf_val = _to_num(r.get("IRPF", 0.0))
        neto_val = _to_num(r.get("Importe Neto", 0.0))

        # Caso especial: Comisión bancaria → comisión = importe, sin IVA/IRPF
        if "comisión bancaria" in tipo_str.lower() or "comision bancaria" in tipo_str.lower():
            com_val  = imp
            iva_val  = 0.0
            irpf_val = 0.0
            neto_val = abs(imp)

        preview_rows.append({
            "Fecha": r.get("Fecha", ""),
            "Concepto": concepto_preview,
            "Tipo": tipo_str,
            "Importe": _fmt_eur(imp),
            "Comisión": _fmt_eu(com_val),
            "Comision": _fmt_eu(com_val),

            "IVA": _fmt_eur(iva_val),
            "IRPF": _fmt_eur(irpf_val),
            "Importe Neto": _fmt_eur(neto_val),
        })

    x_preview = json.dumps({"Filas": int(len(out_df)), "Muestra": preview_rows}, ensure_ascii=True)

    # 5) Generar Excel (hoja única Movimientos_desglosados)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        sheet = "Movimientos_desglosados"
        out_df.to_excel(w, index=False, sheet_name=sheet)

        from openpyxl.styles import Font, Alignment, PatternFill, Border
        from openpyxl.utils import get_column_letter

        wb = w.book
        ws = wb[sheet]

        ws.sheet_view.showGridLines = False

        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill("solid", fgColor="1f3564")
        left_align = Alignment(horizontal="left", vertical="center")

        for cell in ws[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = left_align
            cell.border = Border()

        euro_fmt = '#,##0.00'
        # D..H (Importe, Comisión, IVA, IRPF, Importe Neto)
        for col_idx in range(4, 9):

            col_letter = get_column_letter(col_idx)
            for cell in ws[col_letter][1:]:
                cell.number_format = euro_fmt
                cell.alignment = left_align

        for col_idx in range(1, 9):  # A..H
            col_letter = get_column_letter(col_idx)
            max_len = 10
            for cell in ws[col_letter]:
                v = cell.value
                if v is None:
                    l = 0
                elif isinstance(v, (int, float)):
                    l = len(f"{v:,.2f}")
                else:
                    l = len(str(v))
                if l > max_len:
                    max_len = l
                if cell.row != 1:
                    cell.border = Border()

            ws.column_dimensions[col_letter].width = max(10, min(max_len + 2, 50))

        # --- INICIO: Sombrear filas de remesa desglosada ---
        
        # Define el color de sombreado (Azul claro sutil)
        remesa_fill = PatternFill("solid", fgColor="EAF2F8") # <-- NUEVO COLOR

        for idx, row in out_df.iterrows():
            try:
                tipo_lower = str(row.get("Tipo", "") or "").lower()
                comision_val = float(row.get("Comisión", 0.0) or 0.0)

                es_traspaso = "traspaso" in tipo_lower
                es_comision_banco = "comision" in tipo_lower
                
                is_remesa_line = (comision_val == 0.0) and not es_traspaso and not es_comision_banco

                if is_remesa_line:
                    ws_row = idx + 2 
                    for cell in ws[ws_row]:
                        cell.fill = remesa_fill
            except Exception:
                pass
        # --- FIN: Sombreado ---

    xlsx_bytes = out.getvalue()

    headers = {
        "Content-Disposition": 'attachment; filename*=UTF-8\'\'Movimientos_desglosados.xlsx',
        "X-Preview": x_preview,
    }
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )
# =========================
# Lanzador local
# =========================
if __name__ == "__main__":
    import uvicorn
    print("✅ FastAPI corriendo en http://127.0.0.1:8010")
    uvicorn.run("main:app", host="127.0.0.1", port=8010, reload=True)
