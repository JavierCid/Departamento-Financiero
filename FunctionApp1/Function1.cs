using ClosedXML.Excel;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net; // HttpStatusCode
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FunctionApp1
{
    public class Function1
    {
        // --- Vista previa ---
        private sealed class PreviewDto
        {
            public string? Sociedad { get; set; }
            public string? Periodo { get; set; } // yyyy-MM
            public List<Faltante> Faltantes { get; set; } = new();
            public List<Descuadre> Descuadres { get; set; } = new();
            public List<Faltante> FaltantesInverso { get; set; } = new(); // ⬅️ NUEVO
        }

        private sealed class Faltante { public string Factura { get; set; } = ""; public decimal Importe { get; set; } }
        private sealed class Descuadre { public string Factura { get; set; } = ""; public decimal Prinex { get; set; } public decimal Mayores { get; set; } public decimal Diferencia { get; set; } }

    
        private readonly ILogger _logger;

        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
        }

        // ============================================================
        // ========================= HTTP API =========================
        // ============================================================

        [Function("ProcesarExcel")]
        public async Task<HttpResponseData> ProcesarExcel(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", "options")] HttpRequestData req)
        {
            // Preflight
            if (req.Method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
            {
                var pre = req.CreateResponse(HttpStatusCode.NoContent);
                AddCors(pre, req);
                return pre;
            }

            try
            {
                if (req.Body is null)
                {
                    var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                    await bad.WriteStringAsync("Falta el archivo en el body.");
                    AddCors(bad, req);
                    return bad;
                }

                // 🔹 Leer mes y año desde querystring
                var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
                int.TryParse(query.Get("mes"), out var mes);
                int.TryParse(query.Get("anio"), out var anio);

                // Nombre original (cabecera X-File-Name)
                string fileName = "archivo.xlsx";
                if (req.Headers.TryGetValues("X-File-Name", out var hVals))
                {
                    var hv = hVals.FirstOrDefault();
                    if (!string.IsNullOrWhiteSpace(hv)) fileName = hv;
                }

                using var ms = new MemoryStream();
                await req.Body.CopyToAsync(ms);

                if (ms.Length == 0)
                {
                    var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                    await bad.WriteStringAsync("El archivo está vacío.");
                    AddCors(bad, req);
                    return bad;
                }
                if (ms.Length > 15 * 1024 * 1024) // 15 MB
                {
                    var tooBig = req.CreateResponse((HttpStatusCode)413);
                    await tooBig.WriteStringAsync("Archivo demasiado grande (>15 MB).");
                    AddCors(tooBig, req);
                    return tooBig;
                }

                ms.Position = 0;
                // Firma ZIP “PK”
                var hdr = new byte[2];
                ms.Read(hdr, 0, 2);
                if (hdr[0] != 0x50 || hdr[1] != 0x4B)
                {
                    var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                    await bad.WriteStringAsync("El archivo no parece un .xlsx válido (firma ZIP incorrecta).");
                    AddCors(bad, req);
                    return bad;
                }
                ms.Position = 0;

                var (outBytes, faltantesInverso) = await ProcesarComparacionAsync(ms, fileName, mes, anio);



                var resp = req.CreateResponse(HttpStatusCode.OK);
                resp.Headers.Add("X-Feature-Inverso", "on-2025-11-04");

                resp.Headers.Add("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

                var baseName = Path.GetFileNameWithoutExtension(fileName);
                if (string.IsNullOrWhiteSpace(baseName)) baseName = "Comparacion";
                var outName = $"Comparacion_{baseName}.xlsx";
                resp.Headers.Add("Content-Disposition",
                    $"attachment; filename=\"{outName}\"; filename*=UTF-8''{Uri.EscapeDataString(outName)}");
                resp.Headers.Add("X-Build", DateTime.UtcNow.ToString("yyyyMMdd-HHmmss"));

                // Vista previa y descarga basadas en el Excel REAL ya generado por ProcesarComparacionAsync
                using var outMs = new MemoryStream(outBytes, writable: false);
                using (var wb2 = new ClosedXML.Excel.XLWorkbook(outMs))
                {
                    var ws = wb2.Worksheet("Comparación");
                    var faltantesList = new List<object>();
                    var descuadresList = new List<object>();

                    int lastRow = ws.LastRowUsed()?.RowNumber() ?? 0;
                    int filaFalt = 0, filaDesc = 0;
                    for (int r = 1; r <= lastRow; r++)
                    {
                        var val = ws.Cell(r, 1).GetString().Trim();
                        if (val.Contains("NO están en MAYORES") || val.Contains("NO est�n en MAYORES")) filaFalt = r + 2;
                        if (val.Contains("DESCUADRE")) filaDesc = r + 2;
                    }

                    if (filaFalt > 0)
                    {
                        int r = filaFalt;
                        while (!string.IsNullOrWhiteSpace(ws.Cell(r, 1).GetString()))
                        {
                            faltantesList.Add(new
                            {
                                Factura = ws.Cell(r, 1).GetString().Trim().TrimStart('\''),
                                Importe = ws.Cell(r, 2).GetDouble()
                            });
                            r++;
                        }
                    }

                    if (filaDesc > 0)
                    {
                        int r = filaDesc;
                        while (!string.IsNullOrWhiteSpace(ws.Cell(r, 1).GetString()))
                        {
                            descuadresList.Add(new
                            {
                                Factura = ws.Cell(r, 1).GetString().Trim().TrimStart('\''),
                                ImportePrinex = ws.Cell(r, 2).GetDouble(),
                                ImporteMayores = ws.Cell(r, 3).GetDouble(),
                                Diferencia = ws.Cell(r, 4).GetDouble()
                            });
                            r++;
                        }
                    }

                    var preview = new PreviewDto
                    {
                        Faltantes = faltantesList.Select(x => new Faltante { Factura = (string)x.GetType().GetProperty("Factura")!.GetValue(x)!, Importe = Convert.ToDecimal(x.GetType().GetProperty("Importe")!.GetValue(x)) }).ToList(),
                        Descuadres = descuadresList.Select(x => new Descuadre
                        {
                            Factura = (string)x.GetType().GetProperty("Factura")!.GetValue(x)!,
                            Prinex = Convert.ToDecimal(x.GetType().GetProperty("ImportePrinex")!.GetValue(x)),
                            Mayores = Convert.ToDecimal(x.GetType().GetProperty("ImporteMayores")!.GetValue(x)),
                            Diferencia = Convert.ToDecimal(x.GetType().GetProperty("Diferencia")!.GetValue(x))
                        }).ToList(),
                        FaltantesInverso = faltantesInverso.Select(x => new Faltante { Factura = x.Factura, Importe = x.Importe }).ToList()
                    };
                    resp.Headers.Add("X-Preview", JsonSerializer.Serialize(preview));



                }

                // Enviar archivo + CORS y salir (usa directamente outBytes)
                await resp.WriteBytesAsync(outBytes);
                AddCors(resp, req);
                return resp;
            }

            catch (Exception ex) when (
                ex is FormatException ||
                ex is InvalidOperationException ||
                ex.Message.Contains("ClosedXML", StringComparison.OrdinalIgnoreCase))
            {
                var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                await bad.WriteStringAsync($"Excel inválido o corrupto: {ex.Message}");
                AddCors(bad, req);
                return bad;
            }
            catch (Exception ex)
            {
                var err = req.CreateResponse(HttpStatusCode.InternalServerError);
                await err.WriteStringAsync($"Error inesperado procesando Excel: {ex.Message}");
                AddCors(err, req);
                return err;
            }

        }

        // CORS dinámico
        public static void AddCors(HttpResponseData resp, HttpRequestData req)
{
    var origin = GetAllowedOrigin(req);
    if (!string.IsNullOrEmpty(origin))
    {
        resp.Headers.Add("Access-Control-Allow-Origin", origin);
        resp.Headers.Add("Vary", "Origin");
        resp.Headers.Add("Access-Control-Allow-Methods", "POST, OPTIONS");
        resp.Headers.Add("Access-Control-Allow-Headers", "Content-Type, X-File-Name");
        // 👉 nuevo: permite que el front lea estas cabeceras
        resp.Headers.Add("Access-Control-Expose-Headers", "Content-Disposition, X-Preview");
    }
}


        // ============================================================
        // ================ LÓGICA DE COMPARACIÓN =====================
        // ============================================================

        private const int MIN_DIGITS = 8;
        private const int MAX_DIGITS = 999;

        private async Task<(byte[] Archivo, List<(string Factura, decimal Importe)> FaltantesInverso)> ProcesarComparacionAsync(Stream xlsxStream, string originalFileName, int mesFiltro, int anioFiltro)


        {
            xlsxStream.Position = 0;

            using var wb = new XLWorkbook(xlsxStream);

            // Detectar año/mes desde nombre
            (bool haveYmd, int yy, int mm, int dd) = ExtractYMDFromFileName(originalFileName);

            // Hojas
            var wsMayores = wb.Worksheets.FirstOrDefault(s => string.Equals(s.Name, "MAYORES", StringComparison.OrdinalIgnoreCase));
            var wsSrc = wb.Worksheets.FirstOrDefault(s => string.Equals(s.Name, "Sheet1", StringComparison.OrdinalIgnoreCase))
                        ?? wb.Worksheets.FirstOrDefault(s => string.Equals(s.Name, "Hoja1", StringComparison.OrdinalIgnoreCase));

            if (wsMayores is null || wsSrc is null)
                throw new InvalidOperationException("No se encuentran las hojas 'MAYORES' y/o 'Sheet1/Hoja1'.");

            // Cabeceras requeridas
            var reqMay = new[] { "Cuenta", "Fecha", "Documento", "Haber", "Concepto" };
            var reqSrc = new[] { "S/Fra. Número", "Importe", "Fecha Fra.", "Fase actual", "Sociedad" };

            int rowMay = FindHeaderRow(wsMayores, reqMay, 60);
            int rowSrc = FindHeaderRow(wsSrc, reqSrc, 60);
            if (rowMay == 0) throw new InvalidOperationException("No se localizaron cabeceras en 'MAYORES'.");
            if (rowSrc == 0) throw new InvalidOperationException("No se localizaron cabeceras en 'Sheet1/Hoja1'.");

            // Columnas MAYORES
            int cCuenta = FindHeaderCol(wsMayores, rowMay, "Cuenta");
            int cFechaM = FindHeaderCol(wsMayores, rowMay, "Fecha");
            int cDoc = FindHeaderCol(wsMayores, rowMay, "Documento");
            int cHaber = FindHeaderCol(wsMayores, rowMay, "Haber");
            int cConc = FindHeaderCol(wsMayores, rowMay, "Concepto");
            int cDebe = FindHeaderCol(wsMayores, rowMay, "Debe");
            int cSocMay = FindHeaderCol(wsMayores, rowMay, "Sociedad");

            // Columnas SRC
            int cFra = FindHeaderCol(wsSrc, rowSrc, "S/Fra. Número");
            int cImporte = FindHeaderCol(wsSrc, rowSrc, "Importe");
            int cFechaFra = FindHeaderCol(wsSrc, rowSrc, "Fecha Fra.");
            int cFase = FindHeaderCol(wsSrc, rowSrc, "Fase actual");
            int cSocSrc = FindHeaderCol(wsSrc, rowSrc, "Sociedad");

            // Sociedad por nombre de fichero
            string sociedadClave = DetectSociedadClave(originalFileName);

            // Diccionarios
            var dMay = new Dictionary<string, decimal>(StringComparer.Ordinal);
            var dSrc = new Dictionary<string, decimal>(StringComparer.Ordinal);
            var dMayDisp = new Dictionary<string, string>(StringComparer.Ordinal);
            var dSrcDisp = new Dictionary<string, string>(StringComparer.Ordinal);

            // ====== Recorrer MAYORES ======
            int lastMay = wsMayores.LastRowUsed().RowNumber();
            for (int r = rowMay + 1; r <= lastMay; r++)
            {
                var cuenta = wsMayores.Cell(r, cCuenta).GetString().Trim();
                if (string.IsNullOrEmpty(cuenta)) continue;

                var cTrim = cuenta.Trim();
                if (!(cTrim.StartsWith("41") || cTrim.StartsWith("40"))) continue;
                if (cTrim.StartsWith("4109") || cTrim.StartsWith("4008")) continue;

                var haber = SafeNumber(wsMayores.Cell(r, cHaber).Value);
                if (Math.Abs(haber) < 0.0000001m) continue;

                var fechaM = wsMayores.Cell(r, cFechaM).Value;
                if (anioFiltro > 0 && mesFiltro > 0)
                {
                    if (!IsSameMonth(fechaM, anioFiltro, mesFiltro)) continue;
                }
                else if (haveYmd && !IsSameMonth(fechaM, yy, mm)) continue;


                if (cSocMay > 0 && !CellContainsSociedad(wsMayores.Cell(r, cSocMay).Value, sociedadClave)) continue;

                string rawDoc = wsMayores.Cell(r, cDoc).GetString();
                string rawConc = cConc > 0 ? wsMayores.Cell(r, cConc).GetString() : string.Empty;
                string token = PickBestInvoiceToken(rawDoc, rawConc);

                string mk = MakeMatchKey(token);
                bool tokenOk = !string.IsNullOrEmpty(token) &&
                               (!string.IsNullOrEmpty(mk) && (mk.Length >= MinNeedForToken(token) || IsRangeLikeToken(token) || HasDateSpaces(token)));

                if (!tokenOk)
                {
                    // Fallback: escanear fila
                    var fb = FindBestTokenInRow2(wsMayores, r, cCuenta, cFechaM, cDebe, cHaber, cDoc, cConc, out _, out _);
                    if (!string.IsNullOrEmpty(fb))
                    {
                        token = fb;
                        mk = MakeMatchKey(token);
                        tokenOk = !string.IsNullOrEmpty(mk);
                    }
                }

                if (!tokenOk) continue;

                string disp = MakeDisplayKey(token);
                if (dMay.TryGetValue(mk, out var acc))
                    dMay[mk] = acc + haber;
                else
                {
                    dMay[mk] = haber;
                    dMayDisp[mk] = disp;
                }
            }

            // ====== Recorrer Sheet1/Hoja1 ======
            int lastSrc = wsSrc.LastRowUsed().RowNumber();
            for (int r = rowSrc + 1; r <= lastSrc; r++)
            {
                string fase = NormalizePhase(wsSrc.Cell(r, cFase).Value);
                if (!(fase == "VISADO PM" || fase == "CONTABILIZAR FACTURA")) continue;

                


                if (cSocSrc > 0 && !CellContainsSociedad(wsSrc.Cell(r, cSocSrc).Value, sociedadClave)) continue;

                string token = ExtractNumToken(wsSrc.Cell(r, cFra).Value);
                if (string.IsNullOrEmpty(token)) continue;

                string mk = MakeMatchKey(token);
                if (string.IsNullOrEmpty(mk)) continue;

                decimal importeVal = SafeNumber(wsSrc.Cell(r, cImporte).Value);

                if (dSrc.TryGetValue(mk, out var acc))
                    dSrc[mk] = acc + importeVal;
                else
                {
                    dSrc[mk] = importeVal;
                    dSrcDisp[mk] = MakeDisplayKey(token);
                }
            }
            // ====== Contraste inverso: facturas en MAYORES (41*, salvo 4109) no están en PRINEX ======
            var faltantesInverso = new List<(string Factura, decimal Importe)>();

            foreach (var kv in dMay)
            {
                var mk = kv.Key;
                var importe = kv.Value;
                if (!dSrc.ContainsKey(mk))
                {
                    // Mostrar el mismo display que arriba (documento o concepto)
                    string disp = dMayDisp.TryGetValue(mk, out var v) ? v : mk;
                    faltantesInverso.Add((disp, importe));
                }
            }


            // ====== Crear hoja de salida ======
            var wsOut = wb.Worksheets.FirstOrDefault(s => s.Name == "Comparación");
            if (wsOut != null) wsOut.Delete();
            wsOut = wb.Worksheets.Add("Comparación");

            int rowOut = 1;

            // Banner Sociedad
            int rSoc = rowOut;
            var rngSoc = wsOut.Range($"A{rSoc}:D{rSoc}");
            rngSoc.Merge();
            rngSoc.Value = !string.IsNullOrEmpty(sociedadClave) ? $"Sociedad: {sociedadClave}" : "Sociedad: -";
            rngSoc.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            rngSoc.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
            rngSoc.Style.Fill.BackgroundColor = XLColor.FromArgb(204, 187, 221);
            rngSoc.Style.Font.Bold = true;
            rngSoc.Style.Font.FontColor = XLColor.FromArgb(60, 45, 75);
            rngSoc.Style.Font.FontSize = 18;
            wsOut.Row(rSoc).Height = 28;

            // Mes/Año
            rowOut++;
            var rFecha = wsOut.Range($"A{rowOut}:D{rowOut}");
            rFecha.Merge();
            if (haveYmd)
                rFecha.Value = new DateTime(yy, mm, 1).ToString("MMMM yyyy", new CultureInfo("es-ES"));
            else
                rFecha.Value = "Periodo";
            rFecha.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            rFecha.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
            rFecha.Style.Fill.BackgroundColor = XLColor.FromArgb(210, 232, 255);
            rFecha.Style.Font.Bold = true;
            rFecha.Style.Font.FontSize = 15;
            rFecha.Style.Font.FontColor = XLColor.FromArgb(40, 60, 85);
            wsOut.Row(rowOut).Height = 24;

            // Línea en blanco
            rowOut++;

            // Faltantes
            rowOut++;
            wsOut.Cell(rowOut, 1).Value = "Facturas en PRINEX que NO están en MAYORES";
            rowOut++;
            wsOut.Cell(rowOut, 1).Value = "Nº Factura";
            wsOut.Cell(rowOut, 2).Value = "Importe PRINEX";

            int faltantes = 0;
            foreach (var k in dSrc.Keys.OrderBy(k => k))
            {
                if (!dMay.ContainsKey(k))
                {
                    rowOut++;
                    string show = dSrcDisp.TryGetValue(k, out var v) ? v : k;
                    wsOut.Cell(rowOut, 1).Value = show;
                    wsOut.Cell(rowOut, 2).Value = dSrc[k];
                    faltantes++;
                }
            }

            int f1 = rowOut; // fin bloque faltantes

            // Descuadres
            rowOut += 2;
            int rDescHdr = rowOut;
            wsOut.Cell(rowOut, 1).Value = "Facturas presentes en ambos con DESCUADRE";
            rowOut++;
            wsOut.Cell(rowOut, 1).Value = "Nº Factura";
            wsOut.Cell(rowOut, 2).Value = "Importe PRINEX";
            wsOut.Cell(rowOut, 3).Value = "Haber MAYORES";
            wsOut.Cell(rowOut, 4).Value = "Diferencia";

            int descuadres = 0;
            foreach (var k in dSrc.Keys.OrderBy(k => k))
            {
                if (dMay.ContainsKey(k))
                {
                    if (Math.Round(dSrc[k], 2) != Math.Round(dMay[k], 2))
                    {
                        rowOut++;
                        string show2 = dSrcDisp.TryGetValue(k, out var v) ? v :
                                       (dMayDisp.TryGetValue(k, out var v2) ? v2 : k);
                        wsOut.Cell(rowOut, 1).SetValue("'" + show2);
                        wsOut.Cell(rowOut, 2).Value = dSrc[k];
                        wsOut.Cell(rowOut, 3).Value = dMay[k];
                        wsOut.Cell(rowOut, 4).Value = dSrc[k] - dMay[k];
                        descuadres++;
                    }
                }
            }

            int f2 = rowOut;
            // ====== Bloque inverso ======
            rowOut += 2;
            wsOut.Cell(rowOut, 1).Value = "Facturas que SÍ están en contabilidad pero NO están en PRINEX";


            rowOut++;
            wsOut.Cell(rowOut, 1).Value = "Nº Factura";
            wsOut.Cell(rowOut, 2).Value = "Importe MAYORES";

            foreach (var item in faltantesInverso.OrderBy(i => i.Factura))
            {
                rowOut++;
                wsOut.Cell(rowOut, 1).Value = item.Factura;
                wsOut.Cell(rowOut, 2).Value = item.Importe;
            }

            // Mensaje final (como la macro)
            int totalInc = faltantes + descuadres;
            string titulo = "Comparación de facturas";

            wsOut.Cell(1, 1).Value = $"Sociedad: {(string.IsNullOrEmpty(sociedadClave) ? "-" : sociedadClave)} — {titulo}";

            // Colocar título arriba (reaprovechamos A1)
            wsOut.Cell(1, 1).Value = $"Sociedad: {(string.IsNullOrEmpty(sociedadClave) ? "-" : sociedadClave)} — {titulo}";

            // Estilo final
            StyleComparacionCutePro(wsOut);

            // Formatos monetarios
            var euroFmt = "#,##0.00 [$€-es-ES]";
            // Faltantes: desde primera fila de datos de faltantes
            int faltStart = rDescHdr > 5 ? 6 : 6; // seguro a partir de cabeceras
            // Localiza los rangos reales
            // Datos faltantes
            var faltStartRow = FindFirstDataBelow(wsOut, "NO están en MAYORES") + 2;
            var faltEndRow = FindLastDataInTwoCols(wsOut, faltStartRow, 1, 2);
            if (faltEndRow >= faltStartRow)
            {
                wsOut.Range(faltStartRow, 2, faltEndRow, 2).Style.NumberFormat.Format = euroFmt;
            }
            // Datos descuadres
            var descStartRow = FindFirstDataBelow(wsOut, "DESCUADRE") + 2;
            var descEndRow = FindLastDataInTwoCols(wsOut, descStartRow, 1, 4);
            if (descEndRow >= descStartRow)
            {
                wsOut.Range(descStartRow, 2, descEndRow, 4).Style.NumberFormat.Format = euroFmt;

                // Condicional en diferencia (col D)
                var rng = wsOut.Range(descStartRow, 4, descEndRow, 4);
                rng.AddConditionalFormat().WhenGreaterThan(0).Font.SetFontColor(XLColor.FromArgb(0, 130, 0));
                rng.AddConditionalFormat().WhenLessThan(0).Font.SetFontColor(XLColor.FromArgb(200, 0, 0));
                rng.AddConditionalFormat().WhenEquals(0).Font.SetFontColor(XLColor.FromArgb(90, 90, 90));
            }

            wsOut.Columns("A:D").AdjustToContents();
            // Formato europeo uniforme en todas las columnas numéricas excepto A
            wsOut.Columns("B:D").Style.NumberFormat.Format = "#,##0.00 [$€-es-ES]";
            wsOut.Column(4).Style.Font.FontColor = XLColor.FromArgb(192, 0, 0);

            // Forzar diferencia (columna D) siempre en rojo
            wsOut.Column(4).Style.Font.FontColor = XLColor.FromArgb(192, 0, 0);

            // Quitar cuadrícula y dejar “Comparación” como hoja activa
            // Quitar cuadrícula y dejar “Comparación” visible sin selección fea
            wsOut.ShowGridLines = false;
            wsOut.SheetView.SetView(XLSheetViewOptions.Normal);
            wsOut.Cell(1, 1).Select();

            // Hacer que “Comparación” sea la hoja activa al abrir
            wb.Worksheet("Comparación").SetTabActive();


            // Guardar y devolver
            using var outMs = new MemoryStream();
            wb.SaveAs(outMs);
            return await Task.FromResult((outMs.ToArray(), faltantesInverso));


        }

        // ============================================================
        // =================== ESTILO “CUTE PRO” ======================
        // ============================================================

        private static void StyleComparacionCutePro(IXLWorksheet ws)
        {
            // Base tipográfica
            ws.Style.Font.FontName = "Calibri";
            ws.Style.Font.FontSize = 11;
            ws.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
            ws.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

            // Ocultar cuadrícula (se respeta en Excel al abrir)
            // (ClosedXML no controla DisplayGridlines al abrir, lo dejamos así)

            // Banner cabecera reaplicado
            var rngTitle = ws.Range("A1:D1");
            rngTitle.Merge();
            rngTitle.Style.Font.FontSize = 15;
            rngTitle.Style.Font.Bold = true;
            rngTitle.Style.Font.FontColor = XLColor.FromArgb(40, 60, 85);
            rngTitle.Style.Fill.BackgroundColor = XLColor.FromArgb(204, 187, 221);
            rngTitle.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Row(1).Height = 24;

            // Secciones
            var rFalt = FindRowContains(ws, "NO están en MAYORES");
            var rDesc = FindRowContains(ws, "DESCUADRE");
            var rInv = FindRowContains(ws, "contabilidad pero NO están en PRINEX"); // nueva sección inversa

            if (rFalt > 0) StyleSectionHeader(ws.Range($"A{rFalt}:D{rFalt}"), XLColor.FromArgb(228, 241, 228));
            if (rDesc > 0) StyleSectionHeader(ws.Range($"A{rDesc}:D{rDesc}"), XLColor.FromArgb(228, 241, 228));
            if (rInv > 0) StyleSectionHeader(ws.Range($"A{rInv}:D{rInv}"), XLColor.FromArgb(228, 241, 228));

            // Cajas suaves: simulamos con bordes
            if (rFalt > 0)
            {
                int fStart = rFalt + 1;
                int fEnd = FindLastDataInTwoCols(ws, fStart + 1, 1, 2);
                if (fEnd >= fStart)
                    MakeSoftBox(ws, ws.Range($"A{rFalt}:D{fEnd}"));
                StyleTableHeader(ws.Range($"A{rFalt + 1}:D{rFalt + 1}"));
                ApplyZebra(ws, ws.Range($"A{fStart + 1}:D{fEnd}"));
            }

            if (rDesc > 0)
            {
                int dStart = rDesc + 1;
                int dEnd = FindLastDataInTwoCols(ws, dStart + 1, 1, 4);
                if (dEnd >= dStart)
                    MakeSoftBox(ws, ws.Range($"A{rDesc}:D{dEnd}"));
                StyleTableHeader(ws.Range($"A{rDesc + 1}:D{rDesc + 1}"));
                ApplyZebra(ws, ws.Range($"A{dStart + 1}:D{dEnd}"));
            }
            if (rInv > 0)
            {
                int iStart = rInv + 1;
                int iEnd = FindLastDataInTwoCols(ws, iStart + 1, 1, 2);
                if (iEnd >= iStart)
                    MakeSoftBox(ws, ws.Range($"A{rInv}:D{iEnd}"));
                StyleTableHeader(ws.Range($"A{rInv + 1}:D{rInv + 1}"));
                ApplyZebra(ws, ws.Range($"A{iStart + 1}:D{iEnd}"));
            }

            // Centrar columnas A
            ws.Column(1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
        }

        private static void StyleSectionHeader(IXLRange rng, XLColor bg)
        {
            rng.Style.Font.Bold = true;
            rng.Style.Font.FontColor = XLColor.FromArgb(40, 60, 85);
            rng.Style.Fill.BackgroundColor = bg;
            foreach (var r in rng.Rows())
            {
                foreach (var rowCell in r.Cells())
                {
                    var row = rowCell.WorksheetRow();
                    row.Height = 24;
                    break; // con una celda basta para fijar la altura de esa fila
                }
            }


        }

        private static void StyleTableHeader(IXLRange rng)
        {
            rng.Style.Font.Bold = true;
            rng.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            rng.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
            rng.Style.Fill.BackgroundColor = XLColor.FromArgb(210, 225, 210);
            rng.Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
            rng.Style.Border.InsideBorder = XLBorderStyleValues.Thin;
            rng.Style.Border.OutsideBorderColor = XLColor.FromArgb(200, 210, 200);
            rng.Style.Border.InsideBorderColor = XLColor.FromArgb(200, 210, 200);
            foreach (var r in rng.Rows())
            {
                foreach (var rowCell in r.Cells())
                {
                    var row = rowCell.WorksheetRow();
                    row.Height = 20;
                    break;
                }
            }


        }

        private static void MakeSoftBox(IXLWorksheet ws, IXLRange rng)
        {
            // simulamos la “caja” con borde alrededor
            var outer = rng;
            outer.Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
            outer.Style.Border.OutsideBorderColor = XLColor.FromArgb(215, 223, 235);
        }

        private static void ApplyZebra(IXLWorksheet ws, IXLRange rng)
        {
            int i = 1;
            foreach (var row in rng.Rows())
            {
                if (i % 2 == 0)
                    row.Style.Fill.BackgroundColor = XLColor.FromArgb(248, 251, 253);
                i++;
            }
        }

        private static int FindRowContains(IXLWorksheet ws, string contains)
        {
            foreach (var cell in ws.Column(1).CellsUsed())
            {
                if (cell.GetString().IndexOf(contains, StringComparison.OrdinalIgnoreCase) >= 0)
                    return cell.Address.RowNumber;
            }
            return 0;
        }

        private static int FindFirstDataBelow(IXLWorksheet ws, string contains)
        {
            int r = FindRowContains(ws, contains);
            return r > 0 ? r + 2 : 0;
        }

        private static int FindLastDataInTwoCols(IXLWorksheet ws, int startRow, int col1, int col2)
        {
            int r = startRow;
            int last = startRow - 1;
            while (true)
            {
                var has = !string.IsNullOrWhiteSpace(ws.Cell(r, col1).GetString())
                          || !string.IsNullOrWhiteSpace(ws.Cell(r, col2).GetString());
                if (!has) break;
                last = r;
                r++;
                if (r > ws.LastRowUsed().RowNumber()) break;
            }
            return last;
        }

        // ============================================================
        // =================== HELPERS DE LECTURA =====================
        // ============================================================

        private static int FindHeaderRow(IXLWorksheet ws, IEnumerable<string> requiredHeaders, int maxScanRows = 60)
        {
            for (int r = 1; r <= maxScanRows; r++)
            {
                bool ok = true;
                foreach (var h in requiredHeaders)
                {
                    if (FindHeaderCol(ws, r, h) == 0) { ok = false; break; }
                }
                if (ok) return r;
            }
            return 0;
        }

        private static int FindHeaderCol(IXLWorksheet ws, int headerRow, string headerName)
        {
            var row = ws.Row(headerRow);
            foreach (var cell in row.CellsUsed())
            {
                if (string.Equals(cell.GetString(), headerName, StringComparison.OrdinalIgnoreCase))
                    return cell.Address.ColumnNumber;
            }
            // búsqueda laxa por si hay espacios
            int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
            for (int c = 1; c <= lastCol; c++)
            {
                if (string.Equals(ws.Cell(headerRow, c).GetString().Trim(), headerName, StringComparison.OrdinalIgnoreCase))
                    return c;
            }
            return 0;
        }

        private static decimal SafeNumber(object v)
        {
            if (v is null) return 0m;
            if (v is double d) return (decimal)d;
            if (v is float f) return (decimal)f;
            if (v is decimal m) return m;
            if (v is int i) return i;
            if (v is long l) return l;

            var s = Convert.ToString(v) ?? string.Empty;
            s = s.Trim();

            // Detecta si la coma parece separador decimal
            if (s.Contains(',') && s.LastIndexOf(',') > s.LastIndexOf('.'))
            {
                s = s.Replace(".", "").Replace(",", ".");
            }
            else
            {
                s = s.Replace(",", "");
            }

            if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var res))
                return res;

            // Segundo intento: cultura española
            if (decimal.TryParse(s, NumberStyles.Any, new CultureInfo("es-ES"), out res))
                return res;

            return 0m;

            
        }

        private static bool IsSameMonth(object dt, int y, int m)
        {
            if (dt is DateTime dd) return dd.Year == y && dd.Month == m;
            if (DateTime.TryParse(Convert.ToString(dt), out var d2)) return d2.Year == y && d2.Month == m;
            return false;
        }

        private static string RemoveDiacritics(string s)
        {
            return s
                .Replace("Á", "A").Replace("É", "E").Replace("Í", "I").Replace("Ó", "O").Replace("Ú", "U").Replace("Ü", "U")
                .Replace("á", "a").Replace("é", "e").Replace("í", "i").Replace("ó", "o").Replace("ú", "u").Replace("ü", "u")
                .Replace("Ñ", "N").Replace("ñ", "n");
        }

        private static string NormalizePhase(object s)
        {
            return RemoveDiacritics(Convert.ToString(s) ?? string.Empty).Trim().ToUpperInvariant();
        }

        private static string DetectSociedadClave(string fileName)
        {
            var up = RemoveDiacritics(fileName).ToUpperInvariant();
            var keys = new[] { "TRAVIA", "ELIA", "MECA", "CRAW", "TORQ", "MIDF", "GARDEN", "BAILEN" };
            foreach (var k in keys) if (up.Contains(k)) return k;
            return string.Empty;
        }

        private static bool CellContainsSociedad(object v, string clave)
        {
            if (string.IsNullOrEmpty(clave)) return true;
            var up = RemoveDiacritics(Convert.ToString(v) ?? string.Empty).ToUpperInvariant();
            return up.Contains(clave);
        }

        private static (bool ok, int y, int m, int d) ExtractYMDFromFileName(string fileName)
        {
            // primeros 6 dígitos consecutivos AA MM DD
            var m1 = Regex.Match(fileName, @"(\d{6})");
            if (!m1.Success) return (false, 0, 0, 0);
            var s = m1.Groups[1].Value;
            int y = 2000 + int.Parse(s.Substring(0, 2));
            int mm = int.Parse(s.Substring(2, 2));
            int d = int.Parse(s.Substring(4, 2));
            if (mm >= 1 && mm <= 12 && d >= 1 && d <= 31) return (true, y, mm, d);
            return (false, 0, 0, 0);
        }

        // ============================================================
        // =============== TOKENIZACIÓN / NORMALIZACIÓN ===============
        // ============================================================

        private static string ExtractNumToken(object cellValue)
        {
            var txt = Convert.ToString(cellValue) ?? string.Empty;
            txt = txt.Replace(((char)160).ToString(), " ").Replace("\r", " ").Replace("\n", " ").Trim();

            // A) "dd mm yyyy"
            var m = Regex.Match(txt, @"\b\d{2}\s+\d{2}\s+\d{4}\b", RegexOptions.IgnoreCase);
            if (m.Success) return m.Value;

            // 1) 2/4/6 con barras
            m = Regex.Match(txt, @"\d{2}[\/\u2215\uFF0F]\d{4}[\/\u2215\uFF0F]\d{6}", RegexOptions.IgnoreCase);
            if (m.Success) return m.Value;

            // 1B) REF + 6 dígitos
            m = Regex.Match(txt, @"(?:^|[\s'\-])REF\s*(\d{6})(?:\b|$)", RegexOptions.IgnoreCase);
            if (m.Success) return "REF" + m.Groups[1].Value;

            // 2) dígito–guion–dígito
            m = Regex.Match(txt, @"(\d{3,}\s*[–—-]\s*\d{2,})", RegexOptions.IgnoreCase);
            if (m.Success) return m.Value.Trim();

            // 3) genérico con separadores — escoger “mejor”
            var mc = Regex.Matches(txt, @"(\d[\d\/\.\-\u2215\uFF0F]*\d)", RegexOptions.IgnoreCase);
            if (mc.Count > 0)
            {
                string best = ""; int bestDigits = -1; int bestSlashes = -1;
                foreach (Match mm in mc)
                {
                    string tmp = mm.Value;
                    string onlyDigits = tmp.Replace("/", "").Replace(".", "").Replace("-", "")
                        .Replace(((char)0x2215).ToString(), "").Replace(((char)0xFF0F).ToString(), "");
                    int curDigits = onlyDigits.Count(char.IsDigit);
                    int curSlashes = tmp.Count(ch => ch == '/' || ch == (char)0x2215 || ch == (char)0xFF0F);
                    if (curDigits > bestDigits || (curDigits == bestDigits && curSlashes > bestSlashes))
                    {
                        best = tmp; bestDigits = curDigits; bestSlashes = curSlashes;
                    }
                }
                return best;
            }

            // 4) fallback
            var sp = txt.IndexOf(' ');
            return sp > 0 ? txt.Substring(0, sp) : txt;
        }

        private static string MakeDisplayKey(string token)
        {
            return (token ?? string.Empty).Trim().Replace("\t", "");
        }

        private static string MakeMatchKey(string token)
        {
            var t = (token ?? string.Empty).Trim();

            // 1) fecha con espacios "dd mm yyyy" -> ddmmyy
            var m = Regex.Match(t, @"(?:^|[^\d])(\d{2})\s+(\d{2})\s+(\d{4})(?=\D|$)", RegexOptions.IgnoreCase);
            if (m.Success) return $"{m.Groups[1].Value}{m.Groups[2].Value}{m.Groups[3].Value.Substring(2)}";

            // 2) REF + 6 dígitos -> ddmmyy (mantiene 6)
            m = Regex.Match(t, @"(?:^|[^\w])REF\s*(\d{6})(?=\D|$)", RegexOptions.IgnoreCase);
            if (m.Success) return m.Groups[1].Value;

            // 3) 6 dígitos ddmmyy en cualquier parte
            m = Regex.Match(t, @"(?:^|[^\d])(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])(\d{2})(?=\D|$)", RegexOptions.IgnoreCase);
            if (m.Success) return $"{m.Groups[1].Value}{m.Groups[2].Value}{m.Groups[3].Value}";

            // 4) 1835-2025 -> primer bloque
            if (!t.Contains("/") && !t.Contains(((char)0x2215)) && !t.Contains(((char)0xFF0F)))
            {
                m = Regex.Match(t, @"^\s*(\d{3,})\s*[–—-]\s*\d+", RegexOptions.IgnoreCase);
                if (m.Success) return m.Groups[1].Value;
            }

            // 5) sólo dígitos
            var only = Regex.Replace(t, @"[^\d]+", "");
            return only;
        }

        private static bool HasRef6(string s) =>
            Regex.IsMatch(s ?? "", @"(?:^|[^\w])REF\s*\d{6}(?=\D|$)", RegexOptions.IgnoreCase);
        private static bool HasDateSpaces(string s) =>
            Regex.IsMatch(s ?? "", @"(?:^|[^\d])\d{2}\s+\d{2}\s+\d{4}(?=\D|$)", RegexOptions.IgnoreCase);

        private static bool IsMixedSlashDash(string s)
        {
            var t = s ?? "";
            return t.Contains("/") && t.Contains("-");
        }

        private static int CountDigits(string s) => (s ?? "").Count(char.IsDigit);
        private static int CountSlashes(string s) => (s ?? "").Count(ch => ch == '/' || ch == (char)0x2215 || ch == (char)0xFF0F);

        private static bool IsRangeLikeToken(string s)
        {
            var txt = (s ?? "").Replace(" ", "").Replace("–", "-").Replace("—", "-");
            if (!txt.Contains("/") && !txt.Contains(((char)0x2215)) && !txt.Contains(((char)0xFF0F)))
            {
                return Regex.IsMatch(txt, @"^\d{3,4}\-\d{3,4}(\b|$)", RegexOptions.IgnoreCase);
            }
            return false;
        }

        private static int MinNeedForToken(string t)
        {
            var s = (t ?? "").Trim().ToUpperInvariant();
            if (s.StartsWith("REF")) return 6;
            if (Regex.IsMatch(s, @"^\s*\d{1,2}\s*[/\.\- _]\s*\d{1,2}\s*[/\.\- _]\s*\d{2,4}\s*$", RegexOptions.IgnoreCase))
                return 6;
            return MIN_DIGITS;
        }

        private static string PickBestInvoiceToken(string rawDoc, string rawConc)
        {
            string tDoc = ExtractNumToken(rawDoc);
            string tConc = ExtractNumToken(rawConc);

            if (IsMixedSlashDash(tDoc)) tDoc = "";
            if (IsMixedSlashDash(tConc)) tConc = "";

            if (!string.IsNullOrEmpty(tDoc) && HasRef6(tDoc)) return tDoc;
            if (!string.IsNullOrEmpty(tConc) && HasRef6(tConc)) return tConc;

            if (!string.IsNullOrEmpty(tDoc) && HasDateSpaces(tDoc)) return tDoc;
            if (!string.IsNullOrEmpty(tConc) && HasDateSpaces(tConc)) return tConc;

            // Guion entre bloques numéricos
            var reDash = new Regex(@"^\s*\d{3,}\s*[–—-]\s*\d+\b", RegexOptions.IgnoreCase);
            if (!string.IsNullOrEmpty(tConc) && reDash.IsMatch(tConc)) return tConc;
            if (!string.IsNullOrEmpty(tDoc) && reDash.IsMatch(tDoc)) return tDoc;

            // 2/4/6
            var re246 = new Regex(@"^\d{2}[\/\u2215\uFF0F]\d{4}[\/\u2215\uFF0F]\d{6}$", RegexOptions.IgnoreCase);
            if (!string.IsNullOrEmpty(tConc) && re246.IsMatch(tConc)) return tConc;
            if (!string.IsNullOrEmpty(tDoc) && re246.IsMatch(tDoc)) return tDoc;

            // Comparar por nº dígitos / barras
            int dDoc = CountDigits(MakeMatchKey(tDoc));
            int dConc = CountDigits(MakeMatchKey(tConc));
            int sDoc = CountSlashes(tDoc);
            int sConc = CountSlashes(tConc);

            if (dDoc < MinNeedForToken(tDoc) || dDoc > MAX_DIGITS || IsRangeLikeToken(tDoc)) { tDoc = ""; dDoc = 0; }
            if (dConc < MinNeedForToken(tConc) || dConc > MAX_DIGITS || IsRangeLikeToken(tConc)) { tConc = ""; dConc = 0; }

            if (string.IsNullOrEmpty(tDoc) && !string.IsNullOrEmpty(tConc)) return tConc;
            if (string.IsNullOrEmpty(tConc) && !string.IsNullOrEmpty(tDoc)) return tDoc;
            if (string.IsNullOrEmpty(tDoc) && string.IsNullOrEmpty(tConc)) return "";

            if (dConc > dDoc) return tConc;
            if (dDoc > dConc) return tDoc;
            return sConc >= sDoc ? tConc : tDoc;
        }

        private static string FindBestTokenInRow2(IXLWorksheet ws, int r,
            int cCuenta, int cFecha, int cDebe, int cHaber, int cDoc, int cConc,
            out int bestCol, out string bestHeader)
        {
            bestCol = 0; bestHeader = "";
            int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
            string best = ""; int bestDigits = -1;

            for (int c = 1; c <= lastCol; c++)
            {
                if (c == cCuenta || c == cFecha || c == cDebe || c == cHaber || c == cDoc || c == cConc) continue;

                var raw = ws.Cell(r, c).GetString();
                if (string.IsNullOrWhiteSpace(raw)) continue;

                var tok = ExtractNumToken(raw);
                if (string.IsNullOrEmpty(tok)) continue;

                bool is246 = Regex.IsMatch(tok, @"^\d{2}[\/\u2215\uFF0F]\d{4}[\/\u2215\uFF0F]\d{6}$");
                int d = MakeMatchKey(tok).Length;
                int minNeed = MinNeedForToken(tok);
                if (is246 || ((d >= minNeed && d <= MAX_DIGITS) && !IsRangeLikeToken(tok)))
                {
                    if (d > bestDigits)
                    {
                        best = tok; bestDigits = d; bestCol = c;
                        bestHeader = ws.Cell(FindHeaderRow(ws, new[] { "Cuenta", "Fecha" }, 60), c).GetString();
                    }
                }
            }
            return best;
        }
    
    // ============================================================
// =============== CORS: DETECTAR ORIGEN PERMITIDO =============
// ============================================================

private static string? GetAllowedOrigin(HttpRequestData req)
        {
            // Leer el Origin que viene desde el navegador
            var origin = req.Headers.TryGetValues("Origin", out var vals) ? vals.FirstOrDefault() : null;

            // Lista de orígenes permitidos (ajusta el puerto a tu frontend)
            var allowed = new[] { "http://localhost:7252", "https://localhost:7252" };

            if (!string.IsNullOrEmpty(origin) &&
                allowed.Contains(origin, StringComparer.OrdinalIgnoreCase))
            {
                return origin;
            }

            return null; // si no está permitido, devolvemos null
        }
    
    
    }
}
