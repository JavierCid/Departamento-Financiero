using ClosedXML.Excel;
using DocumentFormat.OpenXml.Wordprocessing;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;


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

        // === Vista previa cuadre PDFs ↔ Prinex ===
        private sealed class PreviewPdfDto
        {
            public List<PdfCoincidencia> Coincidencias { get; set; } = new();
            public List<string> Faltantes { get; set; } = new();
            public List<string> Descuadres { get; set; } = new();
        }

        private sealed class PdfCoincidencia
        {
            public string? Documento { get; set; }
            public string? CoincidenciaDetectada { get; set; }
        }

        // Payload que viene de Blazor (PDFs + Excel en base64)
        private sealed class CuadreRequest
        {
            public List<CuadreFile> Pdfs { get; set; } = new();
            public CuadreFile? Excel { get; set; }
            public string? SociedadFiltro { get; set; }
        }


        private sealed class CuadreFile
        {
            public string Name { get; set; } = string.Empty;
            public string Base64 { get; set; } = string.Empty;
        }



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

        // ============================================================
        // ======= HTTP API: cuadro facturación ↔ Prinex (PDFs) ========
        // ============================================================
        [Function("ContrasteFacturas")]
        public async Task<HttpResponseData> ContrasteFacturas(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", "options", Route = "contraste-facturas")]
            HttpRequestData req)
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
                using var reader = new StreamReader(req.Body);
                var body = await reader.ReadToEndAsync();

                var data = System.Text.Json.JsonSerializer.Deserialize<CuadreRequest>(
                    body,
                    new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (data is null || data.Pdfs is null || data.Pdfs.Count == 0 || data.Excel is null)
                {
                    var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                    await bad.WriteStringAsync("Faltan PDFs o el Excel en la petición.");
                    AddCors(bad, req);
                    return bad;
                }

                var excelBytes = Convert.FromBase64String(data.Excel.Base64);
                var preview = BuildPreviewFromPdfNamesAndExcel(excelBytes, data.Pdfs, data.SociedadFiltro);


                var resp = req.CreateResponse(HttpStatusCode.OK);
                resp.Headers.Add("X-Preview", System.Text.Json.JsonSerializer.Serialize(preview));

                // El front sólo usa la cabecera
                await resp.WriteStringAsync("OK");
                AddCors(resp, req);
                return resp;
            }
            catch (Exception ex)
            {
                var err = req.CreateResponse(HttpStatusCode.InternalServerError);
                await err.WriteStringAsync($"Error procesando PDFs/Excel: {ex.Message}");
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
        // ======== LÓGICA CUADRE PDFs ↔ PRINEX (con repesca proveedor+número) ========
        private static PreviewPdfDto BuildPreviewFromPdfNamesAndExcel(
            byte[] excelBytes,
            List<CuadreFile> pdfFiles)
        {
            var result = new PreviewPdfDto();

            using var ms = new MemoryStream(excelBytes);
            using var wb = new XLWorkbook(ms);

            // Hoja base (Sheet1 / Hoja1 / primera)
            var ws = wb.Worksheets.FirstOrDefault(s => string.Equals(s.Name, "Sheet1", StringComparison.OrdinalIgnoreCase))
                     ?? wb.Worksheets.FirstOrDefault(s => string.Equals(s.Name, "Hoja1", StringComparison.OrdinalIgnoreCase))
                     ?? wb.Worksheets.First();

            // Intentamos localizar cabeceras mínimas
            var reqSrc = new[] { "S/Fra. Número", "Importe" };
            int headerRow = FindHeaderRow(ws, reqSrc, 60);
            if (headerRow == 0)
            {
                // fallback: asumimos fila 1
                headerRow = 1;
            }

            int cFra = FindHeaderCol(ws, headerRow, "S/Fra. Número");
            if (cFra == 0)
            {
                // Fallback muy laxo: primera columna cuyo título contenga "fra" o "factura"
                int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
                for (int c = 1; c <= lastCol && cFra == 0; c++)
                {
                    var h = ws.Cell(headerRow, c).GetString().Trim();
                    var hUp = RemoveDiacritics(h).ToUpperInvariant();
                    if (hUp.Contains("FRA") || hUp.Contains("FACTURA"))
                        cFra = c;
                }

                if (cFra == 0)
                    throw new InvalidOperationException("No se encuentra la columna de número de factura en el Excel de Prinex.");
            }

            int cImporte = FindHeaderCol(ws, headerRow, "Importe");

            // Columna Concepto (opcional, para repesca avanzada)
            int cConcepto = FindHeaderCol(ws, headerRow, "Concepto");
            if (cConcepto == 0)
            {
                int lastColConcept = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
                for (int c = 1; c <= lastColConcept && cConcepto == 0; c++)
                {
                    var h = ws.Cell(headerRow, c).GetString();
                    var hUp = RemoveDiacritics(h).ToUpperInvariant();
                    if (hUp.Contains("CONCEPTO") || hUp.Contains("DESCRIPCION"))
                        cConcepto = c;
                }
            }


            // Columna Nombre proveedor (búsqueda flexible)
            int cProveedor = FindHeaderCol(ws, headerRow, "Nombre proveedor");
            if (cProveedor == 0)
            {
                int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
                for (int c = 1; c <= lastCol && cProveedor == 0; c++)
                {
                    var h = ws.Cell(headerRow, c).GetString();
                    var hUp = RemoveDiacritics(h).ToUpperInvariant();
                    if (hUp.Contains("PROVEEDOR"))
                        cProveedor = c;
                }
            }

            // Columna Fecha Fra. (búsqueda flexible)
            int cFechaFra = FindHeaderCol(ws, headerRow, "Fecha Fra.");
            if (cFechaFra == 0)
            {
                int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
                for (int c = 1; c <= lastCol && cFechaFra == 0; c++)
                {
                    var h = ws.Cell(headerRow, c).GetString();
                    var hUp = RemoveDiacritics(h).ToUpperInvariant();
                    if (hUp.Contains("FECHA") && (hUp.Contains("FRA") || hUp.Contains("FACTURA")))
                        cFechaFra = c;
                }
            }

            // ==== Índice de Prinex por clave simple (bloque numérico largo) ====
            var prinex = new Dictionary<string, (string Display, decimal Importe)>(StringComparer.Ordinal);
            // Info extendida por fila para repesca (aquí podemos ser más generosos)
            var prinexRows = new List<(string Key, string RawFra, string ProviderNorm, string ProviderDisplay, string ConceptNorm, string ConceptDisplay, object? FechaRaw, decimal Importe)>();

            int lastRow = ws.LastRowUsed()?.RowNumber() ?? headerRow;

            for (int r = headerRow + 1; r <= lastRow; r++)
            {
                var numRaw = ws.Cell(r, cFra).GetString();
                var key = SimpleInvoiceKey(numRaw); // sigue siendo estricta (bloque numérico largo)

                decimal importe = 0m;
                if (cImporte > 0)
                {
                    try
                    {
                        importe = SafeNumber(ws.Cell(r, cImporte).Value);
                    }
                    catch
                    {
                        // Si el importe da problemas, lo dejamos a 0 pero no rompemos el contraste
                        importe = 0m;
                    }
                }

                string provDisplay = cProveedor > 0 ? ws.Cell(r, cProveedor).GetString().Trim() : string.Empty;
string provNorm = RemoveDiacritics(provDisplay).ToUpperInvariant();

// Alias proveedores: cualquier MOMENTUM REAL ESTATE lo tratamos como "MTM"
if (!string.IsNullOrEmpty(provNorm) && provNorm.Contains("MOMENTUM"))
{
    provDisplay = "MTM";
    provNorm = "MTM";
}


                string conceptDisplay = cConcepto > 0 ? ws.Cell(r, cConcepto).GetString().Trim() : string.Empty;
                string conceptNorm = RemoveDiacritics(conceptDisplay).ToUpperInvariant();

                object? fechaRaw = cFechaFra > 0 ? (object?)ws.Cell(r, cFechaFra).Value : null;

                // Para la pesca principal (lista "En Prinex pero sin PDF") mantenemos la clave estricta
                if (!string.IsNullOrEmpty(key) && !prinex.ContainsKey(key))
                {
                    prinex[key] = (numRaw.Trim(), importe);
                }

                // Para las repescas SIEMPRE guardamos la fila,
                // aunque SimpleInvoiceKey no haya encontrado un bloque ≥5 dígitos (key vacío)
                prinexRows.Add((key ?? string.Empty,
                                numRaw.Trim(),
                                provNorm,
                                provDisplay,
                                conceptNorm,
                                conceptDisplay,
                                fechaRaw,
                                importe));
            }


            // ==== PDFs: PRIMERA PASADA (NO se toca la lógica de clave simple) ====
            var pdfKeys = new Dictionary<string, string>(StringComparer.Ordinal);

            // Pendientes para repesca (solo NO encontrados en la primera pasada)
            var pendientes = new List<(CuadreFile? Pdf, string? Name, string? BaseName, string? FaltanteMsg)>();


            foreach (var pdf in pdfFiles)
            {
                var name = pdf?.Name ?? string.Empty;
                var baseName = Path.GetFileNameWithoutExtension(name) ?? string.Empty;

                var key = SimpleInvoiceKey(baseName);
                if (string.IsNullOrEmpty(key))
                {
                    // No se pudo extraer número de factura → candidato a repesca
                    pendientes.Add((pdf, name, baseName, $"{name} → No constan cadenas válidas"));
                    continue;
                }

                pdfKeys[key] = name;

                if (prinex.TryGetValue(key, out var info))
                {
                    var tokenFactura = $"<em>{key}</em> (factura)";

                    result.Coincidencias.Add(new PdfCoincidencia
                    {
                        Documento = name,
                        CoincidenciaDetectada = $"Cadena {tokenFactura}"
                    });
                }




                else
                {
                    // No encontrado en Prinex por clave simple → candidato a repesca
                    // Motivo por defecto: no se han encontrado coincidencias
                    pendientes.Add((pdf, name, baseName, $"{name} → No se han encontrado coincidencias"));
                }

            }
            

            // ==== SEGUNDA PASADA: REPESCA SOLO SOBRE 'pendientes' ====
            if (pendientes.Count > 0 && prinexRows.Count > 0 && cProveedor > 0)
            {
                var stillFaltantes = new List<string>();

                foreach (var item in pendientes)
                {
                    var name = item.Name ?? string.Empty;
                    var baseName = item.BaseName ?? string.Empty;
                    var faltMsg = item.FaltanteMsg;

                    // NUEVO: ya no recortamos por el primer '_'; usamos todo el nombre
                    var provTextNorm = RemoveDiacritics(baseName).ToUpperInvariant();

                    // Si el nombre del archivo contiene "MTM", lo tratamos como si incluyera "MOMENTUM REAL ESTATE S.L."
                    if (provTextNorm.Contains("MTM"))
                    {
                        provTextNorm = provTextNorm.Replace("MTM", "MOMENTUM REAL ESTATE");
                    }




                    // Tokens base de proveedor: bloques A–Z de longitud ≥ 5
                    // Tokens base de proveedor:
                    //  - bloques A–Z de longitud ≥4
                    //  - o bloques de 3 letras entre guiones o underscores (ej: _WSP_ o -WSP-)
                    var provTokensBase = Regex.Matches(provTextNorm, @"[A-Z]{4,}")
                                              .Cast<Match>()
                                              .Select(m => m.Value)
                                              .ToList();


                    var provTokens3 = Regex.Matches(provTextNorm, @"(?<=^|[-_])[A-Z]{3}(?=$|[-_])")
                                            .Cast<Match>()
                                            .Select(m => m.Value)
                                            .ToList();

                    var allProvTokens = provTokensBase
                        .Concat(provTokens3)
                        .Distinct()
                        .ToList();

                    if (allProvTokens.Count == 0)
                    {
                        // Sin tokens de proveedor → no hay repesca posible
                        stillFaltantes.Add(faltMsg ?? string.Empty);
                        continue;
                    }

                    // De cada bloque sacamos TODAS las subcadenas de longitud >= 5 (solo para los ≥5)
                    const int MIN_PROV_MATCH = 4;

                    var provTokens = allProvTokens
                        .SelectMany(tok =>
                        {
                            var subs = new List<string>();
                            if (tok.Length < MIN_PROV_MATCH) return subs;

                            for (int len = MIN_PROV_MATCH; len <= tok.Length; len++)
                            {
                                for (int i = 0; i <= tok.Length - len; i++)
                                {
                                    subs.Add(tok.Substring(i, len));
                                }
                            }

                            return subs;
                        })
                        .Distinct()
                        .OrderByDescending(s => s.Length)
                        .ToList();

                    // Si solo hay tokens de 3 letras entre guiones, los mantenemos tal cual
                    provTokens.AddRange(provTokens3);


                    

                    // 2.2. Filtrar filas candidatas por proveedor
                    //    Coincidencia si ALGUNA subcadena de 5 letras del nombre del PDF
                    //    está contenida en el nombre normalizado del proveedor en Prinex.
                    var candidatosProveedor = prinexRows
                        .Where(pr =>
                            !string.IsNullOrEmpty(pr.ProviderNorm) &&
                            provTokens.Any(tok => pr.ProviderNorm.Contains(tok)))
                        .ToList();


                    if (candidatosProveedor.Count == 0)
                    {
                        var motivoInsuf = BuildInsufficientMatchesReason(baseName, prinexRows);
                        if (motivoInsuf != null)
                        {
                            // Mantener el formato "NOMBRE → motivo"
                            stillFaltantes.Add($"{name} → {motivoInsuf}");
                        }
                        else
                        {
                            // Comportamiento anterior
                            stillFaltantes.Add(faltMsg ?? $"{name} → No se han encontrado coincidencias");
                        }


                        continue;
                    }



                    // 2.3. Buscar fragmentos numéricos (≥ 3 dígitos) del Nº factura dentro del nombre del PDF
                    var candidatosFinal = new List<(string Key, string RawFra, string ProviderNorm, string ProviderDisplay, string ConceptNorm, string ConceptDisplay, object? FechaRaw, decimal Importe)>();



                    foreach (var pr in candidatosProveedor)
                    {
                        var numMatches = Regex.Matches(pr.RawFra ?? string.Empty, @"\d{3,}");
                        bool anyFrag = false;
                        foreach (Match m in numMatches)
                        {
                            var frag = m.Value;

                            // Excluir años típicos: 2024, 2025, 2026, 2027
                            if (frag == "2024" || frag == "2025" || frag == "2026" || frag == "2027")
                                continue;

                            if (!string.IsNullOrEmpty(frag) &&
                                baseName.IndexOf(frag, StringComparison.Ordinal) >= 0)
                            {
                                anyFrag = true;
                                break;
                            }
                        }
                        if (anyFrag)
                        {
                            bool isProv3 = provTokens3.Any(tok => pr.ProviderNorm.Contains(tok));

                            if (isProv3)
                            {
                                // Para proveedores de 3 letras (entre guiones o "_") exigimos también coincidencia exacta de fecha (yyMMdd)
                                var fechaTokens = Regex.Matches(baseName, @"(?<!\d)(\d{6})(?!\d)")
                                                       .Cast<Match>()
                                                       .Select(m => m.Groups[1].Value)
                                                       .Where(ft =>
                                                       {
                                                           if (ft.Length != 6) return false;
                                                           if (!int.TryParse(ft.Substring(0, 2), out var yy)) return false;
                                                           if (!int.TryParse(ft.Substring(2, 2), out var mm)) return false;
                                                           if (!int.TryParse(ft.Substring(4, 2), out var dd)) return false;
                                                           return mm >= 1 && mm <= 12 && dd >= 1 && dd <= 31;
                                                       })
                                                       .ToList();

                                if (fechaTokens.Count > 0 && pr.FechaRaw is DateTime fecha)
                                {
                                    var tokenFecha = fecha.ToString("yyMMdd");
                                    if (fechaTokens.Contains(tokenFecha))
                                        candidatosFinal.Add(pr);
                                }
                            }
                            else
                            {
                                candidatosFinal.Add(pr);
                            }
                        }


                    }

                    if (candidatosFinal.Count == 0)
                    {
                        // REPESCA EXTRA:
                        //  - Coincidencia de concepto (≥5 letras seguidas en nombre de archivo y en concepto)
                        //  - Coincidencia de proveedor (≥4 letras seguidas en nombre de archivo y en proveedor)
                        //  - Coincidencia de fragmento numérico del nº de factura (≥3 dígitos) en el nombre del PDF

                        var tokensConcept = Regex.Matches(provTextNorm, @"[A-Z]{4,}")
 .Cast<Match>()
 .Select(m => m.Value)
 .Distinct()
 .ToList();

                        var tokens4 = Regex.Matches(provTextNorm, @"[A-Z]{4,}")
                                           .Cast<Match>()
                                           .Select(m => m.Value)
                                           .Distinct()
                                           .ToList();

                        // NUEVO: tokens de 3 letras SOLO si van entre guiones o underscores (ej: _WSP_)
                        var tokens3BetweenGuions = Regex.Matches(provTextNorm, @"(?<=^|[-_])[A-Z]{3}(?=$|[-_])")
                                                        .Cast<Match>()
                                                        .Select(m => m.Value)
                                                        .Distinct()
                                                        .ToList();


                        var candidatosConcepto = new List<(string Key, string RawFra, string ProviderNorm, string ProviderDisplay, string ConceptNorm, string ConceptDisplay, object? FechaRaw, decimal Importe, string ConceptToken, string ProvToken, string NumFrag)>();

                        foreach (var pr in prinexRows)
                        {
                            if (string.IsNullOrEmpty(pr.ConceptNorm) || string.IsNullOrEmpty(pr.ProviderNorm))
                                continue;

                            // 1) token de concepto (≥4 letras) presente en Concepto
                            string conceptTok = tokensConcept.FirstOrDefault(t => pr.ConceptNorm.Contains(t)) ?? string.Empty;
                            if (string.IsNullOrEmpty(conceptTok))
                                continue;



                            // 2) token de proveedor (≥4 letras) presente en Nombre proveedor
                            string provTok = tokens4.FirstOrDefault(t => pr.ProviderNorm.Contains(t)) ?? string.Empty;
                            if (string.IsNullOrEmpty(provTok))
                                continue;


                            // 3) fragmento numérico (≥3 dígitos) del nº de factura presente en el nombre del PDF
                            string numFrag = string.Empty;
                            var numMatches2 = Regex.Matches(pr.RawFra ?? string.Empty, @"\d{3,}");
                            foreach (Match m2 in numMatches2)
                            {
                                var frag2 = m2.Value;
                                if (string.IsNullOrEmpty(frag2))
                                    continue;

                                // Intento directo
                                string fragToSearch = frag2;
                                if (baseName.IndexOf(fragToSearch, StringComparison.Ordinal) < 0)
                                {
                                    // Si viene con ceros delante (0000164) probamos sin ceros (164)
                                    var fragNoZeros = frag2.TrimStart('0');
                                    if (fragNoZeros.Length >= 3)
                                        fragToSearch = fragNoZeros;
                                }

                                if (!string.IsNullOrEmpty(fragToSearch) &&
                                    baseName.IndexOf(fragToSearch, StringComparison.Ordinal) >= 0)
                                {
                                    numFrag = fragToSearch;
                                    break;
                                }
                            }
                            if (string.IsNullOrEmpty(numFrag))
                                continue;

                            candidatosConcepto.Add((
                                pr.Key,
                                pr.RawFra ?? string.Empty,
                                pr.ProviderNorm ?? string.Empty,
                                pr.ProviderDisplay ?? string.Empty,
                                pr.ConceptNorm ?? string.Empty,
                                pr.ConceptDisplay ?? string.Empty,
                                pr.FechaRaw,
                                pr.Importe,
                                conceptTok,
                                provTok,
                                numFrag ?? string.Empty));

                        }

                        if (candidatosConcepto.Count == 0)
                        {
                            // ==== REPESCA POR FECHA (YYMMDD en nombre del PDF) ====

                            var candidatosFecha =
                                new List<(string Key, string RawFra, string ProviderNorm, string ProviderDisplay,
                                          object? FechaRaw, decimal Importe, string ProvToken, string NumFrag, string FechaToken)>();

                            // Tokens de fecha (YYMMDD válidos) que aparezcan en el nombre del PDF
                            // (permitimos que estén pegados a guiones/underscores/letras, solo exigimos que
                            // no haya dígitos justo antes ni justo después)
                            var fechaTokens = Regex.Matches(baseName, @"(?<!\d)(\d{6})(?!\d)")
                                                   .Cast<Match>()
                                                   .Select(m => m.Groups[1].Value)
                                                   .Where(ft =>
                                                   {
                                                       if (ft.Length != 6) return false;
                                                       if (!int.TryParse(ft.Substring(0, 2), out var yyTok)) return false;
                                                       if (!int.TryParse(ft.Substring(2, 2), out var mmTok)) return false;
                                                       if (!int.TryParse(ft.Substring(4, 2), out var ddTok)) return false;
                                                       return mmTok >= 1 && mmTok <= 12 && ddTok >= 1 && ddTok <= 31;
                                                   })
                                                   .ToList();


                            if (fechaTokens.Count > 0)
                            {
                                foreach (var pr in prinexRows)
                                {
                                    // 1) token de proveedor:
                                    //    - primero intentamos tokens de ≥4 letras
                                    //    - si no hay match, permitimos tokens de 3 letras SOLO si vienen entre guiones/underscores (tokens3BetweenGuions)
                                    string provTok2 = tokens4.FirstOrDefault(t => pr.ProviderNorm.Contains(t)) ?? string.Empty;

                                    if (string.IsNullOrEmpty(provTok2) && tokens3BetweenGuions.Count > 0)
                                    {
                                        provTok2 = tokens3BetweenGuions.FirstOrDefault(t => pr.ProviderNorm.Contains(t)) ?? string.Empty;
                                    }

                                    if (string.IsNullOrEmpty(provTok2))
                                        continue;


                                    // 2) fragmento numérico (≥3 dígitos) del nº de factura presente en el nombre del PDF
                                    string numFrag2 = string.Empty;
                                    var numMatches3 = Regex.Matches(pr.RawFra ?? string.Empty, @"\d{3,}");
                                    foreach (Match m3 in numMatches3)
                                    {
                                        var fragAll = m3.Value;                  // ej: "000253000089"
                                        var fragNoZeros = fragAll.TrimStart('0'); // "253000089" (o vacío)

                                        var candidatesNums = new List<string>();

                                        if (!string.IsNullOrEmpty(fragNoZeros) && fragNoZeros.Length >= 3)
                                        {
                                            // candidato principal: todo el bloque sin ceros iniciales
                                            candidatesNums.Add(fragNoZeros);

                                            // prefijo de 3 dígitos (ej: "253") para casos tipo 000253000089
                                            candidatesNums.Add(fragNoZeros.Substring(0, 3));

                                            // opcional: prefijo de 4 dígitos, por si en el nombre hay algo como 2530…
                                            if (fragNoZeros.Length >= 4)
                                                candidatesNums.Add(fragNoZeros.Substring(0, 4));
                                        }
                                        else
                                        {
                                            // bloque tal cual si no hay suficiente tras quitar ceros
                                            candidatesNums.Add(fragAll);
                                        }

                                        foreach (var cand in candidatesNums.Distinct())
                                        {
                                            if (cand.Length < 3)
                                                continue;

                                            // excluir años sueltos 2024–2027
                                            if (cand == "2024" || cand == "2025" || cand == "2026" || cand == "2027")
                                                continue;

                                            if (baseName.IndexOf(cand, StringComparison.Ordinal) >= 0)
                                            {
                                                numFrag2 = cand;
                                                break;
                                            }
                                        }

                                        if (!string.IsNullOrEmpty(numFrag2))
                                            break;
                                    }
                                    if (string.IsNullOrEmpty(numFrag2))
                                        continue;


                                    // 3) fecha exacta: Fecha Fra. debe coincidir con algún YYMMDD del nombre
                                    DateTime fechaPrinex;
                                    if (pr.FechaRaw is DateTime dt3)
                                        fechaPrinex = dt3;
                                    else if (!DateTime.TryParse(Convert.ToString(pr.FechaRaw), out fechaPrinex))
                                        continue;

                                    var tokenFecha = fechaPrinex.ToString("yyMMdd"); // ej. 05/11/2025 -> "251105"
                                    if (!fechaTokens.Contains(tokenFecha))
                                        continue;

                                    candidatosFecha.Add((
                                        pr.Key ?? string.Empty,
                                        pr.RawFra ?? string.Empty,
                                        pr.ProviderNorm ?? string.Empty,
                                        pr.ProviderDisplay ?? string.Empty,
                                        pr.FechaRaw,
                                        pr.Importe,
                                        provTok2,
                                        numFrag2,
                                        tokenFecha));
                                }
                            }

                            if (candidatosFecha.Count == 0)
                            {
                                // Ni repesca simple, ni por concepto, ni por fecha -> sigue siendo faltante
                                var motivoInsuf = BuildInsufficientMatchesReason(baseName, prinexRows);
                                if (motivoInsuf != null)
                                {
                                    stillFaltantes.Add($"{name} → {motivoInsuf}");
                                }
                                else
                                {
                                    // Comportamiento anterior
                                    stillFaltantes.Add(faltMsg ?? string.Empty);
                                }
                                continue;
                            }



                            // Elegimos la primera candidata por fecha (el filtro ya es muy estricto)
                            var ganadorFecha = candidatosFecha[0];

                            var fechaHtmlF = $"<em>{ganadorFecha.FechaToken}</em> (<strong>fecha</strong>)";
                            var provHtmlF = $"<em>{ganadorFecha.ProvToken}</em> (<strong>proveedor</strong>)";
                            var fraHtmlF = $"<em>{ganadorFecha.NumFrag}</em> (<strong>factura</strong>)";

                            string textoCoincidenciaFecha = $"Cadenas {fechaHtmlF}, {provHtmlF} y {fraHtmlF}";


                            result.Coincidencias.Add(new PdfCoincidencia
                            {
                                Documento = name,
                                CoincidenciaDetectada = textoCoincidenciaFecha

                            });

                            if (!string.IsNullOrEmpty(ganadorFecha.Key) && !pdfKeys.ContainsKey(ganadorFecha.Key))
                            {
                                pdfKeys[ganadorFecha.Key] = name;
                            }

                            continue;
                        }


                        // Elegir candidata ganadora (mismo criterio de fecha que arriba)
                        (bool okYmd2, int yy2, int mm2, int dd2) = ExtractYMDFromFileName(name);
                        var ganadorConcepto = candidatosConcepto[0];

                        if (candidatosConcepto.Count > 1 && okYmd2)
                        {
                            var filtrados2 = candidatosConcepto
                                .Where(pr => pr.FechaRaw != null && IsSameMonth(pr.FechaRaw!, yy2, mm2))
                                .ToList();

                            if (filtrados2.Count > 0)
                                ganadorConcepto = filtrados2[0];
                        }

                        // Subcadenas tal cual aparecen en el nombre del PDF (afterPrefix)
                        string conceptMatch = ganadorConcepto.ConceptToken;
                        string provMatch2 = ganadorConcepto.ProvToken;
                        string numMatch = ganadorConcepto.NumFrag;

                        var conceptIdx = provTextNorm.IndexOf(conceptMatch, StringComparison.Ordinal);
                        if (conceptIdx >= 0 && conceptIdx + conceptMatch.Length <= baseName.Length)
                            conceptMatch = baseName.Substring(conceptIdx, conceptMatch.Length);

                        var provIdx2 = provTextNorm.IndexOf(provMatch2, StringComparison.Ordinal);
                        if (provIdx2 >= 0 && provIdx2 + provMatch2.Length <= baseName.Length)
                            provMatch2 = baseName.Substring(provIdx2, provMatch2.Length);


                        var conceptHtmlExtra = $"<em>{conceptMatch}</em> (<strong>concepto</strong>)";
                        var provHtml2 = $"<em>{provMatch2}</em> (<strong>proveedor</strong>)";
                        var fraHtml2 = $"<em>{numMatch}</em> (<strong>factura</strong>)";

                        var textoCoincidenciaExtra = $"Cadenas {conceptHtmlExtra}, {provHtml2} y {fraHtml2}";


                        result.Coincidencias.Add(new PdfCoincidencia
                        {
                            Documento = name,
                            CoincidenciaDetectada = textoCoincidenciaExtra
                        });

                        if (!string.IsNullOrEmpty(ganadorConcepto.Key) && !pdfKeys.ContainsKey(ganadorConcepto.Key))
                        {
                            pdfKeys[ganadorConcepto.Key] = name;
                        }

                        // Este PDF se rescata, no va a faltantes
                        continue;
                    }


                    // 2.4. Elegir candidata ganadora
                    (bool okYmd, int yy, int mm, int dd) = ExtractYMDFromFileName(name);
                    (string Key, string RawFra, string ProviderNorm, string ProviderDisplay, string ConceptNorm, string ConceptDisplay, object? FechaRaw, decimal Importe) ganador;



                    if (candidatosFinal.Count == 1 || !okYmd)
                    {
                        ganador = candidatosFinal[0];
                    }
                    else
                    {
                        var filtrados = candidatosFinal
                            .Where(pr => IsSameMonth(pr.FechaRaw!, yy, mm))
                            .ToList();

                        ganador = filtrados.Count > 0 ? filtrados[0] : candidatosFinal[0];
                    }

                    // 2.5. Añadir a Coincidencias (repesca) y marcar clave como vista

                    // Detectar fragmento numérico concreto de la factura presente en el nombre del PDF
                    string matchedFrag = string.Empty;
                    var fragMatches = Regex.Matches(ganador.RawFra ?? string.Empty, @"\d{3,}");
                    foreach (Match m in fragMatches)
                    {
                        var frag = m.Value;

                        // Excluir años típicos: 2024, 2025, 2026, 2027
                        if (frag == "2024" || frag == "2025" || frag == "2026" || frag == "2027")
                            continue;

                        if (!string.IsNullOrEmpty(frag) &&
                            baseName.IndexOf(frag, StringComparison.Ordinal) >= 0)
                        {
                            matchedFrag = frag;
                            break;
                        }
                    }

                    // 1) Buscar subcadena del nombre del PDF que realmente coincide con el proveedor
                    string provMatch = string.Empty;

                    foreach (var tok in provTokens.OrderByDescending(t => t.Length))
                    {
                        if (string.IsNullOrEmpty(tok))
                            continue;

                        // Debe existir en el proveedor normalizado
                        if (!ganador.ProviderNorm.Contains(tok))
                            continue;

                        // Y también en el nombre completo del PDF normalizado (sin recortar)
                        var idx = provTextNorm.IndexOf(tok, StringComparison.Ordinal);
                        if (idx >= 0 && idx + tok.Length <= baseName.Length)
                        {
                            provMatch = baseName.Substring(idx, tok.Length);
                            break;
                        }
                    }

                    // --- Regla extra para proveedores cortos (≤4 letras) ---
                    bool proveedorCorto = !string.IsNullOrEmpty(provMatch) && provMatch.Length <= 4;

                    bool tieneFecha = false;
                    string fechaToken = string.Empty;

                    if (proveedorCorto && ganador.FechaRaw is DateTime fechaDt)
                    {
                        var fechaTokens = Regex.Matches(baseName, @"(?<!\d)(\d{6})(?!\d)")
                                              .Cast<Match>()
                                              .Select(mm => mm.Groups[1].Value)
                                              .Where(ft =>
                                              {
                                                  if (ft.Length != 6) return false;
                                                  if (!int.TryParse(ft.Substring(0, 2), out var yy)) return false;
                                                  if (!int.TryParse(ft.Substring(2, 2), out var mm)) return false;
                                                  if (!int.TryParse(ft.Substring(4, 2), out var dd)) return false;
                                                  return mm >= 1 && mm <= 12 && dd >= 1 && dd <= 31;
                                              })
                                              .ToList();

                        var tokFecha = fechaDt.ToString("yyMMdd");
                        if (fechaTokens.Contains(tokFecha))
                        {
                            tieneFecha = true;
                            fechaToken = tokFecha;
                        }
                    }

                    bool tieneConcepto = false;
                    string conceptToken = string.Empty;

                    if (proveedorCorto && !string.IsNullOrEmpty(ganador.ConceptNorm))
                    {
                        var nombreNorm = provTextNorm; // ya normalizado
                        var conceptWords = Regex.Matches(nombreNorm, @"[A-Z]{4,}")
                                                .Cast<Match>()
                                                .Select(mm => mm.Value)
                                                .Distinct()
                                                .OrderByDescending(w => w.Length);

                        foreach (var w in conceptWords)
                        {
                            if (ganador.ConceptNorm.Contains(w))
                            {
                                tieneConcepto = true;
                                conceptToken = w;
                                break;
                            }
                        }
                    }

                    // Si el proveedor es corto, exigimos AL MENOS 3 evidencias reales
                    // proveedor + (nº factura / fecha / concepto)
                    if (proveedorCorto)
                    {
                        int evidencias = 0;

                        if (!string.IsNullOrEmpty(provMatch)) evidencias++;      // proveedor
                        if (!string.IsNullOrEmpty(matchedFrag)) evidencias++;   // nº factura
                        if (tieneFecha) evidencias++;                           // fecha
                        if (tieneConcepto) evidencias++;                        // concepto

                        if (evidencias < 3)
                        {
                            // Lo devolvemos a "Coincidencias insuficientes"
                            var motivoInsuf = BuildInsufficientMatchesReason(baseName, prinexRows);
                            if (!string.IsNullOrEmpty(motivoInsuf))
                                stillFaltantes.Add($"{name} → {motivoInsuf}");
                            else
                                stillFaltantes.Add(faltMsg ?? $"{name} → No se han encontrado coincidencias");

                            continue; // NO se añade a Coincidencias
                        }
                    }


                    // 2) Construir HTML con las evidencias
                    string textoCoincidencia;
                    string? provHtml = null;
                    string? fraHtml = null;
                    string? fechaHtml = null;
                    string? conceptHtml = null;

                    if (!string.IsNullOrEmpty(provMatch))
                        provHtml = $"<em>{provMatch}</em> (<strong>proveedor</strong>)";

                    if (!string.IsNullOrEmpty(matchedFrag))
                        fraHtml = $"<em>{matchedFrag}</em> (<strong>factura</strong>)";

                    if (tieneFecha && !string.IsNullOrEmpty(fechaToken))
                        fechaHtml = $"<em>{fechaToken}</em> (<strong>fecha</strong>)";

                    if (tieneConcepto && !string.IsNullOrEmpty(conceptToken))
                        conceptHtml = $"<em>{conceptToken}</em> (<strong>concepto</strong>)";

                    if (fechaHtml != null && provHtml != null && fraHtml != null)
                    {
                        // Caso tipo: 251117 (fecha), WSP (proveedor) y 2501 (factura)
                        textoCoincidencia = $"Cadenas {fechaHtml}, {provHtml} y {fraHtml}";
                    }
                    else if (conceptHtml != null && provHtml != null && fraHtml != null)
                    {
                        // Caso tipo: empleados (concepto), NODE (proveedor) y 165 (factura)
                        textoCoincidencia = $"Cadenas {conceptHtml}, {provHtml} y {fraHtml}";
                    }
                    else if (provHtml != null && fraHtml != null)
                    {
                        textoCoincidencia = $"Cadenas {provHtml} y {fraHtml}";
                    }
                    else if (provHtml != null)
                    {
                        textoCoincidencia = $"Cadena {provHtml}";
                    }
                    else if (fraHtml != null)
                    {
                        textoCoincidencia = $"Cadena {fraHtml}";
                    }
                    else
                    {
                        textoCoincidencia = "Coincidencia por proveedor / nº factura";
                    }

                    result.Coincidencias.Add(new PdfCoincidencia
                    {
                        Documento = name,
                        CoincidenciaDetectada = textoCoincidencia
                    });

                    if (!string.IsNullOrEmpty(ganador.Key) && !pdfKeys.ContainsKey(ganador.Key))
                    {
                        // Para que no aparezca en "En Prinex pero sin PDF"
                        pdfKeys[ganador.Key] = name;
                    }
                    // IMPORTANTE: este PDF se rescata (ya no va a Faltantes)

                }

                // === REPESCA 4: IMPORTE + (PROVEEDOR o Nº FACTURA o CONCEPTO) ===
                foreach (var item in pendientes)
                {
                    var name = item.Name ?? string.Empty;
                    var baseName = item.BaseName ?? string.Empty;
                    var provTextNorm = RemoveDiacritics(baseName).ToUpperInvariant();

                    // Alias: MTM → MOMENTUM REAL ESTATE (para que matche con el proveedor de Prinex)
                    if (provTextNorm.Contains("MTM"))
                    {
                        provTextNorm = provTextNorm.Replace("MTM", "MOMENTUM REAL ESTATE");
                    }

                    // Buscar cadenas tipo importe en el nombre del PDF (2.420, 163.347.20, 3.084.741.33, etc.)
                    var importesDetectados = Regex.Matches(baseName, @"\d[\d\.]{3,}")
                                                  .Cast<Match>()
                                                  .Select(m => m.Value)
                                                  .ToList();

                    if (importesDetectados.Count == 0) continue;

                    bool encontrado = false;

                    foreach (var pr in prinexRows)
                    {
                        if (pr.Importe <= 0) continue;

                        foreach (var impTxt in importesDetectados)
                        {
                            // Limpieza previa: quitamos puntos/comas “sueltos” al final: "3.084.741.33." → "3.084.741.33"
                            var impTxtClean = impTxt.Trim().TrimEnd('.', ',');

                            // Normalizar importe tipo "3.084.741.33" → "3084741,33"
                            var texto = impTxtClean;
                            var idxUltPunto = texto.LastIndexOf('.');
                            if (idxUltPunto > 0)
                            {
                                var miles = texto.Substring(0, idxUltPunto).Replace(".", "");
                                var dec = texto.Substring(idxUltPunto + 1);
                                texto = $"{miles},{dec}";
                            }

                            if (!decimal.TryParse(texto, NumberStyles.Any, new CultureInfo("es-ES"), out var impParsed))
                                continue;

                            // Normalizamos también los puntos como separadores de miles o decimales
                            string normalizado = impTxtClean.Replace(".", "");
                            // --- Normalización avanzada de importe detectado en nombre de PDF ---
                            // Limpieza robusta de importes: admite cadenas como "3.084.741.33" o "1.234.567.890.12"
                            var impClean = impTxtClean.Replace(",", "."); // unificamos comas a puntos
                            normalizado = Regex.Replace(impClean, @"[^0-9.]", ""); // mantenemos solo números y puntos


                            // Si hay más de un punto y los dos últimos dígitos del último bloque son decimales, conservamos solo el último punto
                            var partes = normalizado.Split('.');
                            if (partes.Length > 2)
                            {
                                string last = partes.Last();
                                if (last.Length == 2) // dos decimales
                                    normalizado = string.Join("", partes.Take(partes.Length - 1)) + "," + last;
                                else
                                    normalizado = string.Join("", partes); // sin decimales, quitar todos los puntos
                            }
                            else if (partes.Length == 2 && partes.Last().Length == 2)
                            {
                                normalizado = partes[0] + "," + partes[1];
                            }


                            if (string.IsNullOrWhiteSpace(normalizado))
                                continue;

                            // Reglas:
                            //  - Si hay varios puntos, el último se toma como separador decimal (2 dígitos si los hay)
                            //  - Los demás puntos se eliminan (separadores de miles)
                            int lastDot = normalizado.LastIndexOf('.');
                            if (lastDot != -1 && lastDot + 1 < normalizado.Length)
                            {
                                string decPart = normalizado.Substring(lastDot + 1);
                                if (decPart.Length == 2)
                                {
                                    normalizado = normalizado.Remove(lastDot, 1).Insert(lastDot, ",");
                                }
                                else
                                {
                                    normalizado = normalizado.Remove(lastDot, 1); // no decimal real, quitarlo
                                }
                            }

                            // Quitamos cualquier otro punto sobrante
                            normalizado = normalizado.Replace(".", "");

                            if (decimal.TryParse(normalizado, NumberStyles.Any, new CultureInfo("es-ES"), out var impPdf))
                            {
                                // Si el PDF tiene importe sin decimales y el Excel con decimales, igualarlos
                                var diff = Math.Abs(Math.Round(pr.Importe, 2) - Math.Round(impPdf, 2));
                                if (diff > 0.01m)
                                {
                                    // También probamos la conversión sin decimales exactos (por ejemplo 2420 == 2420,00)
                                    if (Math.Abs(Math.Round(pr.Importe, 0) - Math.Round(impPdf, 0)) > 0)
                                        continue;
                                }
                            }
                            else
                            {
                                continue;
                            }



                            // Segunda coincidencia: proveedor, nº factura, fecha o concepto
                            // -> Mostrar SOLO el trozo que realmente aparece en el PDF (intersección Excel↔PDF),
                            //    y si rescata, eliminarlo de stillFaltantes.

                            var pdfNorm = RemoveDiacritics(baseName).ToUpperInvariant();

                            // 1) Proveedor: palabra del proveedor (Excel) que esté en el nombre del PDF
                            string provMatch = string.Empty;
                            if (!string.IsNullOrWhiteSpace(pr.ProviderDisplay))
                            {
                                var provWords = Regex.Matches(RemoveDiacritics(pr.ProviderDisplay).ToUpperInvariant(), @"[A-Z0-9]{2,}")
                                                     .Cast<Match>()
                                                     .Select(m => m.Value)
                                                     .Distinct()
                                                     .OrderByDescending(w => w.Length);

                                provMatch = provWords.FirstOrDefault(w => pdfNorm.Contains(w)) ?? string.Empty;
                            }
                            if (string.IsNullOrEmpty(provMatch) && !string.IsNullOrWhiteSpace(pr.ProviderNorm))
                            {
                                var provWords = Regex.Matches(pr.ProviderNorm, @"[A-Z0-9]{2,}")
                                                     .Cast<Match>()
                                                     .Select(m => m.Value)
                                                     .Distinct()
                                                     .OrderByDescending(w => w.Length);

                                provMatch = provWords.FirstOrDefault(w => pdfNorm.Contains(w)) ?? string.Empty;
                            }
                            bool proveedorOk = !string.IsNullOrEmpty(provMatch);

                            // 2) Factura: fragmento numérico REAL del S/Fra. Número que esté en el PDF
                            string matchedFrag = string.Empty;
                            if (!string.IsNullOrWhiteSpace(pr.RawFra))
                            {
                                foreach (Match m in Regex.Matches(pr.RawFra, @"\d{3,}"))
                                {
                                    var frag = m.Value;

                                    // ignora años típicos
                                    if (frag == "2024" || frag == "2025" || frag == "2026" || frag == "2027") continue;

                                    var cand = frag;
                                    if (baseName.IndexOf(cand, StringComparison.Ordinal) < 0)
                                    {
                                        var noZeros = frag.TrimStart('0');
                                        if (noZeros.Length >= 3) cand = noZeros;
                                    }

                                    if (baseName.IndexOf(cand, StringComparison.Ordinal) >= 0)
                                    {
                                        matchedFrag = cand;
                                        break;
                                    }
                                }
                            }
                            bool facturaOk = !string.IsNullOrEmpty(matchedFrag);

                            // 3) Concepto: palabra del concepto (Excel) que esté en el nombre del PDF
                            string conceptMatch = string.Empty;
                            if (!string.IsNullOrWhiteSpace(pr.ConceptDisplay))
                            {
                                var conceptWords = Regex.Matches(RemoveDiacritics(pr.ConceptDisplay).ToUpperInvariant(), @"[A-Z0-9]{3,}")
                                                        .Cast<Match>()
                                                        .Select(m => m.Value)
                                                        .Distinct()
                                                        .OrderByDescending(w => w.Length);

                                conceptMatch = conceptWords.FirstOrDefault(w => pdfNorm.Contains(w)) ?? string.Empty;
                            }
                            if (string.IsNullOrEmpty(conceptMatch) && !string.IsNullOrWhiteSpace(pr.ConceptNorm))
                            {
                                var conceptWords = Regex.Matches(pr.ConceptNorm, @"[A-Z0-9]{3,}")
                                                        .Cast<Match>()
                                                        .Select(m => m.Value)
                                                        .Distinct()
                                                        .OrderByDescending(w => w.Length);

                                conceptMatch = conceptWords.FirstOrDefault(w => pdfNorm.Contains(w)) ?? string.Empty;
                            }
                            bool conceptoOk = !string.IsNullOrEmpty(conceptMatch);

                            // 4) Fecha: yyMMdd exacto en el nombre del PDF
                            bool fechaOk = false;
                            string fechaToken = string.Empty;
                            if (pr.FechaRaw is DateTime dt4)
                            {
                                fechaToken = dt4.ToString("yyMMdd");
                                if (!string.IsNullOrEmpty(fechaToken))
                                    fechaOk = baseName.Contains(fechaToken, StringComparison.Ordinal);
                            }

                            // Fuerza: importe + al menos otra evidencia REAL (mejor: 2 evidencias)
                            int evidencias = 0;
                            if (proveedorOk) evidencias++;
                            if (facturaOk) evidencias++;
                            if (conceptoOk) evidencias++;
                            if (fechaOk) evidencias++;

                            bool coincidenciaFuerte = evidencias >= 2;

                            if (coincidenciaFuerte)
                            {
                                var impDisplay = impPdf.ToString("0.##", new CultureInfo("es-ES"));

                                var partesCoinc = new List<string>
    {
        $"<em>{impDisplay}</em> (<strong>importe</strong>)"
    };

                                if (proveedorOk)
                                    partesCoinc.Add($"<em>{provMatch}</em> (<strong>proveedor</strong>)");

                                if (conceptoOk)
                                    partesCoinc.Add($"<em>{conceptMatch}</em> (<strong>concepto</strong>)");

                                if (facturaOk)
                                    partesCoinc.Add($"<em>{matchedFrag}</em> (<strong>factura</strong>)");

                                if (fechaOk && !string.IsNullOrEmpty(fechaToken))
                                    partesCoinc.Add($"<em>{fechaToken}</em> (<strong>fecha</strong>)");

                                string textoCoinc = (partesCoinc.Count == 1 ? "Cadena " : "Cadenas ") + string.Join(", ", partesCoinc);

                                result.Coincidencias.Add(new PdfCoincidencia
                                {
                                    Documento = name,
                                    CoincidenciaDetectada = textoCoinc
                                });

                                if (!string.IsNullOrEmpty(pr.Key) && !pdfKeys.ContainsKey(pr.Key))
                                    pdfKeys[pr.Key] = name;

                                // IMPORTANTÍSIMO: si lo rescata aquí, sácalo de Faltantes
                                stillFaltantes.RemoveAll(msg =>
                                {
                                    if (string.IsNullOrWhiteSpace(msg)) return false;
                                    var idxArrow = msg.IndexOf('→');
                                    var doc = (idxArrow >= 0 ? msg.Substring(0, idxArrow) : msg).Trim();
                                    return doc.Equals(name, StringComparison.OrdinalIgnoreCase);
                                });

                                encontrado = true;
                                break;
                            }


                        }

                        if (encontrado)
                            break;
                    }
                }


                // Faltantes definitivos = solo los que la repesca no ha rescatado
                result.Faltantes.AddRange(stillFaltantes);

            }
            else
            {
                // Sin repesca (no hay proveedor o no hay pendientes) → Faltantes tal cual primera pasada
                result.Faltantes.AddRange(pendientes.Select(p => p.FaltanteMsg ?? string.Empty));

            }

            // 🔍 Post-procesado: eliminar de Faltantes los PDFs que SÍ tienen coincidencia
            if (result.Faltantes.Count > 0 && result.Coincidencias.Count > 0)
            {
                var docsCoincidentes = new HashSet<string>(
                    result.Coincidencias
                          .Where(c => !string.IsNullOrWhiteSpace(c.Documento))
                          .Select(c => c.Documento!.Trim()),
                    StringComparer.OrdinalIgnoreCase
                );

                result.Faltantes = result.Faltantes
                    .Where(msg =>
                    {
                        if (string.IsNullOrWhiteSpace(msg)) return false;

                        // El nombre del PDF es lo que va antes del "→"
                        var idx = msg.IndexOf('→');
                        var nombreDoc = (idx >= 0 ? msg.Substring(0, idx) : msg).Trim();

                        return !docsCoincidentes.Contains(nombreDoc);
                    })
                    .ToList();
            }

            // ==== En Prinex pero sin PDF (no se toca la forma de construcción) ====
            foreach (var kv in prinex)
            {
                if (!pdfKeys.ContainsKey(kv.Key))
                {
                    var info = kv.Value;
                    result.Descuadres.Add($"En Prinex pero sin PDF: {info.Display} ({info.Importe:0.00} €)");
                }
            }

            return result;

        }

        // Sobrecarga para compatibilidad con la llamada que pasa SociedadFiltro
        private static PreviewPdfDto BuildPreviewFromPdfNamesAndExcel(
            byte[] excelBytes,
            List<CuadreFile> pdfFiles,
            string? _ /*sociedadFiltro*/)
        {
            // De momento ignoramos el filtro y reutilizamos la lógica principal
            return BuildPreviewFromPdfNamesAndExcel(excelBytes, pdfFiles);
        }

        private static string? SimpleInvoiceKey(string? text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return null;

            var cleaned = text.Replace(((char)160).ToString(), " ").Trim();

            // Buscamos bloques de 5+ dígitos (números de factura típicos)
            var matches = Regex.Matches(cleaned, @"\d{5,}");
            if (matches.Count == 0)
                return null;

            // Nos quedamos con el bloque más largo (si hay empate, el primero)
            var best = matches.Cast<Match>()
                              .OrderByDescending(m => m.Value.Length)
                              .First()
                              .Value;

            return best;
        }

        private static string? BuildInsufficientMatchesReason(
    string baseName,
    List<(string Key,
          string RawFra,
          string ProviderNorm,
          string ProviderDisplay,
          string ConceptNorm,
          string ConceptDisplay,
          object? FechaRaw,
          decimal Importe)> prinexRows)
        {
            if (prinexRows == null || prinexRows.Count == 0)
                return null;

            // Nombre PDF “normalizado”
            var pdfNorm = RemoveDiacritics(baseName).ToUpperInvariant();
            var pdfDigits = Regex.Replace(baseName, @"\D", "");


            string? bestProv = null;
            string? bestFra = null;
            string? bestConcept = null;
            string? bestImporte = null;
            string? bestFecha = null;
            int bestScore = 0;

            foreach (var pr in prinexRows)
            {
                // --- proveedor ---
                string? provTok = null;
                if (!string.IsNullOrEmpty(pr.ProviderDisplay))
                {
                    var provNorm = RemoveDiacritics(pr.ProviderDisplay).ToUpperInvariant();
                    var words = Regex.Matches(provNorm, @"[A-Z0-9]{3,}")
                                     .Cast<Match>()
                                     .Select(m => m.Value)
                                     .Distinct()
                                     .OrderByDescending(w => w.Length);

                    provTok = words.FirstOrDefault(w => pdfNorm.Contains(w));
                }

                // --- nº factura / S/Fra. Número ---
                string? fraTok = null;
                var rawFra = pr.RawFra ?? string.Empty;

                // 1) intento con el número completo
                if (rawFra.Length >= 3 &&
                    baseName.IndexOf(rawFra, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    fraTok = rawFra;
                }
                else if (!string.IsNullOrWhiteSpace(rawFra))
                {
                    // 2) si no encaja el texto completo, probamos con fragmentos numéricos (≥3 dígitos)
                    var numMatches = Regex.Matches(rawFra, @"\d{3,}");
                    foreach (Match m2 in numMatches)
                    {
                        var frag = m2.Value.TrimStart('0');
                        if (frag.Length < 3)
                            continue;

                        // ignora años típicos
                        if (frag == "2024" || frag == "2025" || frag == "2026" || frag == "2027")
                            continue;

                        if (baseName.IndexOf(frag, StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            fraTok = frag;
                            break;
                        }
                    }
                }


                // --- concepto ---
                string? conceptTok = null;
                if (!string.IsNullOrEmpty(pr.ConceptDisplay))
                {
                    var conceptNorm = RemoveDiacritics(pr.ConceptDisplay).ToUpperInvariant();
                    var words = Regex.Matches(conceptNorm, @"[A-Z0-9]{3,}")
                                     .Cast<Match>()
                                     .Select(m => m.Value)
                                     .Distinct()
                                     .OrderByDescending(w => w.Length);

                    conceptTok = words.FirstOrDefault(w => pdfNorm.Contains(w));
                }

                // --- importe ---
                string? importeTok = null;
                if (pr.Importe != 0m)
                {
                    var impStr = pr.Importe.ToString("0.##", new CultureInfo("es-ES"));
                    var impDigits = Regex.Replace(impStr, @"\D", "");
                    if (impDigits.Length >= 3 && pdfDigits.Contains(impDigits))
                        importeTok = impStr;
                }

                // --- fecha (yyMMdd en el nombre del PDF) ---
                string? fechaTok = null;
                if (pr.FechaRaw is DateTime dt)
                {
                    var tok = dt.ToString("yyMMdd");
                    if (baseName.IndexOf(tok, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        fechaTok = tok;
                    }
                }


                int score = 0;
                if (!string.IsNullOrEmpty(provTok)) score++;
                if (!string.IsNullOrEmpty(fraTok)) score++;
                if (!string.IsNullOrEmpty(conceptTok)) score++;
                if (!string.IsNullOrEmpty(importeTok)) score++;
                if (!string.IsNullOrEmpty(fechaTok)) score++;

                if (score > bestScore)
                {
                    bestScore = score;
                    bestProv = provTok;
                    bestFra = fraTok;
                    bestConcept = conceptTok;
                    bestImporte = importeTok;
                    bestFecha = fechaTok;
                }
            }

            if (bestScore == 0)
                return null;

            var partes = new List<string>();

            if (!string.IsNullOrEmpty(bestProv))
                partes.Add($"<em>{bestProv}</em> (<strong>proveedor</strong>)");

            if (!string.IsNullOrEmpty(bestFra))
                partes.Add($"<em>{bestFra}</em> (<strong>factura</strong>)");

            if (!string.IsNullOrEmpty(bestConcept))
                partes.Add($"<em>{bestConcept}</em> (<strong>concepto</strong>)");

            if (!string.IsNullOrEmpty(bestImporte))
                partes.Add($"<em>{bestImporte}</em> (<strong>importe</strong>)");

            if (!string.IsNullOrEmpty(bestFecha))
                partes.Add($"<em>{bestFecha}</em> (<strong>fecha</strong>)");


            if (partes.Count == 0)
                return null;

            var prefijo = partes.Count == 1
                ? "Cadena "
                : "Cadenas ";

            return prefijo + string.Join(", ", partes);
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
