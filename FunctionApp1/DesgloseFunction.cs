using System.Net;
using System.Text.Json;
using ClosedXML.Excel;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionApp1;

public class DesgloseFunction
{
    [Function("DesglosarFacturas")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", "options")] HttpRequestData req)
    {
        if (req.Method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
        {
            var pre = req.CreateResponse(HttpStatusCode.NoContent);
            Function1.AddCors(pre, req);
            return pre;
        }

        try
        {
            // Nombre original
            string fileName = req.Headers.TryGetValues("X-File-Name", out var v)
                                ? (v.FirstOrDefault() ?? "archivo.xlsx")
                                : "archivo.xlsx";

            // Leer body a memoria
            using var ms = new MemoryStream();
            await req.Body.CopyToAsync(ms);
            if (ms.Length == 0) return await Bad(req, "El archivo está vacío.");

            // Validar firma ZIP (PK) de .xlsx
            ms.Position = 0;
            var hdr = new byte[2];
            _ = ms.Read(hdr, 0, 2);
            if (hdr[0] != 0x50 || hdr[1] != 0x4B) return await Bad(req, "No parece un .xlsx (firma ZIP incorrecta).");
            ms.Position = 0;

            // ================= LÓGICA DE DESGLOSE =================
            using var wb = new XLWorkbook(ms);

            // Tomar hoja "MAYORES" si existe; si no, primera con datos
            var ws = wb.Worksheets.FirstOrDefault(s => s.Name.Equals("MAYORES", StringComparison.OrdinalIgnoreCase))
                     ?? wb.Worksheets.FirstOrDefault(s => s.FirstCellUsed() != null);
            if (ws is null) return await Bad(req, "No se encontró ninguna hoja con datos.");

            // Detectar fila de cabeceras (buscamos 'Cuenta', 'Concepto', 'Debe', 'Haber', 'Importe')
            int headerRow = FindHeaderRow(ws, new[] { "Cuenta", "Concepto" }, 60);
            if (headerRow == 0) return await Bad(req, "No se localizaron cabeceras (mínimo: 'Cuenta' y 'Concepto').");

            int cCuenta = FindHeaderCol(ws, headerRow, "Cuenta");
            int cConcepto = FindHeaderCol(ws, headerRow, "Concepto");

            // Importes: prioriza HABER; si no hay, busca IMPORTE; si tampoco, usa DEBE (con signo positivo)
            int cHaber = FindHeaderCol(ws, headerRow, "Haber");
            int cImporte = FindHeaderCol(ws, headerRow, "Importe");
            int cDebe = FindHeaderCol(ws, headerRow, "Debe");

            if (cCuenta == 0 || cConcepto == 0)
                return await Bad(req, "Faltan columnas obligatorias: 'Cuenta' y/o 'Concepto'.");

            int lastRow = ws.LastRowUsed()?.RowNumber() ?? headerRow;
            var agregados = new Dictionary<(string Cuenta, string Concepto), decimal>();

            for (int r = headerRow + 1; r <= lastRow; r++)
            {
                var cuenta = ws.Cell(r, cCuenta).GetString().Trim();
                var concepto = ws.Cell(r, cConcepto).GetString().Trim();

                if (string.IsNullOrWhiteSpace(cuenta) && string.IsNullOrWhiteSpace(concepto)) continue;

                decimal importe = 0m;
                if (cHaber > 0)
                    importe = SafeNumber(ws.Cell(r, cHaber).Value);
                else if (cImporte > 0)
                    importe = SafeNumber(ws.Cell(r, cImporte).Value);
                else if (cDebe > 0)
                    importe = SafeNumber(ws.Cell(r, cDebe).Value);
                else
                    continue; // no hay importe utilizable

                var key = (Cuenta: cuenta, Concepto: concepto);
                if (agregados.TryGetValue(key, out var acc))
                    agregados[key] = acc + importe;
                else
                    agregados[key] = importe;
            }

            // Crear/limpiar hoja "Desglose"
            var wsOut = wb.Worksheets.FirstOrDefault(s => s.Name.Equals("Desglose", StringComparison.OrdinalIgnoreCase));
            if (wsOut != null) wsOut.Delete();
            wsOut = wb.Worksheets.Add("Desglose");

            // Escribir tabla
            int row = 1;
            wsOut.Cell(row, 1).Value = "Cuenta";
            wsOut.Cell(row, 2).Value = "Concepto";
            wsOut.Cell(row, 3).Value = "Importe";
            wsOut.Range(row, 1, row, 3).Style.Font.Bold = true;
            row++;

            foreach (var kv in agregados.OrderBy(k => k.Key.Cuenta).ThenBy(k => k.Key.Concepto))
            {
                wsOut.Cell(row, 1).Value = kv.Key.Cuenta;
                wsOut.Cell(row, 2).Value = kv.Key.Concepto;
                wsOut.Cell(row, 3).Value = kv.Value;
                row++;
            }

            // Total al final
            wsOut.Cell(row, 2).Value = "Total";
            wsOut.Cell(row, 2).Style.Font.Bold = true;
            wsOut.Cell(row, 3).FormulaA1 = $"SUM(C2:C{row - 1})";
            wsOut.Cell(row, 3).Style.NumberFormat.Format = "#,##0.00 [$€-es-ES]";
            wsOut.Columns("A:C").AdjustToContents();

            // ========= Vista previa (primeras 20 filas) =========
            var preview = agregados.Take(20).Select(kv => new
            {
                Cuenta = kv.Key.Cuenta,
                Concepto = kv.Key.Concepto,
                Importe = Math.Round(kv.Value, 2)
            }).ToList();

            // Respuesta
            var resp = req.CreateResponse(HttpStatusCode.OK);
            resp.Headers.Add("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

            var baseName = Path.GetFileNameWithoutExtension(fileName);
            var outName = $"Desglose_{(string.IsNullOrWhiteSpace(baseName) ? "archivo" : baseName)}.xlsx";
            resp.Headers.Add("Content-Disposition",
                $"attachment; filename=\"{outName}\"; filename*=UTF-8''{Uri.EscapeDataString(outName)}");
            resp.Headers.Add("X-Preview", JsonSerializer.Serialize(new { Lineas = preview }));

            using var outMs = new MemoryStream();
            wb.SaveAs(outMs);
            await resp.WriteBytesAsync(outMs.ToArray());
            Function1.AddCors(resp, req);
            return resp;
        }
        catch (Exception ex)
        {
            var err = req.CreateResponse(HttpStatusCode.InternalServerError);
            await err.WriteStringAsync($"Error procesando Excel (Desglose): {ex.Message}");
            Function1.AddCors(err, req);
            return err;
        }

        // ==== helpers locales ====
        static async Task<HttpResponseData> Bad(HttpRequestData req, string msg)
        {
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync(msg);
            Function1.AddCors(bad, req);
            return bad;
        }

        static int FindHeaderRow(IXLWorksheet ws, IEnumerable<string> mustHave, int maxScan = 60)
        {
            for (int r = 1; r <= maxScan; r++)
            {
                bool ok = true;
                foreach (var h in mustHave)
                    if (FindHeaderCol(ws, r, h) == 0) { ok = false; break; }
                if (ok) return r;
            }
            return 0;
        }

        static int FindHeaderCol(IXLWorksheet ws, int headerRow, string headerName)
        {
            int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
            for (int c = 1; c <= lastCol; c++)
            {
                var s = ws.Cell(headerRow, c).GetString().Trim();
                if (string.Equals(s, headerName, StringComparison.OrdinalIgnoreCase))
                    return c;
            }
            return 0;
        }

        static decimal SafeNumber(object v)
        {
            if (v is null) return 0m;
            if (v is double d) return (decimal)d;
            if (v is float f) return (decimal)f;
            if (v is decimal m) return m;
            if (v is int i) return i;
            if (v is long l) return l;

            var s = Convert.ToString(v) ?? string.Empty;
            s = s.Replace(" ", "").Replace(".", "").Replace(",", ".");
            return decimal.TryParse(s, System.Globalization.NumberStyles.Any,
                                    System.Globalization.CultureInfo.InvariantCulture, out var res)
                ? res : 0m;
        }
    }
}
