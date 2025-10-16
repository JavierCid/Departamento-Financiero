using System.Net;
using System.Text.Json;
using System.Linq;
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
        // Preflight CORS
        if (req.Method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
        {
            var pre = req.CreateResponse(HttpStatusCode.NoContent);
            Function1.AddCors(pre, req);
            return pre;
        }

        try
        {
            // Nombre original desde cabecera (si viene)
            string fileName = req.Headers.TryGetValues("X-File-Name", out var vals)
                ? (vals.FirstOrDefault() ?? "archivo.xlsx")
                : "archivo.xlsx";

            // Leer body a memoria
            using var ms = new MemoryStream();
            await req.Body.CopyToAsync(ms);
            if (ms.Length == 0)
                return await Bad(req, "El archivo está vacío.");

            // Validación ZIP simple (firma "PK")
            if (ms.Length < 2)
                return await Bad(req, "No parece un .xlsx (tamaño insuficiente).");

            ms.Position = 0;
            var hdr = new byte[2];
            _ = ms.Read(hdr, 0, 2);
            if (hdr[0] != 0x50 || hdr[1] != 0x4B)
                return await Bad(req, "No parece un .xlsx válido (firma ZIP incorrecta).");
            ms.Position = 0;

            // =========================
            // LÓGICA REAL DEL DESGLOSE
            // =========================
            using var wb = new XLWorkbook(ms);

            // Crea (o reutiliza) hoja "Desglose"
            var ws = wb.Worksheets.FirstOrDefault(s => s.Name == "Desglose")
                     ?? wb.Worksheets.Add("Desglose");

            ws.Cell(1, 1).Value = $"Desglose generado desde: {fileName}";

            // TODO: remplaza por tu lógica real de desglose:
            ws.Cell(3, 1).Value = "Cuenta";
            ws.Cell(3, 2).Value = "Concepto";
            ws.Cell(3, 3).Value = "Importe";
            ws.Cell(4, 1).Value = "7000";
            ws.Cell(4, 2).Value = "Ejemplo";
            ws.Cell(4, 3).Value = 123.45;

            // --- Vista previa para el frontend (ajústala a tus datos) ---
            var preview = new
            {
                Lineas = new[]
                {
                    new { Cuenta = "7000", Concepto = "Ejemplo", Importe = 123.45m }
                }
            };

            // Respuesta: adjunta Excel + preview en cabecera
            var resp = req.CreateResponse(HttpStatusCode.OK);
            resp.Headers.Add("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

            var baseName = Path.GetFileNameWithoutExtension(fileName);
            if (string.IsNullOrWhiteSpace(baseName)) baseName = "Desglose";
            var outName = $"Desglose_{baseName}.xlsx";

            resp.Headers.Add(
                "Content-Disposition",
                $"attachment; filename=\"{outName}\"; filename*=UTF-8''{Uri.EscapeDataString(outName)}");

            resp.Headers.Add("X-Preview", JsonSerializer.Serialize(preview));

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
    }

    private static async Task<HttpResponseData> Bad(HttpRequestData req, string msg)
    {
        var bad = req.CreateResponse(HttpStatusCode.BadRequest);
        await bad.WriteStringAsync(msg);
        Function1.AddCors(bad, req);
        return bad;
    }
}
