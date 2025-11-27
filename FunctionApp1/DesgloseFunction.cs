using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.Azure.Functions.Worker;

using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionApp1;

public class DesgloseFunction
{
    private static readonly HttpClient Http = new()
    {
        Timeout = TimeSpan.FromMinutes(10)
    };

    // AJUSTA el puerto si tu FastAPI no está en 8000
    private const string PdfServiceUrl = "http://127.0.0.1:8000/api/pdf2excel";

    [Function("DesglosarFacturas")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(
            AuthorizationLevel.Anonymous,
            "post",
            "options",
            Route = "DesglosarFacturas")]
        HttpRequestData req)
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
            // Necesitamos el Content-Type multipart/form-data con boundary
            if (!req.Headers.TryGetValues("Content-Type", out var ctValues))
                return await Bad(req, "Falta cabecera Content-Type multipart/form-data.");

            var ct = ctValues.FirstOrDefault();
            if (string.IsNullOrWhiteSpace(ct))
                return await Bad(req, "Content-Type vacío.");

            // Reenviar al servicio Python copiando el body en memoria
            byte[] bodyBytes;
            using (var ms = new MemoryStream())
            {
                await req.Body.CopyToAsync(ms);
                bodyBytes = ms.ToArray();
            }

            if (bodyBytes.Length == 0)
                return await Bad(req, "Body vacío recibido en Function.");

            var forward = new HttpRequestMessage(HttpMethod.Post, PdfServiceUrl)
            {
                Content = new ByteArrayContent(bodyBytes)
            };
            forward.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(ct);


            // Copiar cabeceras útiles (por ejemplo X-File-Name)
            foreach (var h in req.Headers)
            {
                if (h.Key.Equals("Content-Type", StringComparison.OrdinalIgnoreCase) ||
                    h.Key.Equals("Content-Length", StringComparison.OrdinalIgnoreCase))
                    continue;

                var value = string.Join(",", h.Value);

                if (!forward.Headers.TryAddWithoutValidation(h.Key, value))
                {
                    forward.Content.Headers.TryAddWithoutValidation(h.Key, value);
                }
            }


            using var pyResp = await Http.SendAsync(forward);

            // Crear respuesta hacia Blazor con mismo status code
            var resp = req.CreateResponse(pyResp.StatusCode);

            // Copiar cabeceras de la respuesta de Python (incluye X-Preview, Content-Disposition, etc.)
            foreach (var h in pyResp.Headers)
                resp.Headers.Add(h.Key, string.Join(",", h.Value));

            if (pyResp.Content != null)
            {
                foreach (var h in pyResp.Content.Headers)
                    resp.Headers.Add(h.Key, string.Join(",", h.Value));

                var bytes = await pyResp.Content.ReadAsByteArrayAsync();
                await resp.WriteBytesAsync(bytes);
            }

            Function1.AddCors(resp, req);
            return resp;
        }
        catch (Exception ex)
        {
            var err = req.CreateResponse(HttpStatusCode.InternalServerError);
            await err.WriteStringAsync($"Error reenviando a pdf-service: {ex.Message}");
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
