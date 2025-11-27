using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

internal class Program
{
    static async Task Main(string[] args)
    {
        // Carpeta raíz de la solución
        var solutionRoot = Path.GetFullPath(
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..")
        );

        // Carpeta del servicio FastAPI (pdf-service)
        var pdfServiceDir = Path.Combine(solutionRoot, "pdf-service");

        Console.WriteLine($"Trabajando en: {pdfServiceDir}");

        var psi = new ProcessStartInfo
        {
            FileName = "py",          // si usas 'py', cambia aquí a "py"
            Arguments = "main.py",
            WorkingDirectory = pdfServiceDir,
            UseShellExecute = false,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
        };

        using var process = Process.Start(psi);

        if (process == null)
        {
            Console.WriteLine("No se pudo lanzar python main.py");
            return;
        }

        Console.WriteLine("✅ FastAPI lanzado en http://127.0.0.1:8000");
        Console.WriteLine("Deja esta ventana abierta mientras uses la web.");
        Console.WriteLine("Pulsa Ctrl+C o cierra la ventana para detener el servidor.");

        // Mantiene vivo el launcher mientras el proceso Python siga corriendo
        await process.WaitForExitAsync();

        Console.WriteLine($"FastAPI terminado con código {process.ExitCode}.");
    }
}
