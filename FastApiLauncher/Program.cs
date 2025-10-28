using System.Diagnostics;
using System.Net.Sockets;

bool PuertoLibre(int port)
{
    try
    {
        using var c = new TcpClient("127.0.0.1", port);
        return false; // puerto ocupado
    }
    catch
    {
        return true; // puerto libre
    }
}

try
{
    if (PuertoLibre(8010))

    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("🚀 Lanzando FastAPI en http://127.0.0.1:8000 ...");
        Console.ResetColor();

        var psi = new ProcessStartInfo
        {
            FileName = @"C:\Users\Javier Cid\AppData\Local\Programs\Python\Python311\python.exe",
            Arguments = "-m uvicorn main:app --host 127.0.0.1 --port 8010 --reload",

            WorkingDirectory = @"C:\Users\Javier Cid\source\repos\Departamento Financiero\pdf-service",
            UseShellExecute = false,
            CreateNoWindow = false // 👈 deja visible la consola del servidor FastAPI
        };

        Process.Start(psi);
    }
    else
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("✅ FastAPI ya está corriendo en http://127.0.0.1:8000. No se lanza de nuevo.");
        Console.ResetColor();
    }
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine("❌ No se pudo lanzar FastAPI: " + ex.Message);
    Console.ResetColor();
}
