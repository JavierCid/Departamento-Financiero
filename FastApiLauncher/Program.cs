using System.Diagnostics;
using System.Net.Sockets;

bool PuertoLibre(int port)
{
    try { using var c = new TcpClient("127.0.0.1", port); return false; }
    catch { return true; }
}

try
{
    if (PuertoLibre(8000))
    {
        var psi = new ProcessStartInfo
        {
            FileName = @"C:\Users\Javier Cid\AppData\Local\Programs\Python\Python311\python.exe",
            Arguments = "-m uvicorn main:app --host 127.0.0.1 --port 8000 --reload",
            WorkingDirectory = @"C:\Users\Javier Cid\source\repos\Departamento Financiero\pdf-service",
            UseShellExecute = false,
            CreateNoWindow = true
        };
        Process.Start(psi);
    }
    else
    {
        Console.WriteLine("FastAPI ya está en 127.0.0.1:8000. No se lanza de nuevo.");
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine("No se pudo lanzar FastAPI: " + ex.Message);
}
