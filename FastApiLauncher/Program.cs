using System.Diagnostics;

try
{
    var psi = new ProcessStartInfo
    {
        FileName = "cmd.exe",
        Arguments = "/C start \"\" \"C:\\Users\\Javier Cid\\source\\repos\\Departamento Financiero\\run_fastapi.bat\"",
        UseShellExecute = true,   // lanza en nueva ventana
        CreateNoWindow = false
    };
    Process.Start(psi);
}
catch (Exception ex)
{
    Console.Error.WriteLine("No se pudo lanzar FastAPI: " + ex.Message);
}

// No bloqueamos; dejamos que VS siga con el resto de proyectos
