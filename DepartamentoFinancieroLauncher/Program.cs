using System.Diagnostics;

try
{
    Process.Start(new ProcessStartInfo
    {
        FileName = "http://localhost:5000", // o la URL de tu app
        UseShellExecute = true
    });
}
catch (Exception ex)
{
    Console.WriteLine($"Error al abrir la página: {ex.Message}");
}
