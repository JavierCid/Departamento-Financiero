using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.Server;


var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services
    .AddServerSideBlazor()
    .AddCircuitOptions(options =>
    {
        options.DetailedErrors = true;                     // para ver errores detallados
        options.JSInteropDefaultCallTimeout = TimeSpan.FromMinutes(10); // subimos timeout
    });

builder.Services.AddScoped(sp => new System.Net.Http.HttpClient());


var app = builder.Build();

app.UseDeveloperExceptionPage();
app.UseStaticFiles();

app.UseRouting();

app.MapBlazorHub();
app.MapFallbackToPage("/_Host");

app.Run();
