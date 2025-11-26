using Extensions;
using Microsoft.AspNetCore.DataProtection;
using DataMigration.Hubs;

var builder = WebApplication.CreateBuilder(args);

// Configure Data Protection for containerized environment
builder.Services.AddDataProtection()
    .SetApplicationName("DataMigration");

// Add services to the container.
builder.Services.AddControllersWithViews();

// Add SignalR for real-time progress updates
builder.Services.AddSignalR();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(builder =>
    {
        builder.AllowAnyOrigin()
               .AllowAnyMethod()
               .AllowAnyHeader();
    });
});

// Register migration services
builder.Services.AddMigrationServices();

// Configure logging to reduce noise in development
if (builder.Environment.IsDevelopment())
{
    builder.Logging.AddFilter("Microsoft.AspNetCore.DataProtection", LogLevel.Error);
    builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Error);
}

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseCors();

app.UseAuthorization();

// Map controllers
app.MapControllers();

// Map SignalR hubs
app.MapHub<MigrationProgressHub>("/migrationProgressHub");

// Default route to Migration/Index
app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Migration}/{action=Index}/{id?}");

app.Run();