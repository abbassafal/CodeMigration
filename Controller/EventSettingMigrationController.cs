using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

[ApiController]
[Route("api/[controller]")]
public class EventSettingMigrationController : ControllerBase
{
    private readonly EventSettingMigrationService _migrationService;
    private readonly ILogger<EventSettingMigrationController> _logger;

    public EventSettingMigrationController(EventSettingMigrationService migrationService, ILogger<EventSettingMigrationController> logger)
    {
        _migrationService = migrationService;
        _logger = logger;
    }

    [HttpPost("migrate")]
    public async Task<IActionResult> MigrateEventSettings()
    {
        try
        {
            var migratedCount = await _migrationService.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event setting migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }
}
